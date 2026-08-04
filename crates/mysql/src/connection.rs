use std::{
    env,
    path::PathBuf,
    str::FromStr,
    time::Duration,
};

use ::metrics::StaticMetricLabel;
use common::{
    errors::{
        database_operational_error,
        database_timeout_error,
        DatabaseOperationalError,
        DatabaseTimeoutError,
    },
    fastrace_helpers::FutureExt as _,
    knobs::{
        MYSQL_INACTIVE_CONNECTION_LIFETIME,
        MYSQL_MAX_CONNECTIONS,
        MYSQL_MAX_CONNECTION_LIFETIME,
        MYSQL_MAX_QUERY_RETRIES,
        MYSQL_TIMEOUT,
    },
    pool_stats::{
        ConnectionPoolStats,
        ConnectionTracker,
    },
    runtime::{
        assert_send,
        Runtime,
    },
};
use dynfmt::{
    ArgumentSpec,
    Error,
    Format,
    FormatArgs,
    Position,
};
use fastrace::func_path;
use futures::{
    pin_mut,
    select_biased,
    Future,
    FutureExt as _,
    Stream,
    TryStreamExt,
};
use metrics::{
    ProgressCounter,
    Timer,
};
use mysql_async::{
    prelude::Queryable,
    Conn,
    DriverError,
    IsolationLevel,
    Opts,
    OptsBuilder,
    Params,
    Pool,
    PoolConstraints,
    PoolOpts,
    Row,
    TxOpts,
    Value as MySqlValue,
};
use prometheus::VMHistogramVec;
use tokio::time::sleep;
use url::Url;

use crate::{
    cancellation::{
        CancellationReservation,
        MySqlCancellationController,
        MySqlConnectionIdTopology,
    },
    metrics::{
        begin_transaction_timer,
        commit_timer,
        connection_lifetime_timer,
        get_connection_timer,
        log_execute,
        log_large_statement,
        log_query,
        log_query_result,
        log_transaction,
        new_connection_pool_stats,
        query_progress_counter,
        LARGE_STATEMENT_THRESHOLD,
    },
};

fn classify_mysql_error(e: mysql_async::Error) -> anyhow::Error {
    match e {
        mysql_async::Error::Driver(
            DriverError::PoolDisconnected | DriverError::ConnectionClosed,
        )
        | mysql_async::Error::Io(_)
        | mysql_async::Error::Server(mysql_async::ServerError {
            // Expected operational Vitess errors:
            code:
            | 1290 // EROptionPreventsStatement "The MySQL server is running with the --read-only option so it cannot execute this statement"
            | 2013 // CRServerLost
            | 1053 // ERServerShutdown
            | 1040 // ERConCount "Too many connections"
            , ..
        }) => {
            database_operational_error(e.into())
        },
        mysql_async::Error::Server(mysql_async::ServerError {
            // ERUnknownError
            code: 1105,
            ref message,
            ..
        }) if message.contains("primary is not serving")
            || message.contains("for tx killer rollback")
            || message.contains("connection pool timed out")
            || message.contains("connection timed out") =>
        {
            database_operational_error(e.into())
        },
        _ => e.into(),
    }
}

// Guard against connections hanging during bootstrapping -- which means
// instances can't start -- and during commit -- which means all future commits
// fail with OCC errors.
//
// To avoid these problems, wrap anything that talks to mysql in with_timeout.
// It returns a classified timeout error after `MYSQL_TIMEOUT`; guarded SQL
// operations then discard their connection through the cancel-safe owner.
pub(crate) async fn with_timeout<R, Fut: Future<Output = Result<R, mysql_async::Error>>>(
    f: Fut,
) -> anyhow::Result<R> {
    select_biased! {
        r = f.fuse() => {
            r.map_err(classify_mysql_error)
        },
        _ = sleep(Duration::from_secs(*MYSQL_TIMEOUT)).fuse() => Err(
            anyhow::anyhow!(database_timeout_error("MySQL"))),
    }
}

struct MySQLFormatArguments {
    escaped_db_name: String,
    params: Vec<String>,
}

impl FormatArgs for MySQLFormatArguments {
    fn get_index(&self, index: usize) -> Result<Option<dynfmt::Argument<'_>>, ()> {
        self.params.get_index(index)
    }

    fn get_key(&self, key: &str) -> Result<Option<dynfmt::Argument<'_>>, ()> {
        match key {
            "db_name" => Ok(Some(&self.escaped_db_name)),
            _ => panic!("Unexpected named argument {key}"),
        }
    }
}

const DB_NAME_ARGUMENT_PATTERN: &str = "@db_name";

// Formats @db_name and ?
struct MySQLRawStatementFormat;

impl<'f> Format<'f> for MySQLRawStatementFormat {
    type Iter = impl Iterator<Item = Result<ArgumentSpec<'f>, Error<'f>>>;

    fn iter_args(&self, format: &'f str) -> Result<Self::Iter, Error<'f>> {
        let db_name_iter = format
            .match_indices(DB_NAME_ARGUMENT_PATTERN)
            .map(|(index, _)| {
                Ok(
                    ArgumentSpec::new(index, index + DB_NAME_ARGUMENT_PATTERN.len())
                        .with_position(Position::Key("db_name")),
                )
            });
        let args_iter = format
            .match_indices('?')
            .map(|(index, _)| Ok(ArgumentSpec::new(index, index + 1)));
        // The resulting iterator should be sorted.
        let mut args: Vec<_> = db_name_iter.chain(args_iter).collect();
        args.sort_by_key(|arg| match arg {
            Ok(arg) => arg.start(),
            Err(_) => 0,
        });
        Ok::<Self::Iter, _>(args.into_iter())
    }
}

// Formats a MySQL query with position parameters into a string, so it can be
// used with the text protocol.
fn format_mysql_text_protocol(
    db_name: &str,
    statement: &str,
    params: Vec<MySqlValue>,
    labels: &[StaticMetricLabel],
) -> anyhow::Result<String> {
    let args = MySQLFormatArguments {
        escaped_db_name: format!("`{db_name}`"),
        params: params
            .into_iter()
            .map(|p| match p {
                MySqlValue::NULL => "NULL".to_owned(),
                MySqlValue::Bytes(bytes) => format!("x'{}'", const_hex::display(bytes)),
                MySqlValue::Int(i) => format!("{i}"),
                MySqlValue::UInt(u) => format!("{u}"),
                // We don't use the following and I don't want to deal with escaping them.
                MySqlValue::Float(_) => panic!("Float MySQL argument not supported"),
                MySqlValue::Double(_) => panic!("Double MySQL argument not supported"),
                MySqlValue::Date(..) => panic!("Date MySQL argument not supported"),
                MySqlValue::Time(..) => panic!("Time MySQL argument not supported"),
            })
            .collect(),
    };
    let result = MySQLRawStatementFormat
        .format(statement, args)
        .map_err(|e| anyhow::anyhow!("failed to format statement: {e}"))?
        .to_string();
    if result.len() > LARGE_STATEMENT_THRESHOLD {
        log_large_statement(labels.to_vec());
    }
    Ok(result)
}

// Formats @db_name
struct MySQLPreparedStatementFormat;

impl<'f> Format<'f> for MySQLPreparedStatementFormat {
    type Iter = impl Iterator<Item = Result<ArgumentSpec<'f>, Error<'f>>>;

    fn iter_args(&self, format: &'f str) -> Result<Self::Iter, Error<'f>> {
        Ok::<Self::Iter, _>(
            format
                .match_indices(DB_NAME_ARGUMENT_PATTERN)
                .map(|(index, _)| {
                    Ok(
                        ArgumentSpec::new(index, index + DB_NAME_ARGUMENT_PATTERN.len())
                            .with_position(Position::Key("db_name")),
                    )
                }),
        )
    }
}

// Formats a MySQL query by only replacing the @db_name but leaves positional
// arguments alone. To be used with MySQL binary protocol.
fn format_mysql_binary_protocol(db_name: &str, statement: &str) -> anyhow::Result<String> {
    let args = MySQLFormatArguments {
        escaped_db_name: format!("`{db_name}`"),
        params: vec![], // No positional arguments.
    };
    Ok(MySQLPreparedStatementFormat
        .format(statement, args)
        .map_err(|e| anyhow::anyhow!("failed to format statement: {e}"))?
        .to_string())
}

pub(crate) struct MySqlConnection<'a, RT: Runtime> {
    conn: Option<Conn>,
    labels: Vec<StaticMetricLabel>,
    pool: &'a ConvexMySqlPool<RT>,
    db_name: &'a str,
    _tracker: ConnectionTracker,
    _timer: Timer<VMHistogramVec>,
}

struct CancelSafeOperation<'a, RT: Runtime> {
    cancellation: &'a MySqlCancellationController<RT>,
    ownership:
        OperationOwnership<'a, Conn, CancellationReservation, MySqlCancellationController<RT>>,
}

trait OperationControl<T, R> {
    fn cancel(&self, value: T, reservation: R);

    fn register(&self, value: &T);
}

impl<RT: Runtime> OperationControl<Conn, CancellationReservation>
    for MySqlCancellationController<RT>
{
    fn cancel(&self, value: Conn, reservation: CancellationReservation) {
        let _ = MySqlCancellationController::cancel(self, value, reservation);
    }

    fn register(&self, value: &Conn) {
        MySqlCancellationController::register(self, value);
    }
}

struct OperationOwnership<'a, T, R, C: OperationControl<T, R>> {
    completed: bool,
    cancellation: &'a C,
    value: &'a mut Option<T>,
    reservation: Option<R>,
}

impl<'a, T, R, C: OperationControl<T, R>> OperationOwnership<'a, T, R, C> {
    fn new(value: &'a mut Option<T>, reservation: R, cancellation: &'a C) -> Self {
        assert!(value.is_some(), "cancel-safe operation requires a value");
        Self {
            completed: false,
            cancellation,
            value,
            reservation: Some(reservation),
        }
    }

    fn value(&mut self) -> &mut T {
        self.value
            .as_mut()
            .expect("cancel-safe operation lost its value")
    }

    fn take_for_cancellation(&mut self) -> Option<(T, R)> {
        match (self.value.take(), self.reservation.take()) {
            (Some(value), Some(reservation)) => Some((value, reservation)),
            (None, None) => None,
            _ => panic!("cancel-safe operation value and reservation diverged"),
        }
    }

    fn install(&mut self, value: T, reservation: R) {
        assert!(
            self.value.is_none() && self.reservation.is_none(),
            "cancel-safe operation installed a value while it still owned another value"
        );
        self.cancellation.register(&value);
        *self.value = Some(value);
        self.reservation = Some(reservation);
    }

    fn complete(&mut self) {
        self.completed = true;
    }
}

impl<T, R, C: OperationControl<T, R>> Drop for OperationOwnership<'_, T, R, C> {
    fn drop(&mut self) {
        if !self.completed
            && let Some((value, reservation)) = self.take_for_cancellation()
        {
            self.cancellation.cancel(value, reservation);
        }
    }
}

impl<'a, RT: Runtime> CancelSafeOperation<'a, RT> {
    async fn begin(
        value: &'a mut Option<Conn>,
        cancellation: &'a MySqlCancellationController<RT>,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            value.is_some(),
            "MySQL connection cannot be reused after a canceled operation"
        );
        let reservation = cancellation.reserve().await?;
        Ok(Self {
            cancellation,
            ownership: OperationOwnership::new(value, reservation, cancellation),
        })
    }

    fn value(&mut self) -> &mut Conn {
        self.ownership.value()
    }

    fn complete(mut self) {
        self.ownership.complete();
    }

    async fn replace_after_discard(
        &mut self,
        replacement: impl Future<Output = anyhow::Result<Conn>>,
    ) -> anyhow::Result<()> {
        // Keep the slot empty before polling replacement acquisition. If acquisition
        // waits or its caller is canceled, the failed value has already been
        // synchronously discarded.
        let (value, reservation) = self
            .ownership
            .take_for_cancellation()
            .expect("cancel-safe operation lost its cancellation reservation");
        let terminal = self.cancellation.cancel(value, reservation).wait().await;
        anyhow::ensure!(
            terminal != crate::cancellation::CancellationTerminal::ControlFailure,
            "MySQL server-side cancellation failed"
        );
        let replacement = replacement.await?;
        let replacement_reservation = self.cancellation.reserve().await?;
        self.ownership.install(replacement, replacement_reservation);
        Ok(())
    }
}

fn connection_error_requires_discard(error: &anyhow::Error) -> bool {
    // These errors may leave the protocol stream incomplete. Other server errors
    // are complete responses, so a caller may catch them and continue using the
    // same transaction.
    error.is::<DatabaseOperationalError>() || error.is::<DatabaseTimeoutError>()
}

fn connection_result_requires_discard<R>(conn: &Conn, result: &anyhow::Result<R>) -> bool {
    // mysql_async can preserve an ordinary server error while a follow-up
    // protocol action (notably transaction rollback after failed commit) leaves
    // the connection disconnected. The returned error alone is not sufficient
    // to decide whether the server session still needs cancellation.
    conn.is_disconnected()
        || result
            .as_ref()
            .is_err_and(|error| connection_error_requires_discard(error))
}

struct TransactionConnectionUse<'a> {
    poisoned: &'a mut bool,
}

impl<'a> TransactionConnectionUse<'a> {
    fn begin(poisoned: &'a mut bool) -> anyhow::Result<Self> {
        anyhow::ensure!(
            !*poisoned,
            "MySQL transaction connection cannot be reused after a canceled or incomplete \
             operation"
        );
        *poisoned = true;
        Ok(Self { poisoned })
    }

    fn complete(self) {
        *self.poisoned = false;
    }
}

struct HandledMySqlOperation<R> {
    connection_reusable: bool,
    error_connection_replaced: bool,
    result: anyhow::Result<R>,
}

async fn handle_errors_with_retries<R, RT: Runtime>(
    operation: &mut CancelSafeOperation<'_, RT>,
    pool: &ConvexMySqlPool<RT>,
    mut f: impl AsyncFnMut(&mut Conn) -> anyhow::Result<R>,
    max_retries: u32,
) -> HandledMySqlOperation<R> {
    let mut attempt = 0;
    loop {
        let result = f(operation.value()).await;
        let connection_disconnected = operation.value().is_disconnected();
        let (e, should_retry) = match result {
            Err(e) if e.is::<DatabaseOperationalError>() => (e, attempt < max_retries),
            Err(e) if e.is::<DatabaseTimeoutError>() => {
                // Don't retry here as we want the caller to receive some
                // backpressure.
                // The mysql protocol doesn't support cancellation, so if a
                // query times out on the client, the connection can't be reused
                // until the server responds to the query.
                // So don't return the connection to the pool.
                (e, false)
            },
            // Some mysql_async protocol paths return a driver error rather than
            // the fatal follow-up error that actually disconnected the connection.
            // Do not let recycler disposal replace authenticated server-side
            // cancellation for those paths.
            Err(e) if connection_disconnected => (e, false),
            result => {
                return HandledMySqlOperation {
                    connection_reusable: true,
                    error_connection_replaced: false,
                    result,
                };
            },
        };
        if should_retry {
            tracing::warn!("Retrying after MySQL error: {e:#}")
        } else if connection_error_requires_discard(&e) {
            tracing::warn!("Discarding connection after MySQL error: {e:#}")
        } else {
            // This branch can carry an ordinary server error preserved across
            // a fatal driver cleanup failure. Do not add that raw server
            // message to the new disconnected-state diagnostic.
            tracing::warn!("Discarding disconnected MySQL connection after driver error")
        }
        if let Err(error) = operation
            .replace_after_discard(pool.acquire_internal())
            .await
        {
            return HandledMySqlOperation {
                connection_reusable: false,
                error_connection_replaced: false,
                result: Err(error),
            };
        }
        if should_retry {
            attempt += 1;
            continue;
        } else {
            return HandledMySqlOperation {
                connection_reusable: true,
                error_connection_replaced: true,
                result: Err(e),
            };
        }
    }
}

async fn handle_errors<R, RT: Runtime>(
    operation: &mut CancelSafeOperation<'_, RT>,
    pool: &ConvexMySqlPool<RT>,
    f: impl AsyncFnOnce(&mut Conn) -> anyhow::Result<R>,
) -> HandledMySqlOperation<R> {
    let mut f = Some(f);
    handle_errors_with_retries(
        operation,
        pool,
        async move |conn| f.take().expect("should never retry")(conn).await,
        0, /* max_retries */
    )
    .await
}

impl<RT: Runtime> MySqlConnection<'_, RT> {
    /// Executes multiple statements, separated by semicolons.
    #[fastrace::trace]
    pub async fn execute_many(&mut self, query: &str) -> anyhow::Result<()> {
        log_execute(self.labels.clone());
        let statement = format_mysql_text_protocol(self.db_name, query, vec![], &self.labels)?;
        let mut operation =
            CancelSafeOperation::begin(&mut self.conn, &self.pool.cancellation).await?;
        let result = handle_errors(&mut operation, self.pool, async move |conn| {
            // Every statement after the first runs only as its result set is
            // consumed, and reports its error only there. Dropping the result
            // instead leaves them pending on the connection, so they run if it
            // happens to be used again and are silently lost if it is not.
            let result = with_timeout(conn.query_iter(statement)).await?;
            with_timeout(result.drop_result()).await?;
            Ok(())
        })
        .await;
        if result.connection_reusable {
            operation.complete();
        }
        result.result
    }

    /// Run a readonly query that returns one or zero results.
    #[fastrace::trace]
    pub async fn query_optional(
        &mut self,
        statement: &str,
        params: Vec<MySqlValue>,
    ) -> anyhow::Result<Option<Row>> {
        log_query(self.labels.clone());
        let (statement, prepared_params) = if self.pool.use_prepared_statements {
            (
                format_mysql_binary_protocol(self.db_name, statement)?,
                Some(params),
            )
        } else {
            (
                format_mysql_text_protocol(self.db_name, statement, params, &self.labels)?,
                None,
            )
        };
        let mut operation =
            CancelSafeOperation::begin(&mut self.conn, &self.pool.cancellation).await?;
        let result = if let Some(params) = prepared_params {
            handle_errors_with_retries(
                &mut operation,
                self.pool,
                async move |conn| with_timeout(conn.exec_first(&statement, params.clone())).await,
                *MYSQL_MAX_QUERY_RETRIES,
            )
            .await
        } else {
            handle_errors_with_retries(
                &mut operation,
                self.pool,
                async move |conn| with_timeout(conn.query_first(&statement)).await,
                *MYSQL_MAX_QUERY_RETRIES,
            )
            .await
        };
        if result.connection_reusable {
            operation.complete();
        }
        if let Ok(Some(row)) = &result.result {
            log_query_result(self.labels.clone()).add_row(row);
        }
        result.result
    }

    /// Run a readonly query and collect the results, mapping them with `f`
    #[fastrace::trace]
    pub async fn query_collect<R: Send>(
        &mut self,
        statement: &str,
        params: Vec<MySqlValue>,
        size_hint: usize,
        f: impl Fn(Row) -> anyhow::Result<R> + Send + Sync + 'static,
    ) -> anyhow::Result<Vec<R>> {
        let labels = self.labels.clone();
        log_query(labels.clone());
        let (statement, prepared_params) = if self.pool.use_prepared_statements {
            (
                format_mysql_binary_protocol(self.db_name, statement)?,
                Some(params),
            )
        } else {
            (
                format_mysql_text_protocol(self.db_name, statement, params, &self.labels)?,
                None,
            )
        };
        let mut operation =
            CancelSafeOperation::begin(&mut self.conn, &self.pool.cancellation).await?;
        let result = if let Some(params) = prepared_params {
            assert_send(handle_errors_with_retries(
                &mut operation,
                self.pool,
                async move |conn| {
                    // The outer cancel-safe owner disconnects this connection if the stream is
                    // canceled or returns an error. The progress counter records incomplete
                    // consumption before ownership is discarded.
                    let progress_counter = query_progress_counter(size_hint, labels.clone());
                    Self::collect_query_stream(
                        with_timeout(
                            conn.exec_stream(&statement, Params::Positional(params.clone())),
                        )
                        .await?,
                        progress_counter,
                        labels.clone(),
                        &f,
                    )
                    .await
                },
                *MYSQL_MAX_QUERY_RETRIES,
            ))
            .await
        } else {
            assert_send(handle_errors_with_retries(
                &mut operation,
                self.pool,
                async move |conn| {
                    let progress_counter = query_progress_counter(size_hint, labels.clone());
                    Self::collect_query_stream(
                        with_timeout(conn.query_stream(&statement)).await?,
                        progress_counter,
                        labels.clone(),
                        &f,
                    )
                    .await
                },
                *MYSQL_MAX_QUERY_RETRIES,
            ))
            .await
        };
        if result.result.is_ok() || result.error_connection_replaced {
            operation.complete();
        }
        result.result
    }

    async fn collect_query_stream<R>(
        stream: impl Stream<Item = mysql_async::Result<Row>>,
        mut progress_counter: ProgressCounter,
        labels: Vec<StaticMetricLabel>,
        f: impl Fn(Row) -> anyhow::Result<R>,
    ) -> anyhow::Result<Vec<R>> {
        let mut result = vec![];
        pin_mut!(stream);
        let mut stats = log_query_result(labels);
        while let Some(row) = with_timeout(stream.try_next()).await? {
            progress_counter.add_processed(1);
            stats.add_row(&row);
            // `f` may be computationally intensive, and
            // `stream.try_next().await` might not yield to tokio if the rows
            // are all available at once. Avoid long poll times by intentionally
            // yielding.
            tokio::task::consume_budget().await;
            result.push(f(row)?);
        }
        progress_counter.complete();
        Ok(result)
    }

    /// Execute a SQL statement, returning the number of rows affected.
    #[fastrace::trace]
    pub async fn exec_iter(
        &mut self,
        statement: &str,
        params: Vec<MySqlValue>,
    ) -> anyhow::Result<u64> {
        log_execute(self.labels.clone());
        let (statement, prepared_params) = if self.pool.use_prepared_statements {
            (
                format_mysql_binary_protocol(self.db_name, statement)?,
                Some(params),
            )
        } else {
            (
                format_mysql_text_protocol(self.db_name, statement, params, &self.labels)?,
                None,
            )
        };
        let mut operation =
            CancelSafeOperation::begin(&mut self.conn, &self.pool.cancellation).await?;
        let result = if let Some(params) = prepared_params {
            handle_errors(&mut operation, self.pool, async move |conn| {
                with_timeout(async {
                    let result = conn
                        .exec_iter(statement, Params::Positional(params))
                        .await?;
                    let affected_rows = result.affected_rows();
                    result.drop_result().await?;
                    Ok(affected_rows)
                })
                .await
            })
            .await
        } else {
            handle_errors(&mut operation, self.pool, async move |conn| {
                with_timeout(async {
                    let result = conn.query_iter(statement).await?;
                    let affected_rows = result.affected_rows();
                    result.drop_result().await?;
                    Ok(affected_rows)
                })
                .await
            })
            .await
        };
        if result.connection_reusable {
            operation.complete();
        }
        result.result
    }

    #[fastrace::trace]
    pub async fn transaction<F, T>(
        &mut self,
        db_cluster_name: &str,
        isolation: Option<IsolationLevel>,
        f: F,
    ) -> anyhow::Result<T>
    where
        F: for<'b> AsyncFnOnce(&'b mut MySqlTransaction<'_>) -> anyhow::Result<T>,
    {
        let timer = begin_transaction_timer(db_cluster_name);
        log_transaction(self.labels.clone());
        let mut operation =
            CancelSafeOperation::begin(&mut self.conn, &self.pool.cancellation).await?;
        let mut transaction_connection_poisoned = false;
        let result: anyhow::Result<T> = async {
            let mut tx_opts = TxOpts::new();
            tx_opts.with_isolation_level(isolation);
            let inner = with_timeout(operation.value().start_transaction(tx_opts)).await?;
            timer.finish();
            let mut transaction = MySqlTransaction {
                inner,
                use_prepared_statements: self.pool.use_prepared_statements,
                db_name: self.db_name,
                labels: &self.labels,
                connection_poisoned: &mut transaction_connection_poisoned,
            };
            let value = f(&mut transaction).await?;
            let timer = commit_timer(db_cluster_name);
            transaction.commit().await?;
            timer.finish();
            Ok(value)
        }
        .await;
        let connection_requires_discard = transaction_connection_poisoned
            || connection_result_requires_discard(operation.value(), &result);
        if connection_requires_discard {
            return match result {
                Ok(_) => Err(anyhow::anyhow!(
                    "MySQL transaction connection was left unusable by a canceled or incomplete \
                     operation"
                )),
                Err(error) => Err(error),
            };
        }
        operation.complete();
        result
    }
}

impl<RT: Runtime> Drop for MySqlConnection<'_, RT> {
    fn drop(&mut self) {
        if let Some(conn) = &self.conn {
            self.pool.cancellation.unregister(conn);
        }
    }
}

pub(crate) struct MySqlTransaction<'a> {
    inner: mysql_async::Transaction<'a>,
    use_prepared_statements: bool,
    db_name: &'a str,
    labels: &'a [StaticMetricLabel],
    connection_poisoned: &'a mut bool,
}

impl MySqlTransaction<'_> {
    /// Executes the given statement and returns the first row of the first
    /// result set.
    pub async fn exec_first(
        &mut self,
        statement: &str,
        params: Vec<MySqlValue>,
    ) -> anyhow::Result<Option<Row>> {
        let (statement, prepared_params) = if self.use_prepared_statements {
            (
                format_mysql_binary_protocol(self.db_name, statement)?,
                Some(params),
            )
        } else {
            (
                format_mysql_text_protocol(self.db_name, statement, params, self.labels)?,
                None,
            )
        };
        let operation = TransactionConnectionUse::begin(self.connection_poisoned)?;
        let future = if let Some(params) = prepared_params {
            self.inner.exec_first(statement, Params::Positional(params))
        } else {
            self.inner.query_first(statement)
        };
        let result = with_timeout(future).await;
        if !connection_result_requires_discard(&self.inner, &result) {
            operation.complete();
        }
        result
    }

    /// Executes the given statement and drops the result.
    pub async fn exec_drop(
        &mut self,
        statement: &str,
        params: Vec<MySqlValue>,
    ) -> anyhow::Result<()> {
        let (statement, prepared_params) = if self.use_prepared_statements {
            (
                format_mysql_binary_protocol(self.db_name, statement)?,
                Some(params),
            )
        } else {
            (
                format_mysql_text_protocol(self.db_name, statement, params, self.labels)?,
                None,
            )
        };
        let operation = TransactionConnectionUse::begin(self.connection_poisoned)?;
        let future = if let Some(params) = prepared_params {
            self.inner.exec_drop(statement, Params::Positional(params))
        } else {
            self.inner.query_drop(statement)
        };
        let result = with_timeout(future).await;
        if !connection_result_requires_discard(&self.inner, &result) {
            operation.complete();
        }
        result
    }

    /// Execute a SQL statement, returning the number of rows affected.
    pub async fn exec_iter(
        &mut self,
        statement: &str,
        params: Vec<MySqlValue>,
    ) -> anyhow::Result<u64> {
        let (statement, prepared_params) = if self.use_prepared_statements {
            (
                format_mysql_binary_protocol(self.db_name, statement)?,
                Some(params),
            )
        } else {
            (
                format_mysql_text_protocol(self.db_name, statement, params, self.labels)?,
                None,
            )
        };
        let operation = TransactionConnectionUse::begin(self.connection_poisoned)?;
        let result = if let Some(params) = prepared_params {
            with_timeout(async {
                let result = self
                    .inner
                    .exec_iter(statement, Params::Positional(params))
                    .await?;
                let affected_rows = result.affected_rows();
                result.drop_result().await?;
                Ok(affected_rows)
            })
            .await
        } else {
            with_timeout(async {
                let result = self.inner.query_iter(statement).await?;
                let affected_rows = result.affected_rows();
                result.drop_result().await?;
                Ok(affected_rows)
            })
            .await
        };
        if !connection_result_requires_discard(&self.inner, &result) {
            operation.complete();
        }
        result
    }

    pub async fn commit(self) -> anyhow::Result<()> {
        let Self {
            inner,
            connection_poisoned,
            ..
        } = self;
        let operation = TransactionConnectionUse::begin(connection_poisoned)?;
        let result = with_timeout(inner.commit()).await;
        if !result
            .as_ref()
            .is_err_and(|error| connection_error_requires_discard(error))
        {
            operation.complete();
        }
        result
    }
}

pub struct ConvexMySqlPool<RT: Runtime> {
    pool: Pool,
    cancellation: MySqlCancellationController<RT>,
    use_prepared_statements: bool,
    runtime: Option<RT>,
    stats: ConnectionPoolStats,
    // Used for metrics
    cluster_name: String,
}

// Deriving the cluster name from the URL is a bit hacky, but seems cleaner than
// to pass cluster_name from 7 layers deep just for metric. It is easy to
// confuse those with the url and db_name that are used in the actual queries.
fn derive_cluster_name(url: &Url) -> &str {
    if url.host_str().is_some_and(|s| s.ends_with(".psdb.cloud")) {
        return url.path().trim_start_matches('/');
    }
    let mut cluster_name = url
        .host_str()
        .and_then(|host| host.split('.').next())
        .unwrap_or("");
    if let Some(name) = cluster_name.strip_suffix("-proxy") {
        cluster_name = name;
    }
    cluster_name
}

impl<RT: Runtime> ConvexMySqlPool<RT> {
    pub fn new(
        url: &Url,
        use_prepared_statements: bool,
        require_leader: bool,
        runtime: RT,
        connection_id_topology: MySqlConnectionIdTopology,
    ) -> anyhow::Result<Self> {
        let cluster_name = derive_cluster_name(url).to_owned();
        // NOTE: the inactive_connection_ttl only applies to connections > min
        // constraint. So to make it apply to all connections, set min=0.
        // Connections are accessed in FIFO order from the pool (not round robin)
        // so the pool should be kept small by limiting inactive_connection_ttl.
        let constraints = PoolConstraints::new(0, *MYSQL_MAX_CONNECTIONS).unwrap();
        let pool_opts = PoolOpts::new()
            .with_constraints(constraints)
            // Jitter max connection lifetime with 20%. This is split between
            // the ttl_check_interval and the per-connection jitter.
            .with_ttl_check_interval(*MYSQL_MAX_CONNECTION_LIFETIME / 10)
            .with_inactive_connection_ttl(*MYSQL_INACTIVE_CONNECTION_LIFETIME)
            .with_abs_conn_ttl(Some(*MYSQL_MAX_CONNECTION_LIFETIME))
            .with_abs_conn_ttl_jitter(Some(*MYSQL_MAX_CONNECTION_LIFETIME / 10))
            .with_reset_connection(false); // persist prepared statements
        let opts = Opts::from_str(url.as_ref())?;
        let ssl_opts = opts.ssl_opts().cloned();
        let mut opts = OptsBuilder::from_opts(opts).pool_opts(pool_opts);
        if require_leader {
            opts = opts.after_connect(|conn| {
                async move {
                    let readonly: Option<(bool,)> = conn
                        .query_first("SELECT @@global.innodb_read_only OR @@global.read_only")
                        .await?;
                    let Some((readonly,)) = readonly else {
                        return Err(mysql_async::Error::Other("expected a result".into()));
                    };
                    if readonly {
                        return Err(mysql_async::Error::Other(
                            database_operational_error(anyhow::anyhow!(
                                "Connected to a read-only database"
                            ))
                            .into(),
                        ));
                    }
                    Ok(())
                }
                .boxed()
            });
        }
        // The MYSQL_CA_FILE environment variable implicitly enables TLS unless
        // the URL specifies require_ssl=false
        if let Some(ca_file_path) = env::var_os("MYSQL_CA_FILE")
            && !ca_file_path.is_empty()
            && !url
                .query_pairs()
                .any(|(k, v)| k == "require_ssl" && v == "false")
        {
            let ca_file_path = PathBuf::from(ca_file_path);
            anyhow::ensure!(
                ca_file_path.exists(),
                "MYSQL_CA_FILE does not exist: {}",
                ca_file_path.display()
            );
            let ssl_opts = ssl_opts
                .unwrap_or_default()
                .with_root_certs(vec![ca_file_path.into()]);
            opts = opts.ssl_opts(ssl_opts);
        }
        let opts: Opts = opts.into();
        let control_pool_opts = PoolOpts::new()
            // Keep the initialized control transport for the lifetime of the
            // cancellation lane. Replacing it could cross a server restart or
            // backend namespace boundary where numeric connection IDs repeat.
            .with_constraints(PoolConstraints::new(1, 1).unwrap())
            .with_reset_connection(false);
        let control_pool =
            Pool::new(OptsBuilder::from_opts(opts.clone()).pool_opts(control_pool_opts));
        let cancellation = MySqlCancellationController::new(
            control_pool,
            runtime.clone(),
            *MYSQL_MAX_CONNECTIONS,
            cluster_name.clone(),
            connection_id_topology,
        );
        Ok(Self {
            pool: Pool::new(opts),
            cancellation,
            use_prepared_statements,
            runtime: Some(runtime),
            stats: new_connection_pool_stats(cluster_name.as_str()),
            cluster_name,
        })
    }

    pub(crate) async fn acquire_internal(&self) -> anyhow::Result<Conn> {
        // In trusted-single-namespace mode, establish the persistent control
        // transport first, so every data connection belongs to the same or a
        // later server epoch. If the server changes afterward,
        // control-generation validation fails closed. This is a no-op for the
        // safe-default client-disconnect mode.
        self.cancellation.initialize().await?;
        let pool_get_timer = get_connection_timer(&self.cluster_name);
        let conn = with_timeout(self.pool.get_conn())
            .trace_if_pending(func_path!()) // only trace if slow
            .await;
        pool_get_timer.finish(conn.is_ok());
        conn
    }

    pub(crate) async fn acquire<'a>(
        &'a self,
        name: &'static str,
        db_name: &'a str,
    ) -> anyhow::Result<MySqlConnection<'a, RT>> {
        let conn = self.acquire_internal().await?;
        self.cancellation.register(&conn);
        Ok(MySqlConnection {
            conn: Some(conn),
            labels: vec![
                StaticMetricLabel::new("name", name),
                StaticMetricLabel::new("cluster_name", self.cluster_name.clone()),
            ],
            pool: self,
            db_name,
            _tracker: ConnectionTracker::new(&self.stats),
            _timer: connection_lifetime_timer(name, &self.cluster_name),
        })
    }

    pub fn cluster_name(&self) -> &str {
        &self.cluster_name
    }

    /// Report gauges with information about the MySQL pool.
    /// Note that this only makes sense if there is a single pool for this
    /// cluster in this process.
    pub fn log_pool_metrics(&self) {
        crate::metrics::log_pool_metrics(&self.cluster_name, &self.pool.metrics());
    }

    pub async fn shutdown(&self) -> anyhow::Result<()> {
        tracing::info!("Shutting down ConvexMySqlPool");
        let data_pool_result = self.pool.clone().disconnect().await;
        let cancellation_result = self.cancellation.shutdown().await;
        data_pool_result?;
        cancellation_result
    }
}

impl<RT: Runtime> Drop for ConvexMySqlPool<RT> {
    fn drop(&mut self) {
        tracing::info!("ConvexMySqlPool dropped");
        let Some(runtime) = self.runtime.take() else {
            return;
        };
        let pool = self.pool.clone();
        let cancellation = self.cancellation.clone();
        runtime.spawn_background("mysql_pool_disconnect", async move {
            let data_pool_result = pool.disconnect().await;
            let cancellation_result = cancellation.shutdown().await;
            if data_pool_result.is_ok() && cancellation_result.is_ok() {
                tracing::info!("ConvexMySqlPool pool successfully closed");
            } else {
                tracing::error!("ConvexMySqlPool pool shutdown failed");
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::{
        OperationControl,
        OperationOwnership,
        TransactionConnectionUse,
    };

    #[derive(Default)]
    struct RecordingOperationControl {
        cancellations: Mutex<Vec<(u64, u64)>>,
        registrations: Mutex<Vec<u64>>,
    }

    impl OperationControl<u64, u64> for RecordingOperationControl {
        fn cancel(&self, value: u64, reservation: u64) {
            self.cancellations
                .lock()
                .unwrap()
                .push((value, reservation));
        }

        fn register(&self, value: &u64) {
            self.registrations.lock().unwrap().push(*value);
        }
    }

    #[test]
    fn incomplete_operation_drop_cancels_once_and_removes_owned_value() {
        let control = RecordingOperationControl::default();
        let mut value = Some(1);
        {
            let _ownership = OperationOwnership::new(&mut value, 10, &control);
        }

        assert_eq!(value, None);
        assert_eq!(*control.cancellations.lock().unwrap(), vec![(1, 10)]);
    }

    #[test]
    fn replacement_is_registered_only_when_installed_with_reservation() {
        let control = RecordingOperationControl::default();
        let mut value = Some(1);
        {
            let mut ownership = OperationOwnership::new(&mut value, 10, &control);

            assert_eq!(ownership.take_for_cancellation(), Some((1, 10)));
            assert!(ownership.value.is_none());
            assert!(control.registrations.lock().unwrap().is_empty());
            ownership.install(2, 20);
            assert_eq!(*ownership.value(), 2);
            ownership.complete();
        }
        assert_eq!(value, Some(2));
        assert_eq!(*control.registrations.lock().unwrap(), vec![2]);
        assert!(control.cancellations.lock().unwrap().is_empty());
    }

    #[test]
    fn cancellation_take_cannot_discard_the_same_value_twice() {
        let control = RecordingOperationControl::default();
        let mut value = Some(1);
        let mut ownership = OperationOwnership::new(&mut value, 10, &control);

        assert_eq!(ownership.take_for_cancellation(), Some((1, 10)));
        assert_eq!(ownership.take_for_cancellation(), None);
        drop(ownership);
        assert!(control.cancellations.lock().unwrap().is_empty());
    }

    #[test]
    fn failed_replacement_reservation_leaves_ownership_empty_and_unregistered() {
        let control = RecordingOperationControl::default();
        let mut value = Some(1);
        {
            let mut ownership = OperationOwnership::new(&mut value, 10, &control);
            assert_eq!(ownership.take_for_cancellation(), Some((1, 10)));
            let _replacement_acquired_before_failed_reservation = 2;
        }
        assert_eq!(value, None);
        assert!(control.registrations.lock().unwrap().is_empty());
        assert!(control.cancellations.lock().unwrap().is_empty());
    }

    #[test]
    fn poisoned_transaction_operation_prevents_reuse() {
        let mut poisoned = false;
        {
            let _operation = TransactionConnectionUse::begin(&mut poisoned).unwrap();
        }
        assert!(poisoned);
        assert!(TransactionConnectionUse::begin(&mut poisoned).is_err());
    }

    #[test]
    fn completed_transaction_operation_allows_reuse() {
        let mut poisoned = false;
        TransactionConnectionUse::begin(&mut poisoned)
            .unwrap()
            .complete();
        assert!(!poisoned);
        assert!(TransactionConnectionUse::begin(&mut poisoned).is_ok());
    }
}
