//! Top-level configuration state registered by the user.

use std::{
    collections::{
        BTreeMap,
        BTreeSet,
    },
    path::{
        Component,
        PathBuf,
    },
    str::FromStr,
};

use common::{
    auth::{
        AuthInfo,
        SerializedAuthInfo,
    },
    obj,
    types::{
        IndexDiff,
        ModuleEnvironment,
    },
};
use database::{
    SchemaDiff,
    SerializedSchemaDiff,
};
use errors::ErrorMetadata;
use serde::{
    Deserialize,
    Serialize,
};
use sync_types::{
    module_path::ACTIONS_DIR,
    CanonicalizedModulePath,
    ModulePath,
};
use value::{
    codegen_convex_serialization,
    remove_string,
    sha256::Sha256Digest,
    ConvexArray,
    ConvexObject,
    ConvexValue,
};

use crate::{
    auth::types::{
        AuthDiff,
        AuthInfoPersisted,
    },
    cron_jobs::types::CronIdentifier,
    modules::module_versions::{
        ModuleSource,
        SourceMap,
    },
    source_packages::types::NodeExecutorPoolTopology,
};

/// User-specified module definition. See [`ModuleMetadata`] and associated
/// structs for the corresponding module metadata used internally by the system.
#[derive(Debug, Clone, PartialOrd, Ord, PartialEq, Eq)]
pub struct ModuleConfig {
    /// Relative path to the module.
    pub path: ModulePath,
    /// Module source.
    pub source: ModuleSource,
    /// The module's source map (if available).
    pub source_map: Option<SourceMap>,
    /// The environment is bundled to run in.
    pub environment: ModuleEnvironment,
    /// The dedicated local Node executor pool required by this module.
    pub node_pool: Option<NodeExecutorPoolName>,
}

/// A module definition that includes a hash instead of the source and
/// source_map, to be used when the module already exists on the server.
#[derive(Debug)]
pub struct ModuleHashConfig {
    /// Relative path to the module.
    pub path: ModulePath,
    /// The environment is bundled to run in.
    pub environment: ModuleEnvironment,
    /// The dedicated local Node executor pool required by this module.
    pub node_pool: Option<NodeExecutorPoolName>,
    // This is a hash of source + source_map.
    pub sha256: Sha256Digest,
}

/// An application-declared local Node executor pool name.
#[derive(Debug, Clone, PartialOrd, Ord, PartialEq, Eq)]
pub struct NodeExecutorPoolName(String);

impl FromStr for NodeExecutorPoolName {
    type Err = anyhow::Error;

    fn from_str(name: &str) -> anyhow::Result<Self> {
        let bytes = name.as_bytes();
        let valid = !bytes.is_empty()
            && bytes.len() <= 32
            && bytes[0].is_ascii_lowercase()
            && bytes
                .iter()
                .skip(1)
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'_');
        if !valid || name == "default" {
            anyhow::bail!(ErrorMetadata::bad_request(
                "InvalidNodeExecutorPoolName",
                "Node executor pool names must match [a-z][a-z0-9_]{0,31}, and 'default' is \
                 reserved",
            ));
        }
        Ok(Self(name.to_owned()))
    }
}

impl AsRef<str> for NodeExecutorPoolName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for NodeExecutorPoolName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

pub const NODE_EXECUTOR_POOL_ENVIRONMENT_PREFIX: &str = "node:pool:";

pub fn format_module_environment(
    environment: ModuleEnvironment,
    node_pool: Option<&NodeExecutorPoolName>,
) -> String {
    match node_pool {
        Some(pool) => {
            assert_eq!(environment, ModuleEnvironment::Node);
            format!("{NODE_EXECUTOR_POOL_ENVIRONMENT_PREFIX}{pool}")
        },
        None => environment.to_string(),
    }
}

fn parse_module_environment_and_pool_inner(
    value: &str,
    explicit_pool: Option<String>,
    allow_legacy_pool_without_marker: bool,
) -> anyhow::Result<(ModuleEnvironment, Option<NodeExecutorPoolName>)> {
    if let Some(pool_name) = value.strip_prefix(NODE_EXECUTOR_POOL_ENVIRONMENT_PREFIX) {
        let pool: NodeExecutorPoolName = pool_name.parse()?;
        if let Some(explicit_pool) = explicit_pool {
            anyhow::ensure!(
                explicit_pool == pool.as_ref(),
                "Node pool metadata does not match the module environment"
            );
        }
        return Ok((ModuleEnvironment::Node, Some(pool)));
    }

    let environment = value.parse()?;
    if allow_legacy_pool_without_marker
        && environment == ModuleEnvironment::Node
        && let Some(pool) = explicit_pool
    {
        return Ok((environment, Some(pool.parse()?)));
    }
    anyhow::ensure!(
        explicit_pool.is_none(),
        "Node pool metadata requires a pool-bearing Node environment"
    );
    Ok((environment, None))
}

/// Parse an API module environment. The required environment marker prevents
/// an older backend from silently ignoring the optional pool field.
pub fn parse_module_environment_and_pool(
    value: &str,
    explicit_pool: Option<String>,
) -> anyhow::Result<(ModuleEnvironment, Option<NodeExecutorPoolName>)> {
    parse_module_environment_and_pool_inner(value, explicit_pool, false)
}

/// Parse durable or archive metadata, including records written by the first
/// pool protocol before the required environment marker was retained there.
pub fn parse_persisted_module_environment_and_pool(
    value: &str,
    explicit_pool: Option<String>,
) -> anyhow::Result<(ModuleEnvironment, Option<NodeExecutorPoolName>)> {
    parse_module_environment_and_pool_inner(value, explicit_pool, true)
}

pub fn node_executor_pool_topology<'a>(
    modules: impl IntoIterator<Item = &'a ModuleConfig>,
) -> anyhow::Result<NodeExecutorPoolTopology> {
    let mut topology = BTreeMap::new();
    let mut default_routes = BTreeSet::new();
    for module in modules {
        let path = module.path.clone().canonicalize();
        if path.is_system() || path.is_deps() {
            anyhow::ensure!(
                module.node_pool.is_none(),
                ErrorMetadata::bad_request(
                    "InvalidNodeExecutorPoolModule",
                    "A Node executor pool can only be assigned to a Node action module",
                )
            );
            continue;
        }

        let directives = bundled_node_directives(&module.source)?;
        if path.is_http()
            || path.is_cron()
            || path.as_str() == "schema.js"
            || path.as_str() == AUTH_CONFIG_FILE_NAME
        {
            anyhow::ensure!(
                module.node_pool.is_none() && directives.pool.is_none(),
                ErrorMetadata::bad_request(
                    "InvalidNodeExecutorPoolModule",
                    "A Node executor pool can only be assigned to a Node action module",
                )
            );
            continue;
        }

        anyhow::ensure!(
            directives.pool == module.node_pool,
            ErrorMetadata::bad_request(
                "NodeExecutorPoolDirectiveMismatch",
                "The bundled Node pool directive does not match the module metadata",
            )
        );

        let Some(pool) = &module.node_pool else {
            if module.environment == ModuleEnvironment::Node {
                anyhow::ensure!(
                    default_routes.insert(path),
                    "Node executor default topology contains a duplicate module"
                );
            }
            continue;
        };
        if module.environment != ModuleEnvironment::Node {
            anyhow::bail!(ErrorMetadata::bad_request(
                "NodeExecutorPoolRequiresNode",
                "A Node executor pool requires the Node environment",
            ));
        }
        anyhow::ensure!(
            directives.uses_node,
            ErrorMetadata::bad_request(
                "NodeExecutorPoolRequiresNodeDirective",
                "A Node executor pool declaration requires a separate `use node` directive",
            )
        );
        anyhow::ensure!(
            topology.insert(path, pool.clone()).is_none(),
            "Node executor pool topology contains a duplicate module"
        );
    }
    Ok(NodeExecutorPoolTopology::new_complete(
        topology,
        default_routes,
    ))
}

#[derive(Default)]
struct BundledNodeDirectives {
    uses_node: bool,
    pool: Option<NodeExecutorPoolName>,
}

fn bundled_node_directives(source: &ModuleSource) -> anyhow::Result<BundledNodeDirectives> {
    let mut directives = BundledNodeDirectives::default();
    let bytes = source.as_bytes();
    let mut cursor = 0;
    if bytes.starts_with(b"#!") {
        cursor = bytes
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(bytes.len(), |newline| newline + 1);
    }
    loop {
        while bytes.get(cursor).is_some_and(u8::is_ascii_whitespace) {
            cursor += 1;
        }
        let Some(&quote @ (b'\'' | b'"')) = bytes.get(cursor) else {
            break;
        };
        let value_start = cursor + 1;
        cursor = value_start;
        while let Some(&byte) = bytes.get(cursor) {
            if byte == b'\\' {
                cursor = cursor.saturating_add(2);
                continue;
            }
            if byte == quote {
                break;
            }
            cursor += 1;
        }
        anyhow::ensure!(
            bytes.get(cursor) == Some(&quote),
            "Bundled JavaScript contains an unterminated directive"
        );
        let directive = &source[value_start..cursor];
        cursor += 1;
        while bytes.get(cursor).is_some_and(u8::is_ascii_whitespace) {
            cursor += 1;
        }
        anyhow::ensure!(
            bytes.get(cursor) == Some(&b';'),
            "Bundled JavaScript directive is missing its statement terminator"
        );
        cursor += 1;
        if directive == "use node" {
            directives.uses_node = true;
            continue;
        }
        if !directive.starts_with("use node pool") {
            continue;
        }
        let name = directive.strip_prefix("use node pool:").ok_or_else(|| {
            ErrorMetadata::bad_request(
                "InvalidNodeExecutorPoolDirective",
                "A bundled Node pool directive must use `use node pool:<name>`",
            )
        })?;
        anyhow::ensure!(
            directives.pool.is_none(),
            ErrorMetadata::bad_request(
                "MultipleNodeExecutorPoolDirectives",
                "A module can declare only one Node executor pool",
            )
        );
        directives.pool = Some(name.parse()?);
    }
    Ok(directives)
}

/// This is not safe to use since convex 0.12.0, where we allow defining actions
/// outside of the /actions subfolder. This method should only be used for old
/// cli clients and source packages where environment is not set.
pub fn deprecated_extract_environment_from_path(p: String) -> anyhow::Result<ModuleEnvironment> {
    let path = PathBuf::from(&p);
    let components = path
        .components()
        .map(|component| match component {
            Component::Normal(c) => c
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("Path {p} contains an invalid Unicode character")),
            Component::RootDir => {
                anyhow::bail!("Module paths must be relative ({p} is absolute)")
            },
            c => anyhow::bail!("Invalid path component {c:?} in {p}"),
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    // This is the old way to indicate a module should execute in Node.js.
    let environment = if matches!(&components[..], &[ACTIONS_DIR, ..]) {
        ModuleEnvironment::Node
    } else {
        ModuleEnvironment::Isolate
    };
    Ok(environment)
}

pub const AUTH_CONFIG_FILE_NAME: &str = "auth.config.js";

/// Representation of Convex config metadata deployed by the client. This
/// metadata isn't written to a table but is instead normalized and represented
/// by state in the other metadata tables.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigMetadata {
    /// The local directory on the client containing modules.
    pub functions: String,
    /// Authentication info. Empty if this instance has not set up
    /// authentication.
    pub auth_info: Vec<AuthInfo>,
}

impl ConfigMetadata {
    /// Create new empty config metadata for a new instance.
    pub fn new() -> Self {
        Self {
            functions: "convex/".to_string(),
            auth_info: vec![],
        }
    }

    pub fn from_file(file: ConfigFile, auth_info: Vec<AuthInfo>) -> Self {
        Self {
            functions: file.functions,
            auth_info,
        }
    }
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigFile {
    pub functions: String,
    // Deprecated, moved to AuthConfig.providers
    pub auth_info: Option<Vec<SerializedAuthInfo>>,
}

impl TryFrom<ConfigMetadata> for ConvexObject {
    type Error = anyhow::Error;

    fn try_from(m: ConfigMetadata) -> anyhow::Result<Self> {
        let mut config = BTreeMap::new();
        config.insert("functions".parse()?, ConvexValue::try_from(m.functions)?);

        // The auth config was moved from `authInfo` to `auth.config.js` in modules,
        // do not include it in the config response if it is empty.
        if !m.auth_info.is_empty() {
            let auth_info = m
                .auth_info
                .into_iter()
                .map(|v| Ok(ConvexObject::try_from(AuthInfoPersisted(v))?.into()))
                .collect::<anyhow::Result<Vec<ConvexValue>>>()?
                .try_into()?;
            config.insert("authInfo".parse()?, auth_info);
        }
        config.try_into()
    }
}

impl TryFrom<ConfigMetadata> for ConvexValue {
    type Error = anyhow::Error;

    fn try_from(value: ConfigMetadata) -> Result<Self, Self::Error> {
        Ok(ConvexObject::try_from(value)?.into())
    }
}

impl TryFrom<ConvexObject> for ConfigMetadata {
    type Error = anyhow::Error;

    fn try_from(o: ConvexObject) -> Result<Self, Self::Error> {
        let mut fields: BTreeMap<_, _> = o.into();
        let functions = match fields.remove("functions") {
            Some(ConvexValue::String(s)) => s.into(),
            _ => anyhow::bail!(
                "Missing or invalid functions field for ConfigMetadata: {:?}",
                fields,
            ),
        };
        let auth_info = match fields.remove("authInfo") {
            Some(v) => ConvexArray::try_from(v)?
                .into_iter()
                .map(|v| {
                    let parsed: AuthInfoPersisted = ConvexObject::try_from(v)?.try_into()?;
                    Ok(parsed.0)
                })
                .collect::<anyhow::Result<Vec<AuthInfo>>>()?,
            _ => vec![],
        };
        Ok(Self {
            functions,
            auth_info,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ConfigDiff {
    pub auth_diff: AuthDiff,
    pub udf_server_version_diff: Option<UdfServerVersionDiff>,
    pub module_diff: ModuleDiff,
    pub cron_diff: CronDiff,
    pub index_diff: ConfigIndexDiff,
    pub schema_diff: Option<SchemaDiff>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializedConfigDiff {
    pub auth: AuthDiff,
    // NOTE: not camel-case
    pub server_version: Option<UdfServerVersionDiff>,
    pub modules: ModuleDiff,
    pub crons: Option<CronDiff>,
    pub indexes: Option<ConfigIndexDiff>,
    pub schema: Option<SerializedSchemaDiff>,
}

codegen_convex_serialization!(ConfigDiff, SerializedConfigDiff);

impl TryFrom<ConfigDiff> for SerializedConfigDiff {
    type Error = anyhow::Error;

    fn try_from(value: ConfigDiff) -> Result<Self, Self::Error> {
        Ok(Self {
            auth: value.auth_diff,
            server_version: value.udf_server_version_diff,
            modules: value.module_diff,
            crons: Some(value.cron_diff),
            indexes: Some(value.index_diff),
            schema: value.schema_diff.map(TryFrom::try_from).transpose()?,
        })
    }
}

impl TryFrom<SerializedConfigDiff> for ConfigDiff {
    type Error = anyhow::Error;

    fn try_from(obj: SerializedConfigDiff) -> anyhow::Result<Self> {
        Ok(Self {
            auth_diff: obj.auth,
            udf_server_version_diff: obj.server_version,
            module_diff: obj.modules,
            cron_diff: obj.crons.unwrap_or_default(),
            index_diff: obj.indexes.unwrap_or_default(),
            schema_diff: obj.schema.map(TryFrom::try_from).transpose()?,
        })
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ConfigIndexDiff {
    pub added: Vec<String>,
    pub dropped: Vec<String>,
}

impl From<IndexDiff> for ConfigIndexDiff {
    fn from(value: IndexDiff) -> Self {
        Self {
            added: value
                .added
                .into_iter()
                .map(|index_metadata| index_metadata.name.to_string())
                .collect(),
            dropped: value
                .dropped
                .into_iter()
                .map(|index_metadata| index_metadata.name.to_string())
                .collect(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UdfServerVersionDiff {
    pub previous_version: String,
    pub next_version: String,
}
impl TryFrom<UdfServerVersionDiff> for ConvexObject {
    type Error = anyhow::Error;

    fn try_from(value: UdfServerVersionDiff) -> Result<Self, Self::Error> {
        obj!("previous_version" => value.previous_version, "next_version" => value.next_version)
    }
}

impl TryFrom<ConvexObject> for UdfServerVersionDiff {
    type Error = anyhow::Error;

    fn try_from(obj: ConvexObject) -> anyhow::Result<Self> {
        let mut fields = BTreeMap::from(obj);
        Ok(Self {
            previous_version: remove_string(&mut fields, "previous_version")?,
            next_version: remove_string(&mut fields, "next_version")?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModuleDiff {
    pub added: Vec<String>,
    pub removed: Vec<String>,
}

impl ModuleDiff {
    pub fn new(
        added_module_paths: BTreeSet<CanonicalizedModulePath>,
        removed_module_paths: BTreeSet<CanonicalizedModulePath>,
    ) -> anyhow::Result<Self> {
        let mut added_functions = Vec::with_capacity(added_module_paths.len());
        for m in added_module_paths {
            if m.is_deps() || m.is_system() {
                continue;
            }
            added_functions.push(m.as_str().to_string());
        }
        let mut removed_functions = Vec::with_capacity(removed_module_paths.len());
        for m in removed_module_paths {
            if m.is_deps() || m.is_system() {
                continue;
            }
            removed_functions.push(m.as_str().to_string());
        }
        Ok(Self {
            added: added_functions,
            removed: removed_functions,
        })
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CronDiff {
    pub added: Vec<String>,
    pub updated: Vec<String>,
    pub deleted: Vec<String>,
}

impl CronDiff {
    pub fn new(
        added_crons: Vec<&CronIdentifier>,
        updated_crons: Vec<&CronIdentifier>,
        deleted_crons: Vec<&CronIdentifier>,
    ) -> Self {
        Self {
            added: added_crons.into_iter().map(|c| c.to_string()).collect(),
            updated: updated_crons.into_iter().map(|c| c.to_string()).collect(),
            deleted: deleted_crons.into_iter().map(|c| c.to_string()).collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn module(source: &str, node_pool: Option<&str>) -> ModuleConfig {
        ModuleConfig {
            path: "consumer.js".parse().unwrap(),
            source: ModuleSource::new(source),
            source_map: None,
            environment: ModuleEnvironment::Node,
            node_pool: node_pool.map(|name| name.parse().unwrap()),
        }
    }

    #[test]
    fn pool_topology_requires_bundled_directive_metadata_agreement() {
        let bundled = "\"use node\";\n\"use node pool:consumer\";\nexport const run = 1;";
        let missing_metadata = module(bundled, None);
        assert!(node_executor_pool_topology([&missing_metadata]).is_err());

        let matching = module(bundled, Some("consumer"));
        let topology = node_executor_pool_topology([&matching]).unwrap();
        assert_eq!(
            topology.get(&"consumer.js".parse().unwrap()),
            matching.node_pool.as_ref()
        );
        assert_eq!(topology.default_route_count(), Some(0));

        let missing_directive = module("\"use node\";\nexport const run = 1;", Some("consumer"));
        assert!(node_executor_pool_topology([&missing_directive]).is_err());

        let minified = module(
            "\"use node\";\"use node pool:consumer\";export const run=1;",
            Some("consumer"),
        );
        node_executor_pool_topology([&minified]).unwrap();

        let missing_node = module(
            "\"use node pool:consumer\";\nexport const run = 1;",
            Some("consumer"),
        );
        assert!(node_executor_pool_topology([&missing_node]).is_err());
    }

    #[test]
    fn pool_topology_accepts_bundled_hashbang() {
        let pooled = module(
            "#!/usr/bin/env node\n\"use node\";\n\"use node pool:consumer\";\nexport const run = \
             1;",
            Some("consumer"),
        );
        node_executor_pool_topology([&pooled]).unwrap();
    }

    #[test]
    fn pool_topology_rejects_retained_directives_on_static_modules() {
        for path in ["http.js", "crons.js", "schema.js", "auth.config.js"] {
            let mut static_module =
                module("\"use node pool:consumer\";\nexport const run = 1;", None);
            static_module.path = path.parse().unwrap();
            static_module.environment = ModuleEnvironment::Isolate;
            assert!(node_executor_pool_topology([&static_module]).is_err());
        }

        let mut dependency = module("\"use node pool:consumer\";\nexport const run = 1;", None);
        dependency.path = "_deps/chunk.js".parse().unwrap();
        dependency.environment = ModuleEnvironment::Isolate;
        node_executor_pool_topology([&dependency]).unwrap();
    }

    #[test]
    fn pool_topology_counts_default_node_action_routes() {
        let ordinary = module("\"use node\";\nexport const run = 1;", None);
        let topology = node_executor_pool_topology([&ordinary]).unwrap();
        assert_eq!(topology.default_route_count(), Some(1));
        assert_eq!(
            topology.default_routes().unwrap(),
            &["consumer.js".parse().unwrap()].into_iter().collect()
        );
    }
}
