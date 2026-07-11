# Rust crates

## Development workflow

Install the repository-pinned tools and JavaScript workspace dependencies once.
`mise` must satisfy the `min_version` in `mise.toml`. Isolate builds invoke the
pinned `pnpm` and `turbo` binaries from `scripts/node_modules`; install those
before the workspace packages so a missing tool does not surface later as a
Cargo build-script failure.

```sh
mise install --locked protoc
npm ci --prefix scripts
just install-js
```

Use `scripts/run_cargo.sh` so build scripts can find the pinned `protoc` without
making `mise` install unrelated repository tools. The workspace Cargo
configuration supplies the target-specific GCC 15 workaround required when
bundled RocksDB 8.10 is compiled on Linux, and the wrapper supplies installed
Clang resource headers to bindgen when the distro library cannot locate them.
Do not rediscover or repeat ad hoc `CXXFLAGS` or `BINDGEN_EXTRA_CLANG_ARGS`
prefixes at each call site.

```sh
# After each change
scripts/run_cargo.sh fmt -p <package>

# When a change is ready
scripts/run_cargo.sh clippy -p <package> --all-targets
scripts/run_cargo.sh build -p <package>
scripts/run_cargo.sh test -p <package>
scripts/run_cargo.sh test -p <package> "test_name" # for a specific test or test group
```

## Rust style

- Before adding a crate dependency or a new abstraction, check whether existing
  workspace infrastructure already provides the capability and prefer the
  simplest extension that works.
- Match domain enums exhaustively instead of using a `_ =>` catch-all so adding
  a variant causes a compile error.
- Use self-documenting domain types: prefer named structs over positional
  tuples, enums over boolean flags, and `Duration` or a newtype over bare
  numbers with implicit units.
- Prefer the `anyhow::Context` trait (`.context(...)`/`.with_context(...)`) over
  `.ok_or_else(|| anyhow::anyhow!(...))` or `.map_err(|_| anyhow::anyhow!(...))`
  for attaching a message to a `Result` or `Option`; it says the same thing more
  concisely.
- Express type conversions as `From`/`TryFrom` impls rather than inherent
  methods like `into_foo()` or `to_foo()`. The trait impl composes with `?`,
  generic bounds, and `impl Into<T>` arguments; a bespoke method does not. Reach
  for an inherent method only when the conversion needs arguments or context the
  trait signature cannot carry.
