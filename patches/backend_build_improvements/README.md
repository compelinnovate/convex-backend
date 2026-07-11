# Backend Build Improvements

This patch makes the backend Docker build honor the Cargo profile the operator selected and keeps
expensive dependency work reusable across first-party source changes. The default path still builds
a normal release image and applies the existing final GNU `strip` pass for image size. Profiling or
debugging builds preserve their requested artifact shape, while ordinary builds use shallow Cargo
Git dependencies, shared Cargo caches, and no redundant eager JavaScript workspace install.

## Local Cargo prerequisites

The workspace Cargo configuration supplies a GNU/Linux-only `-include cstdint` workaround for
bundled RocksDB 8.10 under GCC 15. Explicit caller `CXXFLAGS` values retain precedence, and the
setting does not affect Apple or MSVC builds.

`scripts/run_cargo.sh` resolves only the repository-pinned `protoc` through `mise` instead of
installing unrelated repository tools. On Linux it also supplies installed Clang resource headers
to bindgen when the distro library cannot locate them. Explicit `PROTOC` and
`BINDGEN_EXTRA_CLANG_ARGS` values retain precedence. The scoped Rust workflow uses this wrapper for
formatting, checks, builds, and tests after the pinned JavaScript tools and workspace dependencies
have been installed once.

The direct operator problem is backend profiling and debugging. A self-hosted
operator may need an optimized backend binary with symbol information so `perf`,
`addr2line`, flamegraph tooling, or a debugger can attribute CPU time to useful
Rust frames. The previous Dockerfile path assumed the release profile and then
stripped the copied binaries. That is fine for the normal production image. It
is wrong for a build where a selected custom profile or a forwarded Cargo
environment override explicitly asks to keep debug information.

## Why the old shape was unsafe

The upstream Dockerfile treated release as the fixed build shape. It used
release cargo-chef and release cargo builds, copied binaries from
`target/release`, and applied a final `strip` unless the legacy Docker `debug`
arg was set.

That made nonstandard backend builds awkward in several ways:

- `CARGO_PROFILE_RELEASE_DEBUG=1` could make Cargo emit debuginfo, then the
  Dockerfile's later `strip` removed it from `convex-local-backend` and
  `generate_key`.
- `CARGO_PROFILE_RELEASE_STRIP=none` or another explicit Cargo strip setting
  could be overridden by the Dockerfile's stronger GNU `strip` pass.
- Cargo's built-in profile names do not all match their target directories:
  `dev` and `test` use `target/debug`, while `release` and `bench` use
  `target/release`.
- Guessing a target directory risks packaging the wrong binary when a cached
  artifact exists. It also misses target-triple directories and companion
  artifacts such as packed split-debuginfo files.

The patch is intentionally strict. If Cargo does not produce both requested
executables, the Docker build fails before the runtime image is assembled.

## What changes

`self-hosted/docker-build/Dockerfile.backend` now accepts `CARGO_BUILD_PROFILE`,
defaulting to `release`, and passes that profile through both cargo-chef and the
real backend/keybroker builds:

```sh
--build-arg CARGO_BUILD_PROFILE=release
```

The Dockerfile also forwards the release-profile overrides used for profiling
builds:

```sh
--build-arg CARGO_PROFILE_RELEASE_DEBUG=1
--build-arg CARGO_PROFILE_RELEASE_STRIP=none
```

Non-empty overrides are exported in the cargo-chef layer and in the final cargo
build layer, so dependency builds and first-party builds see the same profile
configuration. Empty Docker ARG defaults are explicitly unset before Cargo runs
because Cargo rejects empty profile override environment variables.

`.cargo/config.toml` is copied into both cargo-chef stages before `prepare` or
`cook` runs. The cooked dependencies and final build therefore use the same
repository environment and target rustflags. Without that copy, the final build
would invalidate dependencies cooked without settings such as
`tokio_unstable`, the custom V8 mirror, and the AArch64 CPU/frame-pointer flags.

`CARGO_TARGET_DIR` is fixed at `/convex/target` in the build stage. This keeps
cargo-chef and the final build on the same mounted cache and prevents a Cargo
config `target-dir` override from separating their outputs. Final artifacts are
copied out of that cache explicitly, so BuildKit can commit them to the build
layer and pass them to the runtime stage.

The BuildKit Cargo caches have explicit identities and use `sharing=locked` in
both Cargo layers. Native target artifacts are separated by `TARGETARCH`.
Serialization is required because cargo-chef removes dummy workspace artifacts
after Cargo releases its target lock; without the BuildKit lock, that cleanup
can race a concurrent real build using the same cache. The git and registry
mounts are also locked because they mount only subdirectories of Cargo home and
therefore do not share Cargo's global package-cache lock between containers.
Cache contents remain an optimization only: an empty or replaced cache causes a
rebuild, not a different runtime artifact.

The repository pins a nightly Rust toolchain, so the final builds use Cargo's
unstable `--artifact-dir` option to copy requested final artifacts out of the
target cache and into `/convex/backend-artifacts`:

```sh
cargo -Z unstable-options build \
  --artifact-dir /convex/backend-artifacts \
  --profile "$CARGO_BUILD_PROFILE" \
  -p local_backend --bin convex-local-backend
```

This avoids encoding Cargo's profile-to-directory mapping in the Dockerfile and
also works if a target triple adds another directory level. Cargo copies packed
split-debuginfo companions such as `.dwp` files with the executable. Explicit
`test -x` checks verify both required binaries before the runtime stage copies
the complete artifact directory.

If `CARGO_BUILD_PROFILE` is explicitly empty, the build fails before cargo-chef
with a direct error. If the selected profile is invalid, if Cargo fails, or if a
required executable is missing, the Docker build also stops.

The final GNU `strip` pass is now limited to the plain default release image. It
is skipped when:

- the legacy Docker `debug` build arg is set;
- `CARGO_BUILD_PROFILE` is anything other than `release`;
- `CARGO_PROFILE_RELEASE_STRIP` is set to any non-empty value;
- `CARGO_PROFILE_RELEASE_DEBUG` requests debuginfo.

That keeps the small default release image behavior while preserving symbols for
explicit debug and profiling builds.

The Dockerfile also provisions `protoc` with `mise` before the cargo layers.
`PROTOC` is set for `cargo chef cook` and for the final cargo build. That keeps
protobuf-using crates buildable in cached dependency layers and in the real
backend build, instead of relying on a host-level `protoc` or discovering the
missing tool only after a later layer invalidates.

## Dependency fetch and source layering

The chef and build stages set `CARGO_UNSTABLE_GIT=shallow-deps`, using the repository-pinned nightly
Cargo support for shallow locked Git dependencies. The prepare, cook, and final build steps mount
the same persistent Cargo Git database, Git checkout, and registry caches. The established Git
database cache identity remains unchanged so existing builders retain it.

All Cargo cache mounts use `sharing=locked`. While holding those mounts exclusively, each Cargo step
removes stale Git lockfiles that a cancelled BuildKit step may have left behind. The cleanup is not
run concurrently with another build using the same repositories.

The pinned pnpm and Turbo tools remain installed before Cargo dependency cooking. The full
JavaScript workspace is copied later with the remaining first-party source. The final backend build
retains the pnpm store cache; isolate's build script performs the frozen workspace install with
lifecycle scripts disabled and builds the packages embedded in the backend binary. The Dockerfile
does not run a separate eager `just install-js`, which would run unrelated workspace lifecycle
downloads whenever package source changes.

### Observed build evidence

On one constrained builder, the previous Cargo path was stopped after more than 1 hour 20 minutes
while it repeatedly fetched complete Git dependency histories. With shallow fetches and shared
caches, the corrected cargo-chef prepare completed in 12 minutes 14 seconds, dependency cooking in
46 seconds, and the final two backend binaries in about 4 minutes 36 seconds.

In that corrected build, the still-eager JavaScript install took 13 minutes 20 seconds and included
a Puppeteer browser download. That observation motivated removing the layer. The image was not
rebuilt after the JavaScript-layer removal, so the patch does not claim a measured replacement time
for that step. Source changes still cause the final backend layer to run the scripts-disabled pnpm
install and required JavaScript builds; the persistent store avoids repeating package transfers.

## How to use it

For the normal production-sized backend image, use the default Dockerfile path:

```sh
docker build -f self-hosted/docker-build/Dockerfile.backend .
```

That builds `release` and runs the final GNU `strip` pass.

For an optimized profiling build with release debuginfo, pass the Cargo release
overrides explicitly:

```sh
docker build \
  -f self-hosted/docker-build/Dockerfile.backend \
  --build-arg CARGO_BUILD_PROFILE=release \
  --build-arg CARGO_PROFILE_RELEASE_DEBUG=1 \
  --build-arg CARGO_PROFILE_RELEASE_STRIP=none \
  .
```

That asks Cargo for release debuginfo and prevents the Dockerfile from stripping
the copied backend binaries afterward.

For a custom Cargo profile, define the profile in the workspace and select it:

```sh
docker build \
  -f self-hosted/docker-build/Dockerfile.backend \
  --build-arg CARGO_BUILD_PROFILE=slim-release \
  .
```

Cargo copies that profile's final binary artifacts into the runtime artifact
directory. The Dockerfile does not need to know the profile's target directory.

On Linux, use embedded debug information or `split-debuginfo = "packed"` when
symbols must be present in the runtime image. Cargo reports packed `.dwp` files
as final artifacts, so `--artifact-dir` and the runtime-stage directory copy
preserve them. `split-debuginfo = "unpacked"` leaves per-codegen-unit `.dwo`
files inside the target cache instead; those cache internals are intentionally
not copied into the runtime image.

The legacy `debug` Docker build arg still skips the final GNU `strip`, but it
does not tell Cargo to emit debuginfo. Use Cargo profile settings when the image
needs symbols.

The legacy strip decision can inspect Docker ARG values, but it cannot infer
that a source checkout changed `[profile.release]` or set release-profile
variables inside `.cargo/config.toml`. A release-profile customization that
must preserve Cargo's output must also pass the corresponding
`CARGO_PROFILE_RELEASE_DEBUG` or `CARGO_PROFILE_RELEASE_STRIP` build arg (or the
legacy `debug` arg). Custom profiles do not have this limitation because every
non-`release` profile skips the final GNU strip.

## Scope

This patch does not make profiling easy by itself. It does not install perf tooling in the runtime
image, change kernel perf settings, tune isolate workers, or improve backend runtime performance.
It makes the backend image faithful to custom profiles and explicit release-profile build args so a
profiling build is actually a profiling build. It also does not make every JavaScript workspace
build incremental; an empty BuildKit cache still performs a cold dependency fetch.

Use the default release path when image size is the priority. Use an explicit
profile or release debuginfo/strip override when symbol fidelity is the
priority.

## Verification

Review confirmed the Dockerfile build, copy, and strip shell fragments pass
`bash -n`. A strip decision-matrix check passed under bash: only plain `release`
runs the final GNU `strip`; `debug`, `dev`, `test`, `bench`, custom profiles,
explicit release strip overrides, and non-false release debug overrides preserve
Cargo's selected artifact shape.

`docker buildx build --check -f self-hosted/docker-build/Dockerfile.backend .`
also passed with no warnings. A minimal BuildKit fixture confirmed that the
automatic `TARGETARCH` argument resolves inside a cache-mount ID. A
dependency-free temporary Cargo fixture confirmed `dev`, `test`, `release`,
`bench`, and custom-profile builds; target-directory precedence; and copying of
packed `.dwp` companions through `--artifact-dir`.

The neighboring self-hosted Compose and script checks passed in the same review
area:

```sh
docker compose -f self-hosted/docker/docker-compose.yml config --quiet
bash -n self-hosted/docker-build/run_backend.sh self-hosted/docker-build/generate_admin_key.sh
```

A local custom-profile cargo build probe was blocked before checking the backend crate because the
host was missing `cmake` for `aws-lc-sys`.

A later normal release image build with shallow Cargo fetches and shared caches produced both
backend binaries and a runnable image with the timings recorded above. The subsequent
JavaScript-layer removal is supported by the isolate build-script contract: cargo-chef uses dummy
workspace build scripts, while the real isolate build runs after the full source copy and performs
the frozen, scripts-disabled install before its filtered Turbo build. No Docker image build was run
after removing the eager install.
