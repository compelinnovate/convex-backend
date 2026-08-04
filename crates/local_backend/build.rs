use vergen::EmitBuilder;

fn main() -> anyhow::Result<()> {
    println!("cargo:rustc-check-cfg=cfg(local_backend_jemalloc)");
    let target_arch = std::env::var("CARGO_CFG_TARGET_ARCH")?;
    let target_os = std::env::var("CARGO_CFG_TARGET_OS")?;
    let target_vendor = std::env::var("CARGO_CFG_TARGET_VENDOR")?;
    // Keep this target set aligned with Cargo.toml. Windows cannot replace
    // jemalloc's configuration globals, Android and DragonFly cannot use this
    // dependency's process-wide allocator override, and tikv-jemalloc-sys
    // rejects the remaining targets below.
    if std::env::var_os("CARGO_FEATURE_JEMALLOC").is_some()
        && target_arch != "wasm32"
        && target_os != "windows"
        && target_os != "android"
        && target_os != "bitrig"
        && target_os != "dragonfly"
        && target_os != "fuchsia"
        && target_os != "redox"
        && target_vendor != "rumprun"
    {
        println!("cargo:rustc-cfg=local_backend_jemalloc");
    }

    // Recompile when there's a new git hash for beacon.
    // This is a workaround for https://github.com/rustyhorde/vergen/issues/174
    // In docker builds, we need a way to pass overrides to Vergen when there's no
    // actual git repo. We'll try emitting as usual, then fall back to env vars
    // that might have been set in the docker build before falling back to empty
    // strings.
    if EmitBuilder::builder()
        .git_sha(false)
        .git_commit_timestamp()
        .fail_on_error()
        .emit()
        .is_err()
    {
        println!("cargo:rerun-if-changed=build.rs");
        println!(
            "cargo:rustc-env=VERGEN_GIT_SHA={}",
            option_env!("VERGEN_GIT_SHA").unwrap_or_else(|| "unknown")
        );
        println!(
            "cargo:rustc-env=VERGEN_GIT_COMMIT_TIMESTAMP={}",
            option_env!("VERGEN_GIT_COMMIT_TIMESTAMP").unwrap_or_else(|| "unknown")
        );
    }
    Ok(())
}
