#[cfg(not(all(target_os = "wasi", target_env = "p2")))]
mod config_write;
#[cfg(not(all(target_os = "wasi", target_env = "p2")))]
mod native;
mod routing;
mod shared;
mod shell;
#[cfg(all(target_os = "wasi", target_env = "p2"))]
mod wasi;
fn main() -> std::process::ExitCode {
    #[cfg(all(target_os = "wasi", target_env = "p2"))]
    {
        wasi::main()
    }
    #[cfg(not(all(target_os = "wasi", target_env = "p2")))]
    {
        native::main()
    }
}
