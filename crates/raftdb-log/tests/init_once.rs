use raftdb_log::{init, LogConfig, LogError};

#[test]
fn init_installs_once_and_a_second_call_is_an_error_not_a_panic() {
    assert_eq!(init(&LogConfig::default()), Ok(()));
    assert_eq!(
        init(&LogConfig::default()),
        Err(LogError::AlreadyInitialized)
    );
    tracing::info!("logging after init works");
}
