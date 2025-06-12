use tracing_subscriber::{
    EnvFilter,
    layer::SubscriberExt,
    util::{SubscriberInitExt, TryInitError},
};

pub fn setup_tracing_test(module: &str) -> Result<(), TryInitError> {
    let io_layer = tracing_subscriber::fmt::layer()
        .with_ansi(true)
        .with_line_number(true);

    let level_layer = EnvFilter::builder()
        .with_default_directive(format!("{module}=trace").parse().expect("invalid module"))
        .from_env_lossy();
    tracing_subscriber::registry()
        .with(io_layer)
        .with(level_layer)
        .try_init()?;
    Ok(())
}

pub fn setup_tracing_test_many(
    modules: impl IntoIterator<Item = &'static str>,
) -> Result<(), TryInitError> {
    let io_layer = tracing_subscriber::fmt::layer()
        .with_ansi(true)
        .with_line_number(true);

    let directives = modules
        .into_iter()
        .fold(EnvFilter::default(), |filter, module| {
            filter.add_directive(format!("{module}=trace").parse().expect("invalid module"))
        });

    tracing_subscriber::registry()
        .with(io_layer)
        .with(directives)
        .try_init()?;
    Ok(())
}
