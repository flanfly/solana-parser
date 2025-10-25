use env_logger;
use indicatif::MultiProgress;
use log;
use serde_json::json;
use std::io;
use std::io::Write;

use crate::Result;

pub fn setup_log<'a, O: Into<Option<&'a str>>>(gcloud_log: O) -> Result<MultiProgress> {
    let mut builder =
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"));

    if gcloud_log.into().is_some() {
        builder.format(|fmt, record| -> io::Result<()> {
            let s = match record.level() {
                log::Level::Error => "ERROR",
                log::Level::Warn => "WARNING",
                log::Level::Info => "INFO",
                log::Level::Debug => "DEBUG",
                log::Level::Trace => "TRACE",
            };

            let j = json!({
                "severity": s,
                "timestamp": fmt.timestamp_millis().to_string(),
                "message": record.args().to_string(),
            });

            fmt.write(j.to_string().as_bytes())?;
            fmt.write(b"\n")?;
            Ok(())
        });
    }

    let logger = builder.build();
    let level = logger.filter();
    let bars = indicatif::MultiProgress::default();

    indicatif_log_bridge::LogWrapper::new(bars.clone(), logger).try_init()?;
    log::set_max_level(level);

    Ok(bars)
}
