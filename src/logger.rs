use env_logger;
use indicatif::MultiProgress;
use log;
use log::kv;
use serde_json::{json, to_value};
use std::io;
use std::io::Write;
use std::result;

use crate::Result;

#[derive(Default)]
struct KvVisitor;

impl<'a> kv::VisitSource<'a> for KvVisitor {
    fn visit_pair(&mut self, key: kv::Key, value: kv::Value<'a>) -> result::Result<(), kv::Error> {
        let v = to_value(value).map_err(|e| kv::Error::boxed(e))?;
        print!("\"{}\": {}, ", key, v);
        Ok(())
    }
}
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

            let mut vis = KvVisitor::default();
            record
                .key_values()
                .visit(&mut vis)
                .map_err(|e| io::Error::new(std::io::ErrorKind::Other, e))?;

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
