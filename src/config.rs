use std::ops::RangeInclusive;
use std::path::Path;
use std::path::absolute;

use anyhow::{Result, anyhow, bail};
use clap;
use serde::{Deserialize, Serialize};
use url::Url;

const MESSAGE_QUEUE_SIZE: usize = 100;
const ARROW_BATCH_SIZE: usize = 1000;
const GCP_LOG_NAME: &str = "solana-parser";

#[derive(Debug, clap::Parser)]
#[command(version, about, long_about = None)]
pub struct Cli {
    #[arg(
        long = "output-dir",
        value_name = "DIR",
        default_value = "data",
        env = "OUTPUT_DIR"
    )]
    pub output_dir: String,

    #[arg(long = "no-progress", default_value_t = false, env = "NO_PROGRESS")]
    pub no_progress: bool,

    #[arg(
        long = "enable-gcloud-log",
        default_value_t = false,
        env = "ENABLE_GCLOUD_LOG"
    )]
    pub gcloud_log: bool,

    #[arg(short = 'e', long = "epochs", value_name = "EPOCHS", env = "EPOCHS")]
    pub epochs: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Configuration {
    pub output_dir: Url,
    pub epochs: RangeInclusive<u64>,

    pub enable_progress: bool,
    pub gcloud_log: Option<String>,
    pub message_queue_size: usize,
    pub arrow_batch_size: usize,
}

impl Configuration {
    pub async fn from_cli(cli: Cli) -> Result<Self> {
        let epochs = cli.epochs.parse::<u64>().map(|i| i..=i).or_else(|_| {
            let parts: Vec<u64> = cli
                .epochs
                .split('-')
                .filter_map(|s| s.parse().ok())
                .collect();
            if parts.len() == 2 && parts[0] <= parts[1] {
                return Ok(parts[0]..=parts[1]);
            }

            bail!(
                "Invalid epoch format: {}. Use a single number or a range like start-end.",
                cli.epochs
            );
        })?;

        Ok(Self {
            output_dir: to_url(&cli.output_dir)?,
            epochs,
            gcloud_log: cli.gcloud_log.then(|| GCP_LOG_NAME.to_string()),
            message_queue_size: MESSAGE_QUEUE_SIZE,
            arrow_batch_size: ARROW_BATCH_SIZE,
            enable_progress: !cli.no_progress,
        })
    }
}

fn to_url(path: &str) -> Result<Url> {
    match Url::parse(path) {
        Ok(url) => Ok(url),
        Err(url::ParseError::RelativeUrlWithoutBase) => {
            let p = Path::new(path).canonicalize().or_else(|_| absolute(path))?;
            Ok(Url::from_file_path(p)
                .map_err(|err| anyhow!("Failed to convert path to file URL: {:?}", err))?)
        }
        Err(err) => Err(err.into()),
    }
}
