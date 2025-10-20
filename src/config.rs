use std::path::{Path, PathBuf};

use clap;
use log::info;
use serde::{Deserialize, Serialize};
use tokio::fs;
use toml;

use crate::Result;

const DEFAULT_CHECKPOINT_FILE: &str = "checkpoint.toml";

#[derive(Debug, clap::Parser)]
#[command(version, about, long_about = None)]
pub struct Cli {
    #[arg(
        long = "checkpoint-file",
        value_name = "FILE",
        default_value = DEFAULT_CHECKPOINT_FILE
    )]
    pub checkpoint_file: PathBuf,

    #[arg(long = "data-dir", value_name = "DIR", default_value = "data")]
    pub data_dir: PathBuf,

    #[arg(long = "tick-queue-size", default_value = "1000")]
    pub tick_queue_size: usize,

    #[arg(long = "tick-batch-size", default_value = "10000")]
    pub tick_batch_size: usize,

    #[arg(long = "token-queue-size", default_value = "10")]
    pub token_queue_size: usize,

    #[arg(long = "token-batch-size", default_value = "100")]
    pub token_batch_size: usize,

    #[command(flatten)]
    pub input: InputGroup,
}

#[derive(Debug, clap::Args)]
#[group(required = true, multiple = false)]
pub struct InputGroup {
    #[arg(short = 'e', long = "epoch")]
    pub epoch: Option<u64>,

    #[arg(short = 'r', long = "resume", default_value_t = false)]
    pub resume: bool,

    #[arg(short = 'l', long = "local-file", value_name = "FILE")]
    pub local_file: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum Input {
    Remote { epoch: u64, offset: u64 },
    Local { path: String, offset: u64 },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Configuration {
    pub data_dir: PathBuf,
    pub checkpoint_file: PathBuf,
    pub input: Input,
    pub tick_queue_size: usize,
    pub token_queue_size: usize,
    pub tick_batch_size: usize,
    pub token_batch_size: usize,
}

impl Configuration {
    pub async fn from_cli(cli: Cli) -> Result<Self> {
        let input = if let Some(path) = cli.input.local_file {
            Input::Local { path, offset: 0 }
        } else if let Some(epoch) = cli.input.epoch {
            Input::Remote { epoch, offset: 0 }
        } else if cli.input.resume {
            return Ok(Self::from_file(&cli.checkpoint_file).await?);
        } else {
            return Err("Either --local-file, --epoch or --resume must be specified".into());
        };

        Ok(Self {
            data_dir: cli.data_dir,
            checkpoint_file: cli.checkpoint_file,
            tick_queue_size: cli.tick_queue_size,
            token_queue_size: cli.token_queue_size,
            tick_batch_size: cli.tick_batch_size,
            token_batch_size: cli.token_batch_size,
            input,
        })
    }

    pub async fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let c = fs::read_to_string(path).await?;
        let s = toml::from_str(&c)?;
        info!("Loaded checkpoint: {:?}", s);
        Ok(s)
    }
}

impl Configuration {
    pub fn offset(&self) -> u64 {
        match &self.input {
            Input::Remote { offset, .. } => *offset,
            Input::Local { offset, .. } => *offset,
        }
    }

    pub async fn checkpoint(&self, offset: u64) -> Result<()> {
        let mut s = self.clone();
        match &mut s.input {
            Input::Remote { offset: o, .. } => *o = offset,
            Input::Local { offset: o, .. } => *o = offset,
        }
        let c = toml::to_string(&s)?;
        fs::write(&self.checkpoint_file, c).await?;
        Ok(())
    }
}
