use std::path::Path;
use std::{ffi::OsString, path::absolute};

use clap;
use log::info;
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use toml;
use url::Url;

use crate::{Result, open_store};

const DEFAULT_CHECKPOINT_FILE: &str = "checkpoint.toml";
const MESSAGE_QUEUE_SIZE: usize = 100;
const ARROW_BATCH_SIZE: usize = 1000;
const GCP_LOG_NAME: &str = "solana-parser";

#[derive(Debug, clap::Parser)]
#[command(version, about, long_about = None)]
pub struct Cli {
    #[arg(
        long = "checkpoint-file",
        value_name = "FILE",
        default_value = DEFAULT_CHECKPOINT_FILE,
        env = "CHECKPOINT_FILE"
    )]
    pub checkpoint_file: String,

    #[arg(
        long = "data-dir",
        value_name = "DIR",
        default_value = "data",
        env = "DATA_DIR"
    )]
    pub data_dir: String,

    #[arg(long = "no-progress", default_value_t = false, env = "NO_PROGRESS")]
    pub no_progress: bool,

    #[arg(
        long = "enable-gcloud-log",
        default_value_t = false,
        env = "ENABLE_GCLOUD_LOG"
    )]
    pub gcloud_log: bool,

    #[command(flatten)]
    pub input: InputGroup,
}

#[derive(Debug, clap::Args)]
#[group(required = true, multiple = false)]
pub struct InputGroup {
    #[arg(short = 'e', long = "epoch", value_name = "EPOCH", env = "EPOCH")]
    pub epoch: Option<u64>,

    #[arg(short = 'r', long = "resume", default_value_t = false, env = "RESUME")]
    pub resume: bool,

    #[arg(
        short = 'l',
        long = "local-file",
        value_name = "FILE",
        env = "LOCAL_FILE"
    )]
    pub local_file: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum Input {
    Remote { epoch: u64, offset: u64 },
    Local { path: String, offset: u64 },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Configuration {
    pub data_dir: Url,
    pub checkpoint_file: Url,
    pub input: Input,

    pub enable_progress: bool,
    pub gcloud_log: Option<String>,
    pub message_queue_size: usize,
    pub arrow_batch_size: usize,
}

impl Configuration {
    pub async fn from_cli(cli: Cli) -> Result<Self> {
        let input = if let Some(path) = cli.input.local_file {
            Input::Local { path, offset: 0 }
        } else if let Some(epoch) = cli.input.epoch {
            Input::Remote { epoch, offset: 0 }
        } else if cli.input.resume {
            return Ok(Self::from_checkpoint(&cli.checkpoint_file).await?);
        } else {
            return Err("Either --local-file, --epoch or --resume must be specified".into());
        };

        let data_dir = to_url(&cli.data_dir)?;
        let checkpoint_file = to_url(&cli.checkpoint_file)?;

        Ok(Self {
            data_dir,
            checkpoint_file,
            input,
            gcloud_log: cli.gcloud_log.then(|| GCP_LOG_NAME.to_string()),
            message_queue_size: MESSAGE_QUEUE_SIZE,
            arrow_batch_size: ARROW_BATCH_SIZE,
            enable_progress: !cli.no_progress,
        })
    }

    pub async fn from_checkpoint<S: AsRef<str>>(s: S) -> Result<Self> {
        let u = to_url(s.as_ref())?;

        let p = Path::new(u.path());
        let dir = p.parent().unwrap_or_else(|| &Path::new("/"));
        let file = p
            .file_name()
            .map(|f| f.to_owned())
            .unwrap_or_else(|| OsString::from(DEFAULT_CHECKPOINT_FILE));

        let mut base = u.clone();
        base.set_path(dir.to_str().ok_or("Invalid UTF-8 in path")?);
        let store = open_store(&base)?;

        let c = store
            .get(&file.to_str().ok_or("Invalid UTF-8 in path")?.into())
            .await?;
        let s = toml::from_slice(&c.bytes().await?)?;

        info!("Loaded checkpoint: {:?}", s);
        Ok(s)
    }
}

fn to_url(path: &str) -> Result<Url> {
    match Url::parse(path) {
        Ok(url) => Ok(url),
        Err(url::ParseError::RelativeUrlWithoutBase) => {
            let p = Path::new(path).canonicalize().or_else(|_| absolute(path))?;
            Ok(Url::from_file_path(p).map_err(|err| {
                format!("Failed to parse path as URL or valid file path: {:?}", err)
            })?)
        }
        Err(err) => Err(err.into()),
    }
}

impl Configuration {
    pub fn offset(&self) -> u64 {
        match &self.input {
            Input::Remote { offset, .. } => *offset,
            Input::Local { offset, .. } => *offset,
        }
    }

    pub async fn remove_checkpoint(&self) -> Result<()> {
        let mut base = self.checkpoint_file.clone();
        base.set_path("/");
        let store = open_store(&base)?;

        store
            .delete(&self.checkpoint_file.path().to_string().into())
            .await?;

        Ok(())
    }

    pub async fn checkpoint(&self, offset: u64) -> Result<()> {
        let mut s = self.clone();
        match &mut s.input {
            Input::Remote { offset: o, .. } => *o = offset,
            Input::Local { offset: o, .. } => *o = offset,
        }

        let mut base = self.checkpoint_file.clone();
        base.set_path("/");
        let store = open_store(&base)?;

        let c = toml::to_string(&s)?;
        store
            .put(
                &self.checkpoint_file.path().into(),
                c.as_bytes().to_owned().into(),
            )
            .await?;

        Ok(())
    }
}
