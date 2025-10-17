use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use clap::Parser;

use solana_parser::Result;

const DEFAULT_PROGRESS_FILE: &str = "progress.toml";

#[derive(Parser)]
#[command(version, about, long_about = None)]
pub struct Cli {
    pub name: Option<String>,

    #[arg(
        long = "progress-file",
        value_name = "FILE",
        default_value = DEFAULT_PROGRESS_FILE
    )]
    pub progress_file: PathBuf,

    #[arg(long = "data-dir", value_name = "DIR", default_value = "data")]
    pub data_dir: PathBuf,

    #[arg(short = 'e', long = "epoch")]
    pub epoch: Option<u64>,

    #[arg(short = 'r', long = "resume", default_value_t = true)]
    pub resume: bool,
}

#[derive(Clone)]
pub struct Abort {
    flag: Arc<AtomicBool>,
}

impl Abort {
    pub fn new() -> Result<Self> {
        let flag = Arc::new(AtomicBool::new(false));
        {
            let flag = flag.clone();
            ctrlc::set_handler(move || {
                if !flag.load(Ordering::SeqCst) {
                    eprintln!("Received Ctrl-C, shutting down...");
                    flag.store(true, Ordering::SeqCst);
                } else {
                    eprintln!("Received Ctrl-C again, forcing shutdown...");
                    std::process::exit(1);
                }
            })?;
        }

        Ok(Self { flag })
    }
}

impl Future for Abort {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.flag.load(Ordering::SeqCst) {
            Poll::Ready(())
        } else {
            // busy wait
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}
