use std::io;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::fs;
use tokio::io::AsyncRead;

use indicatif::{ProgressBar, ProgressStyle};
use serde::{Deserialize, Serialize};

use crate::Result;

const DEFAULT_EPOCH: u64 = 0;

pub struct ProgressReader<R: AsyncRead + Unpin> {
    inner: R,
    progress_bar: ProgressBar,
}

impl<R: AsyncRead + Unpin> ProgressReader<R> {
    pub fn new(total: u64, inner: R) -> Self {
        let pb = ProgressBar::new(total);
        pb.set_style(ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})")
        .unwrap()
        .progress_chars("#>-"));

        Self {
            inner,
            progress_bar: pb,
        }
    }

    pub fn progress_bar(&self) -> ProgressBar {
        self.progress_bar.clone()
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for ProgressReader<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let filled = buf.filled().len();
        let unpinned = self.get_mut();

        let res = Pin::new(&mut unpinned.inner).poll_read(cx, buf);
        if let Poll::Ready(Ok(())) = &res {
            if buf.filled().len() > filled {
                let n = buf.filled().len() - filled;
                unpinned.progress_bar.inc(n as u64);
            }
        }
        res
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Progress {
    pub epoch: u64,
    pub end_offset: u64,
    pub slot: u64,
}

impl Default for Progress {
    fn default() -> Self {
        Self {
            epoch: DEFAULT_EPOCH,
            end_offset: 0,
            slot: 0,
        }
    }
}

impl Progress {
    pub async fn save(&self, path: &Path) -> Result<()> {
        let s = toml::to_string(&self)?;
        fs::write(path, s).await?;
        Ok(())
    }

    pub async fn load(path: &Path) -> Result<Self> {
        let content = fs::read_to_string(path).await?;
        let p: Progress = toml::from_str(&content)?;
        Ok(p)
    }
}
