use std::collections::VecDeque;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time;

use humansize::{DECIMAL, format_size};
use time_humanize::HumanTime;
use tokio::io::AsyncRead;

static TRACKING_WINDOW: LazyLock<time::Duration> = LazyLock::new(|| time::Duration::from_secs(20));

pub struct TrackingReader<R: AsyncRead + Unpin> {
    inner: R,
    total: u64,
    bytes_read: Arc<AtomicU64>,
    observations: VecDeque<(usize, time::Instant)>,
    last_print: time::Instant,
}

impl<R: AsyncRead + Unpin> TrackingReader<R> {
    pub fn new(counter: Arc<AtomicU64>, total: u64, inner: R) -> Self {
        Self {
            inner,
            total,
            bytes_read: counter,
            observations: VecDeque::new(),
            last_print: time::Instant::now(),
        }
    }

    fn estimated_time_remaining(&mut self, now: time::Instant) -> (f32, time::Duration) {
        // trim old observations
        while let Some((_, t)) = self.observations.front() {
            if now.duration_since(*t) > *TRACKING_WINDOW {
                self.observations.pop_front();
            } else {
                break;
            }
        }

        let consumed_bytes = self.observations.iter().map(|(b, _)| *b).sum::<usize>();
        let consumed_secs = self
            .observations
            .iter()
            .map(|(_, t)| now.duration_since(*t).as_secs_f64())
            .sum::<f64>();

        let rate = consumed_bytes as f64 / consumed_secs;
        let eta = (self.total - self.bytes_read.load(Ordering::SeqCst)) as f64 / rate;

        (rate as f32, time::Duration::from_secs_f64(eta))
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for TrackingReader<R> {
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
                let now = time::Instant::now();
                let n = buf.filled().len() - filled;

                unpinned.observations.push_back((n, now));
                unpinned.bytes_read.fetch_add(n as u64, Ordering::SeqCst);
                if now.duration_since(unpinned.last_print) < time::Duration::from_millis(300) {
                    return res;
                }

                let (rate, eta) = unpinned.estimated_time_remaining(now);
                let pct = (unpinned.bytes_read.load(Ordering::SeqCst) as f32
                    / unpinned.total as f32)
                    * 100.0;
                println!(
                    "Read {}/{} bytes ({:.2}%), {:.2} bytes/sec, ETA: {}",
                    format_size(unpinned.bytes_read.load(Ordering::SeqCst) as u64, DECIMAL),
                    format_size(unpinned.total as u64, DECIMAL),
                    pct,
                    rate,
                    HumanTime::from(eta)
                );
                unpinned.last_print = now;
            }
        }
        res
    }
}
