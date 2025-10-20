use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::time;

use object_store::buffered;
use parquet::arrow::async_writer::AsyncFileWriter;

use tokio::sync::{mpsc, watch};
use tokio::{fs, io, runtime, signal, task};
use tokio::{select, try_join};
use tokio_util::io::StreamReader;

use object_store::{ObjectStore, gcp::GoogleCloudStorageBuilder, local::LocalFileSystem};

use url::Url;

use reqwest;

use solana_sdk::signature::Signature;

use yellowstone_faithful_car_parser::node::{Node, NodeReader, Nodes};

use indicatif::{HumanDuration, MultiProgress, ProgressBar, ProgressState, ProgressStyle};
use indicatif_log_bridge::LogWrapper;

use log::{error, info, warn};

mod config;
use config::{Cli, Configuration, Input};

mod arrow;
use crate::arrow::{TickBuilder, TokenBuilder};

mod pump;
use crate::pump::{CreateEvent, TradeEvent};

mod solana;
use crate::solana::notify_block;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn main() -> Result<()> {
    let rt = runtime::Builder::new_multi_thread().enable_all().build()?;

    rt.block_on(run())?;
    Ok(())
}

async fn run() -> Result<()> {
    use clap::Parser;

    let logger =
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).build();
    let bars = MultiProgress::new();
    let level = logger.filter();
    LogWrapper::new(bars.clone(), logger).try_init()?;
    log::set_max_level(level);

    let cli = Cli::parse();
    let cfg = Arc::new(Configuration::from_cli(cli).await?);

    if cfg.data_dir.scheme() == "file" {
        fs::create_dir_all(
            cfg.data_dir
                .to_file_path()
                .map_err(|_| "Invalid data directory path")?,
        )
        .await?;
    }

    let (epoch_rd, total_size) = open_input(&cfg).await?;
    let style = "{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})";
    let eta_lambda = |state: &ProgressState, w: &mut dyn fmt::Write| {
        write!(w, "{:.1}s", HumanDuration(state.eta())).unwrap();
    };

    let progress = bars.add(ProgressBar::new(total_size));
    progress.set_position(cfg.offset());
    progress.reset_eta();
    progress.set_style(
        ProgressStyle::with_template(style)?
            .with_key("eta", eta_lambda)
            .progress_chars("#>-"),
    );
    let mut progress_rd = ProgressReader::new(epoch_rd, progress.clone());

    let (ticks_wr, tokens_wr) = try_join!(
        open_parquet_file(&cfg, "ticks"),
        open_parquet_file(&cfg, "tokens"),
    )?;

    let (ticks_tx, ticks_rx) = mpsc::channel(cfg.tick_queue_size);
    let (tokens_tx, tokens_rx) = mpsc::channel(cfg.token_queue_size);
    let (exit_tx, exit_rx) = watch::channel(false);

    let tick_task = task::spawn({
        let cfg = cfg.clone();
        let exit_rx = exit_rx.clone();
        async move { write_ticks(&cfg, ticks_rx, exit_rx, ticks_wr).await }
    });
    let token_task = task::spawn({
        let cfg = cfg.clone();
        let exit_rx = exit_rx.clone();
        async move { write_tokens(&cfg, tokens_rx, exit_rx, tokens_wr).await }
    });

    tokio::spawn({
        let exit_tx = exit_tx.clone();
        async move {
            signal::ctrl_c().await.unwrap();
            if !*exit_tx.borrow() {
                info!("Received Ctrl-C, shutting down...");
                exit_tx.send(true).unwrap();
            } else {
                info!("Received Ctrl-C again, forcing shutdown...");
                std::process::exit(1);
            }
        }
    });

    let mut worktasks = task::JoinSet::new();
    let mut last_checkpoint = time::Instant::now();
    let mut last_position = cfg.offset();

    loop {
        let mut flag = exit_rx.clone();
        let mut rd = NodeReader::new(&mut progress_rd);
        let nodes = select! {
            res = Nodes::read_until_block(&mut rd) => match res {
                Ok(n) => n,
                Err(e) => {
                    error!("Error reading nodes: {}", e);
                    exit_tx.send(true)?;
                    break;
                }
            },
            _ = flag.changed() => {
                info!("Received exit signal, stopping node processing...");
                break;
            },
        };

        last_position = progress.position();

        let ticks_tx = ticks_tx.clone();
        let tokens_tx = tokens_tx.clone();
        worktasks.spawn(async move { consume_nodes(nodes, ticks_tx, tokens_tx).await });

        if last_checkpoint.elapsed() >= std::time::Duration::from_secs(30) {
            info!("Writing checkpoint...");
            if let Err(err) = cfg.checkpoint(last_position).await {
                warn!("Failed to write checkpoint: {}", err);
            }
            last_checkpoint = time::Instant::now();
        }
    }

    worktasks.join_all().await;
    tick_task.await??;
    token_task.await??;

    if total_size <= last_position {
        cfg.remove_checkpoint().await?;
    } else {
        cfg.checkpoint(last_position).await?;
    }

    Ok(())
}

struct ProgressReader<R> {
    inner: R,
    progress: ProgressBar,
}

impl<R: io::AsyncRead + Unpin> ProgressReader<R> {
    fn new(inner: R, progress: ProgressBar) -> Self {
        Self { inner, progress }
    }
}

impl<R: io::AsyncRead + Unpin> io::AsyncRead for ProgressReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let pre_len = buf.filled().len();
        let poll = Pin::new(&mut self.inner).poll_read(cx, buf);
        if let std::task::Poll::Ready(Ok(())) = &poll {
            let new_len = buf.filled().len();
            let read_bytes = (new_len - pre_len) as u64;
            self.progress.inc(read_bytes);
        }
        poll
    }
}

async fn consume_nodes(
    n: Nodes,
    ticks_tx: mpsc::Sender<(u64, Signature, TradeEvent)>,
    tokens_tx: mpsc::Sender<(u64, Signature, CreateEvent)>,
) -> Result<()> {
    let mut wt = task::JoinSet::new();

    for (_, node) in n.nodes.iter() {
        let blk = if let Node::Block(blk) = node {
            blk
        } else {
            continue;
        };

        let (ticks, tokens) = notify_block(&n, &blk)?;
        for t in ticks {
            wt.spawn({
                let ticks_tx = ticks_tx.clone();
                async move { ticks_tx.send(t).await.map_err(|e| e.to_string()) }
            });
        }
        for t in tokens {
            wt.spawn({
                let tokens_tx = tokens_tx.clone();
                async move { tokens_tx.send(t).await.map_err(|e| e.to_string()) }
            });
        }
    }

    wt.join_all().await;
    Ok(())
}

async fn write_ticks<W>(
    cfg: &Configuration,
    mut rx: mpsc::Receiver<(u64, Signature, TradeEvent)>,
    mut exit_rx: watch::Receiver<bool>,
    fd: W,
) -> Result<()>
where
    W: AsyncFileWriter + Unpin + Send,
{
    let mut tick_builder = TickBuilder::new(fd, cfg.tick_batch_size)?;
    tick_builder.epoch(match &cfg.input {
        &Input::Local { .. } => 0,
        &Input::Remote { epoch, .. } => epoch,
    });

    loop {
        select! {
            _ = exit_rx.changed() => {
                rx.close();
            },
            maybe_tuple = rx.recv() => match maybe_tuple {
                Some((slot, tx, event)) => {
                    tick_builder.append(slot, tx, event);
                    if tick_builder.rows() >= cfg.tick_batch_size {
                        tick_builder.write().await?;
                    }
                },
                None => {
                    break;
                }
            },
        }
    }

    tick_builder.close().await?;
    Ok(())
}

async fn write_tokens<W>(
    cfg: &Configuration,
    mut rx: mpsc::Receiver<(u64, Signature, CreateEvent)>,
    mut exit_rx: watch::Receiver<bool>,
    fd: W,
) -> Result<()>
where
    W: AsyncFileWriter + Unpin + Send,
{
    let mut token_builder = TokenBuilder::new(fd, cfg.token_batch_size)?;
    token_builder.epoch(match &cfg.input {
        &Input::Local { .. } => 0,
        &Input::Remote { epoch, .. } => epoch,
    });

    loop {
        select! {
            _ = exit_rx.changed() => {
                rx.close();
            },
            maybe_tuple = rx.recv() => match maybe_tuple {
                Some((slot, tx, event)) => {
                    token_builder.append(slot, tx, event);
                    if token_builder.rows() >= cfg.token_batch_size {
                        token_builder.write().await?;
                    }
                },
                None => {
                    break;
                }
            },
        }
    }

    token_builder.close().await?;
    Ok(())
}

async fn open_input(cfg: &Configuration) -> Result<(Pin<Box<dyn io::AsyncRead>>, u64)> {
    match &cfg.input {
        &Input::Local { ref path, offset } => stream_local(path, offset).await,
        &Input::Remote { epoch, offset } => stream_remote(epoch, offset).await,
    }
}

pub fn open_store(u: &Url) -> Result<Arc<dyn ObjectStore>> {
    match u.scheme() {
        "file" => {
            let maybe_p = u.to_file_path().ok();
            let p = maybe_p
                .unwrap_or_else(|| "./".to_string().into())
                .canonicalize()?;
            Ok(Arc::new(LocalFileSystem::new_with_prefix(p)?))
        }
        "gs" => Ok(Arc::new(
            GoogleCloudStorageBuilder::default()
                .with_url(u.as_str())
                .build()?,
        )),
        _ => Err(format!("Unsupported URL scheme: {}", u.scheme()).into()),
    }
}

async fn open_parquet_file(
    cfg: &Configuration,
    typ: impl AsRef<str>,
) -> Result<buffered::BufWriter> {
    let (epoch, offset) = match &cfg.input {
        &Input::Local { .. } => ("local".to_owned(), "0".to_owned()),
        &Input::Remote { epoch, offset } => (epoch.to_string(), offset.to_string()),
    };

    let wr = buffered::BufWriter::new(
        open_store(&cfg.data_dir)?,
        format!("{}-epoch={}-offset={}.parquet", typ.as_ref(), epoch, offset).into(),
    );
    Ok(wr)
}

async fn stream_local(
    path: impl AsRef<str>,
    offset: u64,
) -> Result<(Pin<Box<dyn io::AsyncRead>>, u64)> {
    use tokio::io::AsyncSeekExt;
    use tokio::io::SeekFrom;

    let mut fd = fs::File::open(path.as_ref()).await?;
    if offset > 0 {
        fd.seek(SeekFrom::Start(offset)).await?;
    }
    let len = fd.metadata().await?.len();

    Ok((Box::pin(fd), len))
}

async fn stream_remote(epoch: u64, offset: u64) -> Result<(Pin<Box<dyn io::AsyncRead>>, u64)> {
    use futures_util::TryStreamExt;

    let client = reqwest::Client::new();
    let url = format!("https://files.old-faithful.net/{0}/epoch-{0}.car", epoch);
    let mut req = client.request(reqwest::Method::GET, url);

    if offset > 0 {
        let range = format!("bytes={}-", offset);
        req = req.header(reqwest::header::RANGE, range);
    }

    let resp = client.execute(req.build()?).await?.error_for_status()?;
    let total_size = resp
        .content_length()
        .ok_or("Failed to get content length")?;

    let rd = StreamReader::new(
        resp.bytes_stream()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e)),
    );

    Ok((Box::pin(rd), total_size))
}
