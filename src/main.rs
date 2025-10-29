use std::fmt;
use std::ops::RangeInclusive;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time;

use anyhow::{Context, Result, anyhow, bail};

use object_store::buffered;
use parquet::arrow::async_writer::AsyncFileWriter;

use tokio::signal::unix;
use tokio::sync::{mpsc, watch};
use tokio::{fs, io, runtime, task};
use tokio::{join, select, try_join};
use tokio_util::io::StreamReader;

use object_store::{ObjectStore, gcp::GoogleCloudStorageBuilder, local::LocalFileSystem};

use url::Url;

use reqwest;

use solana_sdk::signature::Signature;

use yellowstone_faithful_car_parser::node::{Node, NodeError, NodeReader, Nodes};

use indicatif::{HumanDuration, MultiProgress, ProgressBar, ProgressState, ProgressStyle};

use log::{error, info, warn};

use gethostname::gethostname;

mod config;
use config::{Cli, Configuration};

mod arrow;
use crate::arrow::{TickBuilder, TokenBuilder};

mod pump;
use crate::pump::{CreateEvent, TradeEvent};

mod solana;
use crate::solana::notify_block;

mod logger;
use crate::logger::setup_log;

const VERSION: &str = env!("VERSION");

#[derive(Clone, serde::Deserialize, serde::Serialize, Debug)]
struct EpochDocument {
    pub holder: String,
    pub last_updated: time::SystemTime,
}

fn main() -> Result<()> {
    let rt = runtime::Builder::new_multi_thread().enable_all().build()?;

    rt.block_on(run())?;
    Ok(())
}

async fn run() -> Result<()> {
    use clap::Parser;

    // early init
    let cli = Cli::parse();
    let cfg = Arc::new(Configuration::from_cli(cli).await?);

    let bars = setup_log(cfg.gcloud_log.as_deref())?;
    info!("solana-parser {}", VERSION);

    let nodeid = gethostname().to_string_lossy().into_owned();
    info!("Writing data to: {}", cfg.output_dir);

    if cfg.output_dir.scheme() == "file" {
        fs::create_dir_all(
            cfg.output_dir
                .to_file_path()
                .map_err(|_| anyhow!("Invalid output_dir path"))?,
        )
        .await?;
    }

    let (exit_tx, exit_rx) = watch::channel(false);

    let mut sigint = unix::signal(unix::SignalKind::interrupt())?;
    let mut sigterm = unix::signal(unix::SignalKind::terminate())?;
    tokio::spawn({
        let exit_rx = exit_rx.clone();
        async move {
            loop {
                select! {
                    _ = sigint.recv() => {
                        warn!("Received SIGINT. Initiating graceful shutdown...");
                    }
                    _ = sigterm.recv() => {
                        warn!("Received SIGTERM. Initiating graceful shutdown...");
                    }
                }
                if *exit_rx.borrow() {
                    error!("Shutdown signal received twice, forcing exit");
                    std::process::exit(-1);
                }
                if let Err(e) = exit_tx.send(true) {
                    error!("Failed to send shutdown signal: {}", e);
                }
            }
        }
    });

    while !*exit_rx.borrow() {
        match lock_epoch(cfg.epochs.clone(), &cfg, &nodeid).await {
            Ok(None) => {
                info!("No available epochs to process");
                return Ok(());
            }
            Ok(Some(epoch)) => {
                let mut offset = 0u64;

                'read_loop: while !*exit_rx.borrow() {
                    info!("Processing epoch {} from offset {}", epoch, offset);

                    let (rd, size) = match stream_remote(epoch, offset).await {
                        Ok(res) => res,
                        Err(e) => {
                            error!("Error streaming epoch {}: {}", epoch, e);
                            if let Err(err) = unlock_epoch(epoch, &cfg, &nodeid).await {
                                error!("Failed to unlock epoch {}: {}", epoch, err);
                            }
                            break 'read_loop;
                        }
                    };

                    let res = run_with_reader(
                        rd,
                        offset,
                        size,
                        epoch,
                        exit_rx.clone(),
                        cfg.clone(),
                        bars.clone(),
                    )
                    .await;
                    match res {
                        Ok(read_bytes) if read_bytes >= size => {
                            info!("Completed processing epoch {}", epoch);
                            break 'read_loop;
                        }
                        Ok(read_bytes) => {
                            warn!(
                                "Incomplete processing of epoch {}: read {}/{} bytes",
                                epoch, read_bytes, size
                            );
                            offset = read_bytes;
                        }
                        Err(e) => {
                            error!("Error processing epoch {}: {}", epoch, e);
                        }
                    }
                }

                if let Err(e) = unlock_epoch(epoch, &cfg, &nodeid).await {
                    error!("Failed to unlock epoch {}: {}", epoch, e);
                }

                if !*exit_rx.borrow() {
                    info!("Finalizing epoch {}", epoch);
                    finalize_epoch(epoch, &cfg).await?;
                }
            }
            Err(e) => {
                error!("Error locking epoch: {}", e);
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }

    Ok(())
}

async fn run_with_reader(
    epoch_rd: Pin<Box<dyn io::AsyncRead>>,
    mut offset: u64,
    total_size: u64,
    epoch: u64,
    exit_rx: watch::Receiver<bool>,
    cfg: Arc<Configuration>,
    bars: MultiProgress,
) -> Result<u64> {
    info!(
        "Start processing epoch={} size={} offset={}",
        epoch, total_size, offset
    );

    let progress = if cfg.enable_progress {
        let style = "{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})";
        let eta_lambda = |state: &ProgressState, w: &mut dyn fmt::Write| {
            write!(w, "{:.1}s", HumanDuration(state.eta())).unwrap();
        };
        let p = bars.add(ProgressBar::new(total_size));
        p.reset_eta();
        p.set_style(
            ProgressStyle::with_template(style)?
                .with_key("eta", eta_lambda)
                .progress_chars("#>-"),
        );
        Some(p)
    } else {
        None
    };

    let mut progress_rd = ProgressReader::new(epoch_rd, offset, progress.clone());

    let (ticks_wr, tokens_wr) = try_join!(
        open_parquet_file(epoch, offset, &cfg, "ticks"),
        open_parquet_file(epoch, offset, &cfg, "tokens"),
    )?;

    let (msg_tx, msg_rx) = mpsc::channel(cfg.message_queue_size);

    let write_task = task::spawn({
        let cfg = cfg.clone();
        async move { write_messages(epoch, &cfg, msg_rx, ticks_wr, tokens_wr).await }
    });

    let mut worktasks = task::JoinSet::new();

    while !*exit_rx.borrow() {
        let mut exit_rx = exit_rx.clone();
        let mut rd = NodeReader::new(&mut progress_rd);
        let nodes = select! {
            res = Nodes::read_until_block(&mut rd) => match res {
                Ok(n) => n,
                Err(NodeError::Io(ref err)) if err.kind() == io::ErrorKind::UnexpectedEof => {
                    info!("Reached end of input");
                    break;
                }
                Err(e) => {
                    error!("Error reading nodes: {:?}", e);
                    break;
                }
            },
            _ = exit_rx.changed() => {
                info!("Received exit signal, stopping node processing...");
                break;
            },
        };

        offset = progress_rd.position();

        worktasks.spawn({
            let msg_tx = msg_tx.clone();
            async move { consume_nodes(nodes, msg_tx).await }
        });
    }

    msg_tx.send(Message::Close).await?;
    worktasks.join_all().await;
    write_task.await??;

    Ok(offset)
}

struct ProgressReader<R> {
    inner: R,
    position: AtomicU64,
    progress: Option<ProgressBar>,
}

impl<R: io::AsyncRead + Unpin> ProgressReader<R> {
    fn new(inner: R, offset: u64, progress: Option<ProgressBar>) -> Self {
        if let Some(ref p) = progress {
            p.set_position(offset);
        }
        Self {
            inner,
            progress,
            position: AtomicU64::new(offset),
        }
    }

    fn position(&self) -> u64 {
        self.position.load(Ordering::Relaxed)
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

            self.position.fetch_add(read_bytes, Ordering::Relaxed);
            if let Some(progress) = &self.progress {
                progress.inc(read_bytes);
            }
        }

        poll
    }
}

enum Message {
    Tick(u64, Signature, TradeEvent),
    Token(u64, Signature, CreateEvent),
    Close,
}

async fn consume_nodes(n: Nodes, msg_tx: mpsc::Sender<Message>) -> Result<()> {
    let mut wt = task::JoinSet::new();

    for (_, node) in n.nodes.iter() {
        let blk = if let Node::Block(blk) = node {
            blk
        } else {
            continue;
        };

        let (ticks, tokens) = notify_block(&n, &blk)?;
        for (slot, sig, ev) in ticks {
            wt.spawn({
                let msg_tx = msg_tx.clone();
                let msg = Message::Tick(slot, sig, ev);
                async move { msg_tx.send(msg).await.map_err(|e| e.to_string()) }
            });
        }
        for (slot, sig, ev) in tokens {
            wt.spawn({
                let msg_tx = msg_tx.clone();
                let msg = Message::Token(slot, sig, ev);
                async move { msg_tx.send(msg).await.map_err(|e| e.to_string()) }
            });
        }
    }

    wt.join_all().await;
    Ok(())
}

async fn write_messages<W>(
    epoch: u64,
    cfg: &Configuration,
    mut rx: mpsc::Receiver<Message>,
    tick_fd: W,
    token_fd: W,
) -> Result<()>
where
    W: AsyncFileWriter + Unpin + Send,
{
    let mut tick_builder = TickBuilder::new(tick_fd, cfg.arrow_batch_size)?;
    let mut token_builder = TokenBuilder::new(token_fd, cfg.arrow_batch_size)?;

    tick_builder.epoch(epoch);
    token_builder.epoch(epoch);

    while let Some(msg) = rx.recv().await {
        match msg {
            Message::Tick(slot, tx, event) => {
                tick_builder.append(slot, tx, event);
                if tick_builder.rows() >= cfg.arrow_batch_size {
                    tick_builder.write().await?;
                }
            }
            Message::Token(slot, tx, event) => {
                token_builder.append(slot, tx, event);
                if token_builder.rows() >= cfg.arrow_batch_size {
                    token_builder.write().await?;
                }
            }
            Message::Close => {
                break;
            }
        }
    }

    join!(tick_builder.close(), token_builder.close());
    info!("Parquet writers closed");

    Ok(())
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
        _ => Err(anyhow!("Unsupported URL scheme: {}", u.scheme())),
    }
}

async fn open_parquet_file(
    epoch: u64,
    offset: u64,
    cfg: &Configuration,
    typ: impl AsRef<str>,
) -> Result<buffered::BufWriter> {
    let wr = buffered::BufWriter::new(
        open_store(&cfg.output_dir)?,
        format!("{}-epoch={}-offset={}.parquet", typ.as_ref(), epoch, offset).into(),
    );
    Ok(wr)
}

async fn stream_remote(epoch: u64, offset: u64) -> Result<(Pin<Box<dyn io::AsyncRead>>, u64)> {
    use futures_util::TryStreamExt;

    let client = reqwest::Client::new();
    let url = format!("https://files.old-faithful.net/{0}/epoch-{0}.car", epoch);

    info!("Streaming remote file: {}", url);

    let mut req = client.request(reqwest::Method::GET, url);

    if offset > 0 {
        let range = format!("bytes={}-", offset);
        req = req.header(reqwest::header::RANGE, range);
    }

    let resp = client.execute(req.build()?).await?.error_for_status()?;
    let total_size = resp
        .content_length()
        .ok_or(anyhow!("Failed to get content length"))?;

    let rd = StreamReader::new(
        resp.bytes_stream()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e)),
    );

    Ok((Box::pin(rd), total_size))
}

async fn lock_epoch(
    epoch: RangeInclusive<u64>,
    cfg: &Configuration,
    nodeid: &str,
) -> Result<Option<u64>> {
    let store = open_store(&cfg.output_dir).context("open_store for output_dir")?;
    for e in epoch.clone() {
        if store
            .head(&format!("epoch-{}.done", e).as_str().into())
            .await
            .is_ok()
        {
            info!("Epoch {} is already marked done, skipping", e);
            continue;
        }

        match try_lock_epoch(e, store.clone(), nodeid).await {
            Ok(()) => {
                return Ok(Some(e));
            }
            Err(err) => {
                info!("Failed to lock epoch {}: {}", e, err);
            }
        }
    }

    Ok(None)
}

async fn finalize_epoch(epoch: u64, cfg: &Configuration) -> Result<()> {
    let store = open_store(&cfg.output_dir).context("open_store for output_dir")?;
    let loc = format!("epoch-{}.done", epoch);
    store
        .put(&loc.as_str().into(), Vec::new().into())
        .await
        .context("put epoch done file")?;
    Ok(())
}

async fn try_lock_epoch(epoch: u64, store: Arc<dyn ObjectStore>, nodeid: &str) -> Result<()> {
    let loc = format!("epoch-{}.lock.toml", epoch);
    let maxage = time::SystemTime::now()
        .checked_sub(std::time::Duration::from_secs(24 * 60 * 60))
        .ok_or(anyhow!("Failed to compute maxage time"))?;

    for t in 0..5 {
        info!("Attempting to lock epoch {} (try {}/5)...", epoch, t + 1);

        let obj = store.get(&loc.as_str().into()).await;

        let mut generation = "0".to_owned();
        match obj {
            Ok(res) => {
                let meta = res.meta.clone();
                let doc = toml::from_slice::<EpochDocument>(&res.bytes().await?);

                if let Ok(doc) = doc {
                    if doc.holder == nodeid {
                        info!("Epoch {} is already locked by this node", epoch);
                        generation = meta.version.unwrap_or(generation);
                    } else if doc.last_updated < maxage {
                        info!(
                            "Epoch {} was locked by node {} but last updated over 24 hours ago, taking over",
                            epoch, doc.holder
                        );
                        generation = meta.version.unwrap_or(generation);
                    } else {
                        info!("Epoch {} is locked by node {}, skipping", epoch, doc.holder);
                        return Err(anyhow!("Epoch {} is locked by another node", epoch));
                    }
                }
            }
            Err(object_store::Error::NotFound { .. }) => {}
            Err(e) => {
                bail!("Failed to get epoch {} lock: {:?}", epoch, e);
            }
        }

        let doc = EpochDocument {
            holder: nodeid.to_owned(),
            last_updated: time::SystemTime::now(),
        };
        let docstr = toml::to_string(&doc)?;

        let mode = if generation == "0" {
            object_store::PutMode::Create
        } else {
            object_store::PutMode::Update(object_store::UpdateVersion {
                e_tag: None,
                version: Some(generation),
            })
        };
        let putres = store
            .put_opts(
                &loc.as_str().into(),
                docstr.as_bytes().to_owned().into(),
                object_store::PutOptions {
                    mode,
                    ..Default::default()
                },
            )
            .await;

        match putres {
            Ok(_) => {
                info!("Locked epoch {} for node {}", epoch, nodeid);
                return Ok(());
            }
            Err(object_store::Error::Precondition { .. }) => {
                info!(
                    "Epoch {} lock precondition failed, already locked by another node",
                    epoch
                );
            }
            Err(e) => {
                warn!("Failed to put epoch {} lock: {:?}", epoch, e);
            }
        }
    }

    bail!("Failed to lock epoch {} after multiple attempts", epoch);
}

async fn unlock_epoch(epoch: u64, cfg: &Configuration, nodeid: &str) -> Result<()> {
    let store = open_store(&cfg.output_dir).context("open_store for sync_dir")?;
    let loc = format!("epoch-{}.lock.toml", epoch);

    let obj = store.get(&loc.as_str().into()).await?;
    let doc = toml::from_slice::<EpochDocument>(&obj.bytes().await?)?;
    if doc.holder != nodeid {
        bail!(
            "Cannot unlock epoch {}: held by node {}, not this node {}",
            epoch,
            doc.holder,
            nodeid
        );
    }

    store.delete(&loc.as_str().into()).await?;
    Ok(())
}
