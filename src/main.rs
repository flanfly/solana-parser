use std::future::Future;
use std::io::{self, Read};
use std::iter;
use std::pin::Pin;
use std::result;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll};

use tokio::io::AsyncRead;
use tokio::{fs, select, try_join};
use tokio_util::io::StreamReader;

use solana_bincode::limited_deserialize;
use solana_sdk::message::AddressLoader;
use solana_sdk::message::v0::{LoadedAddresses, MessageAddressTableLookup};
use solana_sdk::transaction::{
    AddressLoaderError, SanitizedTransaction, SanitizedVersionedTransaction, VersionedTransaction,
};
use solana_storage_proto::convert::generated;
use solana_transaction_status::TransactionStatusMeta;

use yellowstone_faithful_car_parser::node::{Block, Node, NodeReader, Nodes};

use borsh::BorshDeserialize;
use futures_util::TryStreamExt;
use prost::Message;
use reqwest::{Client, Method, header};
use serde::{Deserialize, Serialize};

mod arrow;
use crate::arrow::{TickBuilder, TokenBuilder};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

mod pump;
use crate::pump::{CreateEvent, EmitCpiInstruction, Event, Instruction, TradeEvent};

#[derive(Debug, Serialize, Deserialize)]
struct Progress {
    pub epoch: u64,
    pub end_offset: u64,
    pub slot: u64,
}

#[derive(Debug, Clone)]
pub struct MessageAddressLoaderFromTxMeta<'a> {
    pub meta: &'a TransactionStatusMeta,
}

impl<'a> AddressLoader for MessageAddressLoaderFromTxMeta<'a> {
    fn load_addresses(
        self,
        _lookups: &[MessageAddressTableLookup],
    ) -> result::Result<LoadedAddresses, AddressLoaderError> {
        Ok(self.meta.loaded_addresses.clone())
    }
}

pub fn decompress_zstd(data: Vec<u8>) -> Result<Vec<u8>> {
    let mut decoder = zstd::Decoder::new(&data[..])?;
    let mut decompressed = Vec::new();
    decoder.read_to_end(&mut decompressed)?;
    Ok(decompressed)
}

#[derive(Clone)]
struct Abort {
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

struct TrackingReader<R: AsyncRead + Unpin> {
    inner: R,
    bytes_read: Arc<AtomicUsize>,
}

impl<R: AsyncRead + Unpin> TrackingReader<R> {
    pub fn new(counter: Arc<AtomicUsize>, inner: R) -> Self {
        Self {
            inner,
            bytes_read: counter,
        }
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
                let n = buf.filled().len() - filled;

                unpinned.bytes_read.fetch_add(n, Ordering::SeqCst);
                //println!("Read {} bytes", n);
            }
        }
        res
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let epoch = 800;

    fs::create_dir_all("data").await?;

    let tick_path = format!("data/{}-ticks.parquet", epoch);
    let tick_fd = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&tick_path)
        .await?;
    let mut ticks = TickBuilder::new(tick_fd, 1000)?;
    ticks.epoch(epoch);

    let token_path = format!("data/{}-tokens.parquet", epoch);
    let token_fd = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&token_path)
        .await?;
    let mut tokens = TokenBuilder::new(token_fd, 1000)?;
    tokens.epoch(epoch);

    let client = Client::new();
    let url = format!("https://files.old-faithful.net/{0}/epoch-{0}.car", epoch);
    //let range = format!("bytes={}-", 29949054);
    let req = client
        .request(Method::GET, url)
        //.header(header::RANGE, range)
        .build()?;

    let resp = client.execute(req).await?.error_for_status()?;
    let bytes_counter = Arc::new(AtomicUsize::new(0));
    let mut rd = NodeReader::new(TrackingReader::new(
        bytes_counter.clone(),
        StreamReader::new(
            resp.bytes_stream()
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e)),
        ),
    ));

    let abort = Abort::new()?;

    loop {
        select! {
            _ = abort.clone() => {
                println!("Aborted: flushing and exiting...");
                try_join! {
                    tokens.close(),
                    ticks.close(),
                }?;
               return Ok(());
            },
            res = Nodes::read_until_block(&mut rd) => {
                match res {
                    Ok(n) => {
                        for (_, node) in n.nodes.iter() {
                            match node {
                                Node::Block(blk) => {
                                    let next_block_offset = bytes_counter.load(Ordering::SeqCst);

                                    ticks.slot(blk.slot);
                                    tokens.slot(blk.slot);
                                    notify_block(&n, blk,  &mut ticks, &mut tokens).await?;

                                    // save progress
                                    let ticks_written = ticks.rows();
                                    let tokens_written = tokens.rows();
                                    try_join!{tokens.write(), ticks.write()}?;

                                    let progstr = toml::to_string(&Progress {
                                        epoch: epoch,
                                        end_offset: next_block_offset as u64,
                                        slot: blk.slot,
                                    })?;
                                    fs::write("progress.toml", progstr).await?;

                                    println!("epoch={}, slot={}, ticks written={}, tokens written={}, offset={}", epoch, blk.slot, ticks_written, tokens_written, next_block_offset);
                                }
                                Node::Epoch(_) | Node::Transaction(_) | Node::Entry(_) | Node::DataFrame(_) | Node::Subset(_) | Node::Rewards(_) => {}
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Error reading CAR: {}", e);
                        break;
                    }
                }
            }
        }
    }

    println!("Finished reading CAR: flushing and exiting...");
    try_join! {
        tokens.close(),
        ticks.close(),
    }?;
    Ok(())
}

async fn notify_block(
    n: &Nodes,
    blk: &Block,
    ticks: &mut TickBuilder<fs::File>,
    tokens: &mut TokenBuilder<fs::File>,
) -> Result<()> {
    let txn = blk
        .entries
        .iter()
        .filter_map(|cid| match n.nodes.get(cid) {
            Some(Node::Entry(e)) => Some(e),
            _ => None,
        })
        .flat_map(|e| e.transactions.iter())
        .filter_map(|cid| match n.nodes.get(cid) {
            Some(Node::Transaction(t)) => Some(t),
            _ => None,
        });

    for raw in txn {
        let mut metafrm = n.reassemble_dataframes(&raw.metadata)?;
        if !metafrm.is_empty() {
            metafrm = decompress_zstd(metafrm)?;
        }
        let meta =
            TransactionStatusMeta::try_from(generated::TransactionStatusMeta::decode(&*metafrm)?)?;
        if meta.status.is_err() {
            continue;
        }

        let bodyfrm = n.reassemble_dataframes(&raw.data)?;
        let body: VersionedTransaction = limited_deserialize(&bodyfrm, 10 * 1024 * 1024)?;

        let msg_hash = body.verify_and_hash_message()?;
        let tx = SanitizedTransaction::try_new(
            SanitizedVersionedTransaction::try_from(body)?,
            msg_hash,
            false,
            MessageAddressLoaderFromTxMeta { meta: &meta },
            &Default::default(),
        )?;

        let ak = tx.message().account_keys();
        let isns = tx
            .message()
            .instructions()
            .iter()
            .enumerate()
            .flat_map(|(idx, isn)| {
                iter::once(isn).chain(
                    meta.inner_instructions
                        .as_ref()
                        .and_then(|i| i.get(idx))
                        .map(|i| i.instructions.iter().map(|iisn| &iisn.instruction))
                        .into_iter()
                        .flatten(),
                )
            })
            .filter(|isn| ak[isn.program_id_index as usize] == *pump::PROGRAM_ID);

        for isn in isns {
            match pump::Instruction::try_from_slice(&isn.data) {
                Ok(Instruction::EmitCpi(EmitCpiInstruction { event })) => match event {
                    Event::Trade(TradeEvent {
                        mint,
                        sol_amount,
                        token_amount,
                        is_buy,
                        timestamp,
                        virtual_sol_reserves,
                        virtual_token_reserves,
                        real_sol_reserves,
                        real_token_reserves,
                        ..
                    }) => {
                        ticks.append(
                            tx.signature(),
                            &mint,
                            is_buy,
                            sol_amount,
                            token_amount,
                            timestamp,
                            virtual_sol_reserves,
                            virtual_token_reserves,
                            real_sol_reserves,
                            real_token_reserves,
                        );
                    }
                    Event::Create(CreateEvent {
                        name,
                        symbol,
                        uri,
                        mint,
                        timestamp,
                        virtual_token_reserves,
                        virtual_sol_reserves,
                        real_token_reserves,
                        ..
                    }) => {
                        tokens.append(
                            tx.signature(),
                            &name,
                            &symbol,
                            &uri,
                            &mint,
                            timestamp,
                            virtual_token_reserves,
                            virtual_sol_reserves,
                            real_token_reserves,
                        );
                    }
                    Event::Complete(_) | Event::CompletePumpAmm(_) | Event::Other(_) => {}
                },
                Ok(_) => {}
                Err(_e) => {
                    //println!("Failed to decode instruction: {}", e);
                    continue;
                }
            }
        }
    }

    Ok(())
}
