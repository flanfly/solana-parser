use std::collections::HashSet;
use std::error::Error;
use std::future::Future;
use std::io::{self, Read};
use std::iter;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock};
use std::task::{Context, Poll};

use arrow::array::{
    ArrayBuilder, BooleanBuilder, Int64Builder, RecordBatch, StringBuilder, UInt64Array,
    UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema};

use borsh::BorshDeserialize;
use futures_util::TryStreamExt;
use parquet::arrow::AsyncArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use prost::Message;
use reqwest::Client;
use smallvec::SmallVec;

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

use crate::pump::{CreateEvent, EmitCpiInstruction, Event, Instruction, TradeEvent};

mod pump;

const EXPECTED_BATCH_SIZE: usize = 1024;

static TICK_SCHEMA: LazyLock<Schema> = LazyLock::new(|| {
    Schema::new(vec![
        Field::new("epoch", DataType::UInt64, false),
        Field::new("slot", DataType::UInt64, false),
        Field::new("transaction", DataType::Utf8, false),
        Field::new("mint", DataType::Utf8, false),
        Field::new("is_buy", DataType::Boolean, false),
        Field::new("sol", DataType::UInt64, false),
        Field::new("token", DataType::UInt64, false),
        Field::new("timestamp", DataType::Int64, false),
        Field::new("virtual_sol", DataType::UInt64, false),
        Field::new("virtual_tokens", DataType::UInt64, false),
        Field::new("real_sol", DataType::UInt64, false),
        Field::new("real_token", DataType::UInt64, false),
    ])
});

static TOKEN_SCHEMA: LazyLock<Schema> = LazyLock::new(|| {
    Schema::new(vec![
        Field::new("epoch", DataType::UInt64, false),
        Field::new("slot", DataType::UInt64, false),
        Field::new("transaction", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("uri", DataType::Utf8, false),
        Field::new("mint", DataType::Utf8, false),
        Field::new("timestamp", DataType::Int64, false),
        Field::new("virtual_token", DataType::UInt64, true),
        Field::new("virtual_sol", DataType::UInt64, true),
        Field::new("real_token", DataType::UInt64, true),
    ])
});

#[derive(Debug, Clone)]
pub struct MessageAddressLoaderFromTxMeta<'a> {
    pub meta: &'a TransactionStatusMeta,
}

impl<'a> AddressLoader for MessageAddressLoaderFromTxMeta<'a> {
    fn load_addresses(
        self,
        _lookups: &[MessageAddressTableLookup],
    ) -> Result<LoadedAddresses, AddressLoaderError> {
        Ok(self.meta.loaded_addresses.clone())
    }
}

pub fn decompress_zstd(data: Vec<u8>) -> Result<Vec<u8>, Box<dyn Error>> {
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
    pub fn new() -> Result<Self, Box<dyn Error>> {
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
}

impl<R: AsyncRead + Unpin> TrackingReader<R> {
    pub fn new(inner: R) -> Self {
        Self { inner }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for TrackingReader<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let filled = buf.filled().len();
        let res = Pin::new(&mut self.get_mut().inner).poll_read(cx, buf);
        if let Poll::Ready(Ok(())) = &res {
            if buf.filled().len() > filled {
                println!("Read {} bytes", buf.filled().len() - filled);
            }
        }
        res
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let epoch = 800;

    fs::create_dir_all("data").await?;

    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();

    let tick_path = format!("data/{}-ticks.parquet", epoch);
    let tick_fd = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&tick_path)
        .await?;
    let mut tick_wr =
        AsyncArrowWriter::try_new(tick_fd, Arc::new(TICK_SCHEMA.clone()), Some(props.clone()))?;

    let token_path = format!("data/{}-tokens.parquet", epoch);
    let token_fd = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&token_path)
        .await?;
    let mut token_wr =
        AsyncArrowWriter::try_new(token_fd, Arc::new(TOKEN_SCHEMA.clone()), Some(props))?;

    let client = Client::new();
    let url = format!("https://files.old-faithful.net/{0}/epoch-{0}.car", epoch);

    let resp = client.get(url).send().await?.error_for_status()?;
    let mut rd = NodeReader::new(TrackingReader::new(StreamReader::new(
        resp.bytes_stream()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e)),
    )));

    let abort = Abort::new()?;

    loop {
        select! {
            _ = abort.clone() => {
                println!("Aborted: flushing and exiting...");
                try_join! {
                    token_wr.close(),
                    tick_wr.close(),
                }?;
               return Ok(());
            },
            res = Nodes::read_until_block(&mut rd) => {
                match res {
                    Ok(n) => {
                        for (_, node) in n.nodes.iter() {
                            match node {
                                Node::Block(blk) => {
                                    notify_block(&n, blk, epoch,  &mut tick_wr, &mut token_wr).await?;
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
        token_wr.close(),
        tick_wr.close(),
    }?;
    Ok(())
}

async fn notify_block(
    n: &Nodes,
    blk: &Block,
    last_epoch: u64,
    tick_wr: &mut AsyncArrowWriter<fs::File>,
    token_wr: &mut AsyncArrowWriter<fs::File>,
) -> Result<(), Box<dyn Error>> {
    println!("Block: slot {}", blk.slot);

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

    // tick
    let mut tick_sig_col = StringBuilder::with_capacity(EXPECTED_BATCH_SIZE, EXPECTED_BATCH_SIZE);
    let mut tick_mint_col = StringBuilder::with_capacity(EXPECTED_BATCH_SIZE, EXPECTED_BATCH_SIZE);
    let mut tick_is_buy_col = BooleanBuilder::with_capacity(EXPECTED_BATCH_SIZE);
    let mut tick_sol_col = UInt64Builder::with_capacity(EXPECTED_BATCH_SIZE);
    let mut tick_token_col = UInt64Builder::with_capacity(EXPECTED_BATCH_SIZE);
    let mut tick_timestamp_col = Int64Builder::with_capacity(EXPECTED_BATCH_SIZE);
    let mut tick_virtual_sol_col = UInt64Builder::with_capacity(EXPECTED_BATCH_SIZE);
    let mut tick_virtual_token_col = UInt64Builder::with_capacity(EXPECTED_BATCH_SIZE);
    let mut tick_real_sol_col = UInt64Builder::with_capacity(EXPECTED_BATCH_SIZE);
    let mut tick_real_token_col = UInt64Builder::with_capacity(EXPECTED_BATCH_SIZE);

    // token
    let mut token_sig_col = StringBuilder::with_capacity(EXPECTED_BATCH_SIZE, EXPECTED_BATCH_SIZE);
    let mut token_name_col = StringBuilder::with_capacity(EXPECTED_BATCH_SIZE, EXPECTED_BATCH_SIZE);
    let mut token_symbol_col =
        StringBuilder::with_capacity(EXPECTED_BATCH_SIZE, EXPECTED_BATCH_SIZE);
    let mut token_uri_col = StringBuilder::with_capacity(EXPECTED_BATCH_SIZE, EXPECTED_BATCH_SIZE);
    let mut token_mint_col = StringBuilder::with_capacity(EXPECTED_BATCH_SIZE, EXPECTED_BATCH_SIZE);
    let mut token_timestamp_col = Int64Builder::with_capacity(EXPECTED_BATCH_SIZE);
    let mut token_virtual_token_col = UInt64Builder::with_capacity(EXPECTED_BATCH_SIZE);
    let mut token_virtual_sol_col = UInt64Builder::with_capacity(EXPECTED_BATCH_SIZE);
    let mut token_real_token_col = UInt64Builder::with_capacity(EXPECTED_BATCH_SIZE);

    let mut num_ticks = 0;
    let mut num_tokens = 0;

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
            &HashSet::default(),
        )?;

        let (ticks, tokens) = notify_transaction(&meta, &tx)?;
        num_ticks += ticks.len();
        num_tokens += tokens.len();

        for tick in ticks.into_iter() {
            tick_sig_col.append_value(tx.signatures()[0].to_string());
            tick_mint_col.append_value(tick.mint.to_string());
            tick_is_buy_col.append_value(tick.is_buy);
            tick_sol_col.append_value(tick.sol_amount);
            tick_token_col.append_value(tick.token_amount);
            tick_timestamp_col.append_value(tick.timestamp);
            tick_virtual_sol_col.append_value(tick.virtual_sol_reserves);
            tick_virtual_token_col.append_value(tick.virtual_token_reserves);
            tick_real_sol_col.append_value(tick.real_sol_reserves);
            tick_real_token_col.append_value(tick.real_token_reserves);
        }

        for token in tokens.into_iter() {
            token_sig_col.append_value(tx.signatures()[0].to_string());
            token_name_col.append_value(&token.name);
            token_symbol_col.append_value(&token.symbol);
            token_uri_col.append_value(&token.uri);
            token_mint_col.append_value(token.mint.to_string());
            token_timestamp_col.append_value(token.timestamp);
            token_virtual_token_col.append_option(token.virtual_token_reserves);
            token_virtual_sol_col.append_option(token.virtual_sol_reserves);
            token_real_token_col.append_option(token.real_token_reserves);
        }
    }

    let tick_batch = RecordBatch::try_new(
        Arc::new(TICK_SCHEMA.clone()),
        vec![
            Arc::new(UInt64Array::from(vec![last_epoch; tick_sig_col.len()])),
            Arc::new(UInt64Array::from(vec![blk.slot; tick_sig_col.len()])),
            Arc::new(tick_sig_col.finish()),
            Arc::new(tick_mint_col.finish()),
            Arc::new(tick_is_buy_col.finish()),
            Arc::new(tick_sol_col.finish()),
            Arc::new(tick_token_col.finish()),
            Arc::new(tick_timestamp_col.finish()),
            Arc::new(tick_virtual_sol_col.finish()),
            Arc::new(tick_virtual_token_col.finish()),
            Arc::new(tick_real_sol_col.finish()),
            Arc::new(tick_real_token_col.finish()),
        ],
    )?;
    let token_batch = RecordBatch::try_new(
        Arc::new(TOKEN_SCHEMA.clone()),
        vec![
            Arc::new(UInt64Array::from(vec![last_epoch; token_sig_col.len()])),
            Arc::new(UInt64Array::from(vec![blk.slot; token_sig_col.len()])),
            Arc::new(token_sig_col.finish()),
            Arc::new(token_name_col.finish()),
            Arc::new(token_symbol_col.finish()),
            Arc::new(token_uri_col.finish()),
            Arc::new(token_mint_col.finish()),
            Arc::new(token_timestamp_col.finish()),
            Arc::new(token_virtual_token_col.finish()),
            Arc::new(token_virtual_sol_col.finish()),
            Arc::new(token_real_token_col.finish()),
        ],
    )?;

    try_join!(token_wr.write(&token_batch), tick_wr.write(&tick_batch))?;

    if num_ticks > 0 {
        println!("  wrote {} ticks", num_ticks);
    }
    if num_tokens > 0 {
        println!("  wrote {} tokens", num_tokens);
    }

    Ok(())
}

type TickRows = SmallVec<[TradeEvent; 100]>;
type TokenRows = SmallVec<[CreateEvent; 10]>;

fn notify_transaction(
    meta: &TransactionStatusMeta,
    tx: &SanitizedTransaction,
) -> Result<(TickRows, TokenRows), Box<dyn Error>> {
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
                    //.and_then(|i| i.iter().find(|i| i.index as usize == idx))
                    .and_then(|i| i.get(idx))
                    .map(|i| i.instructions.iter().map(|iisn| &iisn.instruction))
                    .into_iter()
                    .flatten(),
            )
        })
        .filter(|isn| ak[isn.program_id_index as usize] == *pump::PROGRAM_ID);

    let mut tick_rows = TickRows::default();
    let mut token_rows = TokenRows::default();

    for isn in isns {
        match pump::Instruction::try_from_slice(&isn.data) {
            Ok(Instruction::Buy(_))
            | Ok(Instruction::Sell(_))
            | Ok(Instruction::Create(_))
            | Ok(Instruction::Migrate(_))
            | Ok(Instruction::Other(_)) => {
                // ignore
            }
            Ok(Instruction::EmitCpi(EmitCpiInstruction { event })) => match event {
                Event::Trade(tev) => {
                    tick_rows.push(tev);
                }
                Event::Create(cev) => {
                    token_rows.push(cev);
                }
                Event::Complete(_) | Event::CompletePumpAmm(_) | Event::Other(_) => {}
            },
            Err(_e) => {
                //println!("Failed to decode instruction: {}", e);
                continue;
            }
        }
    }

    Ok((tick_rows, token_rows))
}
