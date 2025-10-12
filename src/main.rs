use std::collections::HashSet;
use std::error::Error;
use std::io::{self, Read};
use std::iter;

use borsh::BorshDeserialize;
use futures_util::TryStreamExt;
use prost::Message;
use reqwest::Client;
use tokio::fs;
use tokio_util::io::StreamReader;

use solana_bincode::limited_deserialize;
use solana_sdk::message::AddressLoader;
use solana_sdk::message::v0::{LoadedAddresses, MessageAddressTableLookup};
use solana_sdk::transaction::{
    AddressLoaderError, SanitizedTransaction, SanitizedVersionedTransaction, VersionedTransaction,
};
use solana_storage_proto::convert::generated;
use solana_transaction_status::TransactionStatusMeta;

use yellowstone_faithful_car_parser::node::{Node, NodeReader, Nodes};

use crate::pump::{
    BuyInstruction, CompleteEvent, CompletePumpAmmEvent, CreateEvent, CreateInstruction,
    EmitCpiInstruction, Event, Instruction, MigrateInstruction, SellInstruction, TradeEvent,
};

mod pump;

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

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    fs::create_dir_all("data").await?;

    let client = Client::new();
    let url = "https://files.old-faithful.net/800/epoch-800.car";

    let resp = client.get(url).send().await?.error_for_status()?;
    let mut rd = NodeReader::new(StreamReader::new(
        resp.bytes_stream()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e)),
    ));

    while let Ok(n) = Nodes::read_until_block(&mut rd).await {
        for (_, node) in n.nodes.iter() {
            if let Node::Block(blk) = node {
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
                    let meta = TransactionStatusMeta::try_from(
                        generated::TransactionStatusMeta::decode(&*metafrm)?,
                    )?;
                    if meta.status.is_err() {
                        continue;
                    }

                    let bodyfrm = n.reassemble_dataframes(&raw.data)?;
                    let body: VersionedTransaction =
                        limited_deserialize(&bodyfrm, 10 * 1024 * 1024)?;

                    let msg_hash = body.verify_and_hash_message()?;
                    let tx = SanitizedTransaction::try_new(
                        SanitizedVersionedTransaction::try_from(body)?,
                        msg_hash,
                        false,
                        MessageAddressLoaderFromTxMeta { meta: &meta },
                        &HashSet::default(),
                    )?;

                    notify_transaction(&meta, &tx)?;
                }
            }
        }
    }
    Ok(())
}

fn notify_transaction(
    meta: &TransactionStatusMeta,
    tx: &SanitizedTransaction,
) -> Result<(), Box<dyn Error>> {
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

    for isn in isns {
        match pump::Instruction::try_from_slice(&isn.data) {
            Ok(Instruction::Buy(BuyInstruction { .. })) => {
                println!("Buy");
            }
            Ok(Instruction::Sell(SellInstruction { .. })) => {
                println!("Sell");
            }
            Ok(Instruction::Create(CreateInstruction { .. })) => {
                println!("Create");
            }
            Ok(Instruction::Migrate(MigrateInstruction { .. })) => {
                println!("Migrate");
            }
            Ok(Instruction::EmitCpi(EmitCpiInstruction { event })) => match event {
                Event::Trade(TradeEvent { .. }) => {
                    println!("TradeEvent");
                }
                Event::Create(CreateEvent { .. }) => {
                    println!("CreateEvent");
                }
                Event::Complete(CompleteEvent { .. }) => {
                    println!("CompleteEvent");
                }
                Event::CompletePumpAmm(CompletePumpAmmEvent { .. }) => {
                    println!("CompletePumpAmmEvent");
                }
                Event::Other(_) => {
                    println!("Other event");
                }
            },
            Ok(Instruction::Other(_)) => {
                println!("Other instruction");
            }
            Err(e) => {
                println!("Failed to decode instruction: {}", e);
                continue;
            }
        }
    }

    Ok(())
}
