use std::io::Read;
use std::iter;
use std::result;

use solana_bincode::limited_deserialize;
use solana_sdk::message::AddressLoader;
use solana_sdk::message::v0::{LoadedAddresses, MessageAddressTableLookup};
use solana_sdk::transaction::{
    AddressLoaderError, SanitizedTransaction, SanitizedVersionedTransaction, VersionedTransaction,
};
use solana_storage_proto::convert::generated;
use solana_transaction_status::TransactionStatusMeta;

use tokio::fs;

use borsh::BorshDeserialize;
use yellowstone_faithful_car_parser::node::{Block, Node, Nodes};

use prost::Message;

use crate::Result;
use crate::arrow::{TickBuilder, TokenBuilder};
use crate::pump::{CreateEvent, EmitCpiInstruction, Event, Instruction, PROGRAM_ID, TradeEvent};

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

fn decompress_zstd(data: Vec<u8>) -> Result<Vec<u8>> {
    let mut decoder = zstd::Decoder::new(&data[..])?;
    let mut decompressed = Vec::new();
    decoder.read_to_end(&mut decompressed)?;
    Ok(decompressed)
}

pub async fn notify_block(
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
            .filter(|isn| ak[isn.program_id_index as usize] == *PROGRAM_ID);

        for isn in isns {
            match Instruction::try_from_slice(&isn.data) {
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
