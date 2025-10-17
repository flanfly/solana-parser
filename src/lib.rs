use std::sync::atomic::{AtomicBool, Ordering};

use solana_sdk::signature::Signature;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::{fs, task};

use yellowstone_faithful_car_parser::node::{Block, Node, NodeReader, Nodes};

use crossbeam::queue::ArrayQueue;

mod arrow;
pub use crate::arrow::{TickBuilder, TokenBuilder};

mod solana;

mod pump;
pub use crate::pump::{CreateEvent, TradeEvent};

mod reader;
pub use crate::reader::{Progress, ProgressReader};
use crate::solana::notify_block;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub async fn pipeline<W, R, F>(
    ticks_fd: &mut W,
    tokens_fd: &mut W,
    nodes_fd: &mut R,
    checkpoint_file: F,
    epoch: u64,
) -> Result<()>
where
    W: AsyncWriteExt + Unpin + Send + 'static,
    R: AsyncReadExt + Unpin + Send + 'static,
    F: AsMut<fs::File> + Unpin + Send + 'static,
{
    // input queue
    let mut block_queue = ArrayQueue::<Nodes>::new(1024);

    // output queues
    let mut ticks_queue = ArrayQueue::<(u64, Signature, TradeEvent)>::new(1024);
    let mut tokens_queue = ArrayQueue::<(u64, Signature, CreateEvent)>::new(1024);

    // finished flag
    let finished = AtomicBool::new(false);

    // setup processing pipeline
    tokio_scoped::scope(|s| {
        for _ in 0..4 {
            s.spawn(async {
                loop {
                    match block_queue.pop() {
                        Some(n) => {
                            for (_, node) in n.nodes.iter() {
                                match node {
                                    Node::Block(blk) => {
                                        let res = notify_block(
                                            &n,
                                            &blk,
                                            &mut ticks_queue,
                                            &mut tokens_queue,
                                        )
                                        .await;
                                        match res {
                                            Ok(_) => {}
                                            Err(e) => {
                                                eprintln!("Block processor error: {}", e);
                                                finished.store(true, Ordering::SeqCst);
                                            }
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                        None if finished.load(Ordering::SeqCst) => {
                            break;
                        }
                        None => {
                            task::yield_now().await;
                        }
                    }
                }
            });
        }

        s.spawn(async {
            let res = async || -> Result<()> {
                let mut ticks = TickBuilder::new(ticks_fd, 1000)?;
                ticks.epoch(epoch);

                loop {
                    match ticks_queue.pop() {
                        Some((slot, tx, event)) => {
                            ticks.append(slot, tx, event);
                            if ticks.rows() >= 1000 {
                                ticks.write().await?;
                            }
                        }
                        None if finished.load(Ordering::SeqCst) => {
                            // flush remaining ticks
                            if ticks.rows() > 0 {
                                ticks.write().await?;
                            }
                            break;
                        }
                        None => {
                            task::yield_now().await;
                        }
                    }
                }

                ticks.close().await?;
                Ok(())
            };

            match res().await {
                Ok(_) => {}
                Err(e) => {
                    eprintln!("Tick consumer error: {}", e);
                    finished.store(true, Ordering::SeqCst);
                }
            }
        });

        s.spawn(async {
            let res = async || -> Result<()> {
                let mut tokens = TokenBuilder::new(tokens_fd, 1000)?;
                tokens.epoch(epoch);

                loop {
                    match tokens_queue.pop() {
                        Some((slot, tx, event)) => {
                            tokens.append(slot, tx, event);
                            if tokens.rows() >= 1000 {
                                tokens.write().await?;
                            }
                        }
                        None if finished.load(Ordering::SeqCst) => {
                            // flush remaining ticks
                            if tokens.rows() > 0 {
                                tokens.write().await?;
                            }
                            break;
                        }
                        None => {
                            task::yield_now().await;
                        }
                    }
                }

                tokens.close().await?;
                Ok(())
            };
            match res().await {
                Ok(_) => {}
                Err(e) => {
                    eprintln!("Token consumer error: {}", e);
                    finished.store(true, Ordering::SeqCst);
                }
            }
        });

        // save progress
        //let ticks_written = ticks.rows();
        // let tokens_written = tokens.rows();
        //progress.slot = blk.slot;
        //progress.end_offset = next_block_offset as u64;
        //progress.save(progress_file).await?;

        // write blocks to input queue
        s.spawn(async {
            let res = async || -> Result<()> {
                loop {
                    let nodes = Nodes::read_until_block(&mut NodeReader::new(nodes_fd)).await?;
                    while let Err(n) = block_queue.push(nodes) {
                        nodes = n;
                        task::yield_now().await;
                    }
                }
            };
            match res().await {
                Ok(_) => {}
                Err(e) => {
                    eprintln!("Block reader error: {}", e);
                    finished.store(true, Ordering::SeqCst);
                }
            }
        });

        Ok(())
    })
}
