use std::io;

use tokio::{fs, select, try_join};
use tokio_util::io::StreamReader;

use yellowstone_faithful_car_parser::node::NodeReader;

use clap::Parser;
use futures_util::TryStreamExt;
use reqwest::{Client, Method, header};

use solana_parser::{Progress, ProgressReader, Result, TickBuilder, TokenBuilder, consume_block};

mod cli;
use crate::cli::{Abort, Cli};

async fn open_parquet_file(typ: &str, epoch: u64, start_offset: u64) -> Result<fs::File> {
    let path = format!("data/{}-{}-{}.parquet", typ, epoch, start_offset);
    let fd = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)
        .await?;
    Ok(fd)
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    fs::create_dir_all(cli.data_dir).await?;

    let mut progress = match (cli.epoch, Progress::load(&cli.progress_file).await) {
        (Some(epoch), Err(_)) => {
            let mut p = Progress::default();
            p.epoch = epoch;
            p
        }
        (Some(epoch), Ok(_)) => {
            eprintln!(
                "Warning: progress file exists, but --epoch is specified. Ignoring progress file and starting from epoch={}",
                epoch
            );
            let mut p = Progress::default();
            p.epoch = epoch;
            p
        }
        (None, Ok(p)) => {
            println!(
                "resume epoch={}, slot={}, offset={}",
                p.epoch,
                p.slot + 1,
                p.end_offset
            );
            p
        }
        (None, Err(_)) => {
            let mut p = Progress::default();
            if let Some(epoch) = cli.epoch {
                p.epoch = epoch;
            }
            println!("start epoch={}", p.epoch);
            p
        }
    };

    let (ticks_fd, tokens_fd) = try_join! {
        open_parquet_file("ticks", progress.epoch, progress.end_offset),
        open_parquet_file("tokens", progress.epoch, progress.end_offset),
    }?;

    let ticks_fd = fs::File::open("/dev/null").await?;
    let tokens_fd = fs::File::open("/dev/null").await?;

    let mut ticks = TickBuilder::new(ticks_fd, 1000)?;
    ticks.epoch(progress.epoch);

    let mut tokens = TokenBuilder::new(tokens_fd, 1000)?;
    tokens.epoch(progress.epoch);

    let client = Client::new();
    let url = format!(
        "https://files.old-faithful.net/{0}/epoch-{0}.car",
        progress.epoch
    );
    let mut req = client.request(Method::GET, url);

    if progress.end_offset > 0 {
        let range = format!("bytes={}-", progress.end_offset);
        req = req.header(header::RANGE, range);
    }

    let resp = client.execute(req.build()?).await?.error_for_status()?;
    let total_size = resp
        .content_length()
        .ok_or("Failed to get content length")?;

    println!("total_size={}", total_size);

    //let fd = fs::File::open("epoch-800.car").await?;
    //let total_size = fd.metadata().await?.len();

    let rd = StreamReader::new(
        resp.bytes_stream()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e)),
    );
    let rd = ProgressReader::new(total_size, rd);
    let mut rd = NodeReader::new(rd);
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
            res = consume_block(&mut rd, &mut progress, &cli.progress_file, &mut ticks, &mut tokens) => {
                match res {
                    Ok(()) => {}
                    Err(e) => {
                        println!("Error consuming block: {:?}, flushing and exiting...", e);
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
