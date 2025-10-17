use std::hint::black_box;
use std::path::PathBuf;
use std::sync::{Arc, atomic::AtomicU64};
use std::time;

use tokio::fs;
use tokio::runtime::Runtime;

use criterion::{Criterion, criterion_group, criterion_main};

use yellowstone_faithful_car_parser::node::NodeReader;

use solana_parser::{Progress, TickBuilder, TokenBuilder, consume_block};

struct ConsumeNodeArgs {
    rd: fs::File,
    progress: Progress,
    progress_file: PathBuf,
    bytes_counter: Arc<AtomicU64>,
    ticks: TickBuilder<fs::File>,
    tokens: TokenBuilder<fs::File>,
}

fn criterion_benchmark(c: &mut Criterion) {
    let batch_size = 1000;

    c.bench_function("consume_block", move |b| {
        let rt = Runtime::new().expect("Failed to create Tokio runtime");
        b.to_async(rt).iter_custom(async |num| {
            let mut dur = time::Duration::ZERO;

            for _ in 0..num {
                let rd = fs::File::open("epoch-800.car")
                    .await
                    .expect("test input open failed");
                let tickwr = fs::File::create("/dev/null")
                    .await
                    .expect("tick writer create failed");
                let tokenwr = fs::File::create("/dev/null")
                    .await
                    .expect("token writer create failed");
                let mut args = ConsumeNodeArgs {
                    rd,
                    progress: Progress::default(),
                    progress_file: "real".into(),
                    bytes_counter: Arc::new(AtomicU64::new(0)),
                    ticks: TickBuilder::new(tickwr, batch_size).expect("TickBuilder::new failed"),
                    tokens: TokenBuilder::new(tokenwr, batch_size)
                        .expect("TokenBuilder::new failed"),
                };

                let start = time::Instant::now();
                let mut nrd = NodeReader::new(args.rd);
                let res = consume_block(
                    &mut nrd,
                    &mut args.progress,
                    &args.progress_file,
                    args.bytes_counter,
                    &mut args.ticks,
                    &mut args.tokens,
                );
                black_box(res.await).expect("consume_block failed");
                dur += start.elapsed();
            }
            dur
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
