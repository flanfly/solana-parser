use std::sync::{Arc, LazyLock};

use arrow::array::{
    ArrayBuilder, BooleanBuilder, Int64Builder, RecordBatch, StringBuilder, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use parquet::arrow::AsyncArrowWriter;
use parquet::arrow::async_writer::AsyncFileWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use solana_sdk::signature::Signature;

use crate::Result;
use crate::pump::{CreateEvent, TradeEvent};

static TICK_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    SchemaRef::new(Schema::new(vec![
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
    ]))
});

pub struct TickBuilder<W: AsyncFileWriter + Unpin + Send> {
    epoch: Option<u64>,
    writer: AsyncArrowWriter<W>,

    // builders
    col_epoch: UInt64Builder,
    col_slot: UInt64Builder,
    col_transaction: StringBuilder,
    col_mint: StringBuilder,
    col_is_buy: BooleanBuilder,
    col_sol: UInt64Builder,
    col_token: UInt64Builder,
    col_timestamp: Int64Builder,
    col_virtual_sol: UInt64Builder,
    col_virtual_token: UInt64Builder,
    col_real_sol: UInt64Builder,
    col_real_token: UInt64Builder,
}

impl<W: AsyncFileWriter + Send + Unpin> TickBuilder<W> {
    pub fn new(fd: W, batch_size: usize) -> Result<Self> {
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let aaw = AsyncArrowWriter::try_new(fd, TICK_SCHEMA.clone(), Some(props))?;
        let t = Self {
            epoch: None,
            writer: aaw,
            col_epoch: UInt64Builder::with_capacity(batch_size),
            col_slot: UInt64Builder::with_capacity(batch_size),
            col_transaction: StringBuilder::with_capacity(batch_size, batch_size * 64),
            col_mint: StringBuilder::with_capacity(batch_size, batch_size * 44),
            col_is_buy: BooleanBuilder::with_capacity(batch_size),
            col_sol: UInt64Builder::with_capacity(batch_size),
            col_token: UInt64Builder::with_capacity(batch_size),
            col_timestamp: Int64Builder::with_capacity(batch_size),
            col_virtual_sol: UInt64Builder::with_capacity(batch_size),
            col_virtual_token: UInt64Builder::with_capacity(batch_size),
            col_real_sol: UInt64Builder::with_capacity(batch_size),
            col_real_token: UInt64Builder::with_capacity(batch_size),
        };

        Ok(t)
    }

    pub fn epoch(&mut self, epoch: u64) {
        self.epoch = Some(epoch);
    }

    pub fn rows(&self) -> usize {
        self.col_epoch.len()
    }

    pub fn append(&mut self, slot: u64, tx: Signature, event: TradeEvent) {
        self.col_epoch
            .append_value(self.epoch.expect("epoch not set"));
        self.col_slot.append_value(slot);
        self.col_transaction.append_value(tx.to_string());
        self.col_mint.append_value(event.mint.to_string());
        self.col_is_buy.append_value(event.is_buy);
        self.col_sol.append_value(event.sol_amount);
        self.col_token.append_value(event.token_amount);
        self.col_timestamp.append_value(event.timestamp);
        self.col_virtual_sol
            .append_value(event.virtual_sol_reserves);
        self.col_virtual_token
            .append_value(event.virtual_token_reserves);
        self.col_real_sol.append_value(event.virtual_sol_reserves);
        self.col_real_token
            .append_value(event.virtual_token_reserves);
    }

    pub async fn write(&mut self) -> Result<()> {
        let batch = RecordBatch::try_new(
            TICK_SCHEMA.clone(),
            vec![
                Arc::new(self.col_epoch.finish()),
                Arc::new(self.col_slot.finish()),
                Arc::new(self.col_transaction.finish()),
                Arc::new(self.col_mint.finish()),
                Arc::new(self.col_is_buy.finish()),
                Arc::new(self.col_sol.finish()),
                Arc::new(self.col_token.finish()),
                Arc::new(self.col_timestamp.finish()),
                Arc::new(self.col_virtual_sol.finish()),
                Arc::new(self.col_virtual_token.finish()),
                Arc::new(self.col_real_sol.finish()),
                Arc::new(self.col_real_token.finish()),
            ],
        )?;
        self.writer.write(&batch).await.map_err(|e| e.into())
    }

    pub async fn close(self) -> Result<()> {
        let _ = self.writer.close().await?;
        Ok(())
    }
}

static TOKEN_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    SchemaRef::new(Schema::new(vec![
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
    ]))
});

pub struct TokenBuilder<W: AsyncFileWriter + Unpin + Send> {
    epoch: Option<u64>,
    writer: AsyncArrowWriter<W>,

    // builders
    col_epoch: UInt64Builder,
    col_slot: UInt64Builder,
    col_transaction: StringBuilder,
    col_name: StringBuilder,
    col_symbol: StringBuilder,
    col_uri: StringBuilder,
    col_mint: StringBuilder,
    col_timestamp: Int64Builder,
    col_virtual_token: UInt64Builder,
    col_virtual_sol: UInt64Builder,
    col_real_token: UInt64Builder,
}

impl<W: AsyncFileWriter + Send + Unpin> TokenBuilder<W> {
    pub fn new(fd: W, batch_size: usize) -> Result<Self> {
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let aaw = AsyncArrowWriter::try_new(fd, TOKEN_SCHEMA.clone(), Some(props))?;
        let t = Self {
            epoch: None,
            writer: aaw,
            col_epoch: UInt64Builder::with_capacity(batch_size),
            col_slot: UInt64Builder::with_capacity(batch_size),
            col_transaction: StringBuilder::with_capacity(batch_size, batch_size * 64),
            col_name: StringBuilder::with_capacity(batch_size, batch_size * 32),
            col_symbol: StringBuilder::with_capacity(batch_size, batch_size * 16),
            col_uri: StringBuilder::with_capacity(batch_size, batch_size * 128),
            col_mint: StringBuilder::with_capacity(batch_size, batch_size * 44),
            col_timestamp: Int64Builder::with_capacity(batch_size),
            col_virtual_token: UInt64Builder::with_capacity(batch_size),
            col_virtual_sol: UInt64Builder::with_capacity(batch_size),
            col_real_token: UInt64Builder::with_capacity(batch_size),
        };

        Ok(t)
    }

    pub fn epoch(&mut self, epoch: u64) {
        self.epoch = Some(epoch);
    }

    pub fn rows(&self) -> usize {
        self.col_epoch.len()
    }

    pub fn append(&mut self, slot: u64, tx: Signature, event: CreateEvent) {
        self.col_epoch
            .append_value(self.epoch.expect("epoch not set"));
        self.col_slot.append_value(slot);
        self.col_transaction.append_value(tx.to_string());
        self.col_name.append_value(event.name);
        self.col_symbol.append_value(event.symbol);
        self.col_uri.append_value(event.uri);
        self.col_mint.append_value(event.mint.to_string());
        self.col_timestamp.append_value(event.timestamp);
        if let Some(vt) = event.virtual_token_reserves {
            self.col_virtual_token.append_value(vt);
        } else {
            self.col_virtual_token.append_null();
        }
        if let Some(vs) = event.virtual_sol_reserves {
            self.col_virtual_sol.append_value(vs);
        } else {
            self.col_virtual_sol.append_null();
        }
        if let Some(rt) = event.real_token_reserves {
            self.col_real_token.append_value(rt);
        } else {
            self.col_real_token.append_null();
        }
    }

    pub async fn write(&mut self) -> Result<()> {
        let batch = RecordBatch::try_new(
            TOKEN_SCHEMA.clone(),
            vec![
                Arc::new(self.col_epoch.finish()),
                Arc::new(self.col_slot.finish()),
                Arc::new(self.col_transaction.finish()),
                Arc::new(self.col_name.finish()),
                Arc::new(self.col_symbol.finish()),
                Arc::new(self.col_uri.finish()),
                Arc::new(self.col_mint.finish()),
                Arc::new(self.col_timestamp.finish()),
                Arc::new(self.col_virtual_token.finish()),
                Arc::new(self.col_virtual_sol.finish()),
                Arc::new(self.col_real_token.finish()),
            ],
        )?;
        self.writer.write(&batch).await.map_err(|e| e.into())
    }

    pub async fn close(self) -> Result<()> {
        let _ = self.writer.close().await?;
        Ok(())
    }
}
