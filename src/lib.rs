mod arrow;
pub use crate::arrow::{TickBuilder, TokenBuilder};

mod pump;
pub use crate::pump::{CreateEvent, TradeEvent};

mod solana;
pub use crate::solana::notify_block;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
