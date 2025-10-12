use std::io::{ErrorKind, Read, Result};
use std::str::FromStr;
use std::sync::LazyLock;

use borsh::BorshDeserialize;
use solana_sdk::pubkey::Pubkey;

pub static PROGRAM_ID: LazyLock<Pubkey> =
    LazyLock::new(|| Pubkey::from_str("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P").unwrap());

pub const BUY_DISCRIMINATOR: [u8; 8] = [102, 6, 61, 18, 1, 218, 235, 234];
pub const SELL_DISCRIMINATOR: [u8; 8] = [51, 230, 133, 164, 1, 127, 131, 173];
pub const CREATE_DISCRIMINATOR: [u8; 8] = [24, 30, 200, 40, 5, 28, 7, 119];
pub const MIGRATE_DISCRIMINATOR: [u8; 8] = [155, 234, 231, 146, 236, 158, 162, 30];
pub const EMIT_CPI_DISCRIMINATOR: [u8; 8] = [228, 69, 165, 46, 81, 203, 154, 29];

#[derive(Debug, Clone)]
pub enum Instruction {
    Buy(BuyInstruction),
    Sell(SellInstruction),
    Create(CreateInstruction),
    Migrate(MigrateInstruction),
    EmitCpi(EmitCpiInstruction),
    Other(Vec<u8>),
}

impl BorshDeserialize for Instruction {
    fn deserialize_reader<R: Read>(rd: &mut R) -> Result<Self> {
        let mut d = [0u8; 8];
        rd.read_exact(&mut d)?;

        match d {
            BUY_DISCRIMINATOR => {
                let ix = BuyInstruction::deserialize_reader(rd)?;
                Ok(Instruction::Buy(ix))
            }
            SELL_DISCRIMINATOR => {
                let ix = SellInstruction::deserialize_reader(rd)?;
                Ok(Instruction::Sell(ix))
            }
            CREATE_DISCRIMINATOR => {
                let ix = CreateInstruction::deserialize_reader(rd)?;
                Ok(Instruction::Create(ix))
            }
            MIGRATE_DISCRIMINATOR => {
                let ix = MigrateInstruction::deserialize_reader(rd)?;
                Ok(Instruction::Migrate(ix))
            }
            EMIT_CPI_DISCRIMINATOR => {
                let ix = EmitCpiInstruction::deserialize_reader(rd)?;
                Ok(Instruction::EmitCpi(ix))
            }
            _ => {
                let mut buf = Vec::new();
                rd.read_to_end(&mut buf)?;
                Ok(Instruction::Other(buf))
            }
        }
    }
}

#[derive(Debug, Clone, BorshDeserialize)]
pub struct BuyInstruction {
    pub amount: u64,
    pub max_sol_cost: u64,

    // added in pump_aug_28
    #[borsh(deserialize_with = "deserialize_option_bool")]
    pub track_volume: Option<bool>,

    #[borsh(deserialize_with = "deserialize_trailer")]
    pub trailer: Vec<u8>,
}

fn deserialize_option_bool<R: Read>(rd: &mut R) -> Result<Option<bool>> {
    let mut buf = [0u8; 1];
    match rd.read_exact(&mut buf) {
        Ok(()) => Ok(Some(buf[0] != 0)),
        Err(e) if e.kind() == ErrorKind::UnexpectedEof => Ok(None),
        Err(e) => {
            println!("deserialize_option_bool error: {:?}", e);
            Err(e)
        }
    }
}

fn deserialize_option<R: Read, T: BorshDeserialize>(rd: &mut R) -> Result<Option<T>> {
    match T::deserialize_reader(rd) {
        Ok(value) => Ok(Some(value)),
        Err(e) if e.to_string().contains("Unexpected length of input") => Ok(None),
        Err(e) if e.to_string().contains("Invalud bool representation") => Ok(None),
        Err(e) => Err(e),
    }
}

fn deserialize_trailer<R: Read>(rd: &mut R) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    rd.read_to_end(&mut buf)?;

    if buf.iter().all(|&b| b == 0) {
        Ok(buf)
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Non-zero bytes in trailer",
        ))
    }
}

#[derive(Debug, Clone, BorshDeserialize)]
pub struct SellInstruction {
    pub amount: u64,
    pub min_sol_received: u64,
    #[borsh(deserialize_with = "deserialize_trailer")]
    pub trailer: Vec<u8>,
}

#[derive(Debug, Clone, BorshDeserialize)]
pub struct CreateInstruction {
    pub name: String,
    pub symbol: String,
    pub uri: String,
    pub creator: Pubkey,
    #[borsh(deserialize_with = "deserialize_trailer")]
    pub trailer: Vec<u8>,
}

#[derive(Debug, Clone, BorshDeserialize)]
pub struct MigrateInstruction {
    #[borsh(deserialize_with = "deserialize_trailer")]
    pub trailer: Vec<u8>,
}

#[derive(Debug, Clone, BorshDeserialize)]
pub struct EmitCpiInstruction {
    pub event: Event,
}

pub const TRADE_EVENT_DISCRIMINATOR: [u8; 8] = [189, 219, 127, 211, 78, 230, 97, 238];
pub const CREATE_EVENT_DISCRIMINATOR: [u8; 8] = [27, 114, 169, 77, 222, 235, 99, 118];
pub const COMPLETE_EVENT_DISCRIMINATOR: [u8; 8] = [95, 114, 97, 156, 212, 46, 152, 8];
pub const COMPLETE_PUMP_AMM_EVENT_DISCRIMINATOR: [u8; 8] = [189, 233, 93, 185, 92, 148, 234, 148];

#[derive(Debug, Clone)]
pub enum Event {
    Trade(TradeEvent),
    Create(CreateEvent),
    Complete(CompleteEvent),
    CompletePumpAmm(CompletePumpAmmEvent),
    Other(Vec<u8>),
}

impl BorshDeserialize for Event {
    fn deserialize_reader<R: Read>(rd: &mut R) -> Result<Self> {
        let mut d = [0u8; 8];
        rd.read_exact(&mut d)?;

        match d {
            TRADE_EVENT_DISCRIMINATOR => {
                let ev = TradeEvent::deserialize_reader(rd)?;
                Ok(Event::Trade(ev))
            }
            CREATE_EVENT_DISCRIMINATOR => {
                let ev = CreateEvent::deserialize_reader(rd)?;
                Ok(Event::Create(ev))
            }
            COMPLETE_EVENT_DISCRIMINATOR => {
                let ev = CompleteEvent::deserialize_reader(rd)?;
                Ok(Event::Complete(ev))
            }
            COMPLETE_PUMP_AMM_EVENT_DISCRIMINATOR => {
                let ev = CompletePumpAmmEvent::deserialize_reader(rd)?;
                Ok(Event::CompletePumpAmm(ev))
            }
            _ => {
                let mut buf = Vec::new();
                rd.read_to_end(&mut buf)?;
                Ok(Event::Other(buf))
            }
        }
    }
}

#[derive(Debug, Clone, BorshDeserialize)]
pub struct TradeEvent {
    pub mint: Pubkey,
    pub sol_amount: u64,
    pub token_amount: u64,
    pub is_buy: bool,
    pub user: Pubkey,
    pub timestamp: i64,
    pub virtual_sol_reserves: u64,
    pub virtual_token_reserves: u64,
    pub real_sol_reserves: u64,
    pub real_token_reserves: u64,

    // added in May 08 (e2b66e4f)
    #[borsh(deserialize_with = "deserialize_option")]
    pub fee_recipient: Option<Pubkey>,
    #[borsh(deserialize_with = "deserialize_option")]
    pub fee_basis_points: Option<u64>,
    #[borsh(deserialize_with = "deserialize_option")]
    pub fee: Option<u64>,
    #[borsh(deserialize_with = "deserialize_option")]
    pub creator: Option<Pubkey>,
    #[borsh(deserialize_with = "deserialize_option")]
    pub creator_fee_basis_points: Option<u64>,
    #[borsh(deserialize_with = "deserialize_option")]
    pub creator_fee: Option<u64>,

    // added in Aug 28 (0c64400)
    #[borsh(deserialize_with = "deserialize_option")]
    pub track_volume: Option<bool>,
    #[borsh(deserialize_with = "deserialize_option")]
    pub total_unclaimed_tokens: Option<u64>,
    #[borsh(deserialize_with = "deserialize_option")]
    pub total_claimed_tokens: Option<u64>,
    #[borsh(deserialize_with = "deserialize_option")]
    pub current_sol_volume: Option<u64>,
    #[borsh(deserialize_with = "deserialize_option")]
    pub last_update_timestamp: Option<i64>,
}

#[derive(Debug, Clone, BorshDeserialize)]
pub struct CreateEvent {
    pub name: String,
    pub symbol: String,
    pub uri: String,
    pub mint: Pubkey,
    pub bonding_curve: Pubkey,
    pub user: Pubkey,
    pub creator: Pubkey,
    pub timestamp: i64,

    // added in May 08 (e2b66e4f)
    #[borsh(deserialize_with = "deserialize_option")]
    pub virtual_token_reserves: Option<u64>,
    #[borsh(deserialize_with = "deserialize_option")]
    pub virtual_sol_reserves: Option<u64>,
    #[borsh(deserialize_with = "deserialize_option")]
    pub real_token_reserves: Option<u64>,
    #[borsh(deserialize_with = "deserialize_option")]
    pub token_total_supply: Option<u64>,
}

#[derive(Debug, Clone, BorshDeserialize)]
pub struct CompleteEvent {
    pub user: Pubkey,
    pub mint: Pubkey,
    pub bonding_curve: Pubkey,
    pub timestamp: i64,
}

#[derive(Debug, Clone, BorshDeserialize)]
pub struct CompletePumpAmmEvent {
    pub user: Pubkey,
    pub mint: Pubkey,
    pub mint_amount: u64,
    pub sol_amount: u64,
    pub pool_migration_fee: u64,
    pub bonding_curve: Pubkey,
    pub timestamp: i64,
    pub pool: Pubkey,
}
