use std::collections::HashMap;

use proc_macro::{TokenStream, TokenTree};

#[derive(thiserror::Error, Debug)]
enum Error {
    #[error("parse error: {0}")]
    Parse(String),
    #[error("unexpected token: {0}")]
    UnexpectedToken(String),
    #[error("expected token")]
    ExpectedToken,
    #[error("lock family required: support \"parking_lot\" and \"tokio\"")]
    LockFamilyRequired,
    #[error("unsupported lock family: {0}")]
    UnsupportedLockFamily(String),
}

type Result<T> = core::result::Result<T, Error>;

#[derive(Debug)]
enum LockFamily {
    None,
    ParkingLot,
    Tokio,
}

#[derive(Debug)]
enum LockType {
    ParkingLotMutex,
    ParkingLotRwLock,
    TokioMutex,
    TokioRwLock,
}

impl std::str::FromStr for LockType {
    type Err = Error;

    fn from_str(s: &str) -> core::result::Result<Self, Self::Err> {
        match s {
            "ParkingLotMutex" => Ok(Self::ParkingLotMutex),
            "ParkingLotRwLock" => Ok(Self::ParkingLotRwLock),
            "TokioMutex" => Ok(Self::TokioMutex),
            "TokioRwLock" => Ok(Self::TokioRwLock),
            _ => Err(Error::Parse(s.to_string())),
        }
    }
}

fn ensure_punct(token: &TokenTree, expected: &str) -> Result<()> {
    match token {
        TokenTree::Punct(punct) => {
            if punct.to_string().as_str() != expected {
                return Err(Error::UnexpectedToken(punct.to_string()));
            }
        }
        _ => return Err(Error::UnexpectedToken(token.to_string())),
    }
    Ok(())
}

fn read_ident_string(token: &TokenTree) -> Result<String> {
    match token {
        TokenTree::Ident(ident) => Ok(ident.to_string()),
        _ => Err(Error::UnexpectedToken(token.to_string())),
    }
}

fn mapping(attr: TokenStream) -> Result<HashMap<String, LockType>> {
    let mut result = HashMap::new();

    let mut iter = attr.into_iter();
    let lock_family = match iter.next().ok_or(Error::LockFamilyRequired)? {
        TokenTree::Ident(ident) => match ident.to_string().as_str() {
            "_" => LockFamily::None,
            "parking_lot" => LockFamily::ParkingLot,
            "tokio" => LockFamily::Tokio,
            _ => return Err(Error::UnsupportedLockFamily(ident.to_string())),
        },
        _ => return Err(Error::LockFamilyRequired),
    };

    match lock_family {
        LockFamily::None => {}
        LockFamily::ParkingLot => {
            result.insert("Mutex".to_string(), LockType::ParkingLotMutex);
            result.insert("RwLock".to_string(), LockType::ParkingLotRwLock);
        }
        LockFamily::Tokio => {
            result.insert("Mutex".to_string(), LockType::TokioMutex);
            result.insert("RwLock".to_string(), LockType::TokioRwLock);
        }
    }

    while let Some(token) = iter.next() {
        ensure_punct(&token, ",")?;
        let t_name = iter.next().ok_or(Error::ExpectedToken)?;
        let name = read_ident_string(&t_name)?;
        ensure_punct(&iter.next().ok_or(Error::ExpectedToken)?, "=")?;
        let t_lock_type = iter.next().ok_or(Error::ExpectedToken)?;
        let lock_type = read_ident_string(&t_lock_type)?.parse()?;
        result.insert(name, lock_type);
    }

    Ok(result)
}

#[proc_macro_attribute]
pub fn trace_lock(attr: TokenStream, item: TokenStream) -> TokenStream {
    println!("attr:\n{:#?}", attr);

    let mapping = mapping(attr).unwrap();

    println!("mapping: {:?}", mapping);

    println!("item:\n{:#?}", item);
    item
}
