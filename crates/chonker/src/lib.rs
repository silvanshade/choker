#![deny(clippy::all)]
#![deny(clippy::pedantic)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::similar_names)]
#![allow(unused)]

mod archive;
pub(crate) mod cdc;
pub(crate) mod codec;
mod crypto;
mod database;
pub mod io;

pub use crate::{
    archive::ChonkerArchive,
    codec::{decode::DecodeContext, encode::EncodeContext},
};

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;
type BoxResult<T> = Result<T, BoxError>;
