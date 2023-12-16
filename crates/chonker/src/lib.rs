#![deny(clippy::all)]
#![deny(clippy::pedantic)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::module_name_repetitions)]

mod archive;
pub(crate) mod cdc;
pub(crate) mod codec;
mod crypto;
mod database;
mod io;

pub use archive::Archive;
pub use codec::encode::EncodeContext;

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;
type BoxResult<T> = Result<T, BoxError>;
