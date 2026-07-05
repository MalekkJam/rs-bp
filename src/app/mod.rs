pub mod cli;
mod persistence;
mod runtime;

pub(crate) type AppResult<T> = Result<T, Box<dyn std::error::Error>>;
