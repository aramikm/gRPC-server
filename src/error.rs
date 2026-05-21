use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("storage error: {0}")]
    Storage(#[from] rocksdb::Error),

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("invalid page token")]
    InvalidPageToken,

    #[error("encode error: {0}")]
    Encode(#[from] prost::EncodeError),

    #[error("decode error: {0}")]
    Decode(#[from] prost::DecodeError),

    #[error("config error: {0}")]
    Config(String),
}

impl From<Error> for tonic::Status {
    fn from(e: Error) -> Self {
        match e {
            Error::InvalidArgument(msg) => tonic::Status::invalid_argument(msg),
            Error::InvalidPageToken => tonic::Status::invalid_argument("invalid page token"),
            Error::Storage(e) => tonic::Status::internal(format!("storage: {e}")),
            Error::Encode(e) => tonic::Status::internal(format!("encode: {e}")),
            Error::Decode(e) => tonic::Status::internal(format!("decode: {e}")),
            Error::Config(msg) => tonic::Status::internal(msg),
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
