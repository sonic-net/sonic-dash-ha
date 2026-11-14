pub mod message_id_generator;
pub mod result;
// tonic's generated client returns Result<_, tonic::Status>.
#[allow(clippy::result_large_err)]
pub mod swbus;
