use thiserror::Error;

#[derive(Error, Debug)]
pub enum DbError {
    #[error("Table already exists")]
    TableExists,
}
