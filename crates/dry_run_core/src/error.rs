#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("connection failed: {0}")]
    Connection(String),

    #[error("authentication failed: {0}")]
    Auth(String),

    #[error("insufficient privileges: {0}")]
    Privilege(String),

    #[error("version parse error: {0}")]
    VersionParse(String),

    #[error("introspection failed: {0}")]
    Introspection(String),

    #[error("history store error: {0}")]
    History(String),

    #[error("config error: {0}")]
    Config(String),

    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
