use serde::{Deserialize, Serialize};
use vsss_rs::Share;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Request {
    Share,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Response {
    Share(Share),
}
