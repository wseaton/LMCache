/// common types and utilities for S3 connector
/// priority levels for S3 operations (for future use)
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Priority {
    Peek = 0,
    Prefetch = 1,
    Get = 2,
    Put = 3,
}

impl Priority {
    #[allow(dead_code)]
    pub fn as_u8(&self) -> u8 {
        *self as u8
    }
}
