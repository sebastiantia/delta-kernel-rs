use std::collections::HashSet;

use crate::{DeltaResult, EngineData};

#[derive(Debug, Hash, Eq, PartialEq)]
/// The subset of file action fields that uniquely identifies it in the log, used for deduplication
/// of adds and removes during log replay.
pub struct FileActionKey {
    pub(crate) path: String,
    pub(crate) dv_unique_id: Option<String>,
}

impl FileActionKey {
    pub fn new(path: impl Into<String>, dv_unique_id: Option<String>) -> Self {
        let path = path.into();
        Self { path, dv_unique_id }
    }
}

/// Trait defining the interface for log replay processors that process and filter
/// Delta Lake log actions based on different strategies.
pub trait LogReplayProcessor {
    /// The type of results produced by this processor
    type ProcessingResult;

    /// Process a batch of actions and return the filtered result
    fn process_batch(
        &mut self,
        batch: Box<dyn EngineData>,
        is_log_batch: bool,
    ) -> DeltaResult<Self::ProcessingResult>;

    // Get a reference to the set of seen file keys
    fn seen_file_keys(&mut self) -> &mut HashSet<FileActionKey>;
}
