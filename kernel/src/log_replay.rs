use std::collections::HashSet;
use tracing::debug;

use crate::actions::deletion_vector::DeletionVectorDescriptor;
use crate::engine_data::{GetData, TypedGetData as _};
use crate::{DeltaResult, EngineData, Error};

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

/// Base trait for visitors that process file actions during log replay
pub trait FileActionVisitor {
    /// Get a reference to the set of seen file keys
    fn seen_file_keys(&mut self) -> &mut HashSet<FileActionKey>;

    /// Checks if log replay already processed this logical file (in which case the current action
    /// should be ignored). If not already seen, register it so we can recognize future duplicates.
    /// Returns `true` if we have seen the file and should ignore it, `false` if we have not seen it
    /// and should process it.
    fn check_and_record_seen(&mut self, key: FileActionKey, is_log_batch: bool) -> bool {
        // Note: each (add.path + add.dv_unique_id()) pair has a
        // unique Add + Remove pair in the log. For example:
        // https://github.com/delta-io/delta/blob/master/spark/src/test/resources/delta/table-with-dv-large/_delta_log/00000000000000000001.json

        if self.seen_file_keys().contains(&key) {
            debug!(
                "Ignoring duplicate ({}, {:?}) in scan, is log {}",
                key.path, key.dv_unique_id, is_log_batch
            );
            true
        } else {
            debug!(
                "Including ({}, {:?}) in scan, is log {}",
                key.path, key.dv_unique_id, is_log_batch
            );
            if is_log_batch {
                // Remember file actions from this batch so we can ignore duplicates as we process
                // batches from older commit and/or checkpoint files. We don't track checkpoint
                // batches because they are already the oldest actions and never replace anything.
                self.seen_file_keys().insert(key);
            }
            false
        }
    }

    /// Index in getters array for add.path
    fn add_path_index(&self) -> usize;

    /// Index in getters array for remove.path
    fn remove_path_index(&self) -> Option<usize>;

    /// Starting index for add action's deletion vector getters
    /// (Assumes 3 consecutive items: storageType, pathOrInlineDv, offset)
    fn add_dv_start_index(&self) -> usize;

    /// Starting index for remove action's deletion vector getters
    /// (Assumes 3 consecutive items: storageType, pathOrInlineDv, offset)
    fn remove_dv_start_index(&self) -> Option<usize>;

    /// Extract deletion vector unique ID
    fn extract_dv_unique_id<'a>(
        &self,
        i: usize,
        getters: &[&'a dyn GetData<'a>],
        is_add: bool,
    ) -> DeltaResult<Option<String>> {
        // Get the starting index based on action type
        let start_idx = if is_add {
            self.add_dv_start_index()
        } else if let Some(idx) = self.remove_dv_start_index() {
            idx
        } else {
            return Err(Error::GenericError {
                source: "DV getters should exist".into(),
            });
        };

        // Extract the DV unique ID
        match getters[start_idx].get_opt(i, "deletionVector.storageType")? {
            Some(storage_type) => Ok(Some(DeletionVectorDescriptor::unique_id_from_parts(
                storage_type,
                getters[start_idx + 1].get(i, "deletionVector.pathOrInlineDv")?,
                getters[start_idx + 2].get_opt(i, "deletionVector.offset")?,
            ))),
            None => Ok(None),
        }
    }

    /// Extract file action key and determine if it's an add operation
    fn extract_file_action<'a>(
        &self,
        i: usize,
        getters: &[&'a dyn GetData<'a>],
    ) -> DeltaResult<Option<(FileActionKey, bool)>> {
        // Try to extract an add action path
        if let Some(path) = getters[self.add_path_index()].get_str(i, "add.path")? {
            let dv_unique_id = self.extract_dv_unique_id(i, getters, true)?;
            let file_key = FileActionKey::new(path, dv_unique_id);
            return Ok(Some((file_key, true)));
        }

        // The AddRemoveDedupVisitor does not include remove action getters when
        // dealing with non-log batches (since they are not needed for deduplication).
        let Some(remove_idx) = self.remove_path_index() else {
            return Ok(None);
        };

        // Try to extract a remove action path
        if let Some(path) = getters[remove_idx].get_str(i, "remove.path")? {
            let dv_unique_id = self.extract_dv_unique_id(i, getters, false)?;
            let file_key = FileActionKey::new(path, dv_unique_id);
            return Ok(Some((file_key, false)));
        }

        // No path found, not a file action
        Ok(None)
    }
}
