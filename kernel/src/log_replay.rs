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

    // Create a selection vector of appropriate length with all elements set to the given value
    fn create_selection_vector(
        &self,
        batch: &Box<dyn EngineData>,
        default_value: bool,
    ) -> Vec<bool> {
        let selection_vector = vec![default_value; batch.len()];
        assert_eq!(
            selection_vector.len(),
            batch.len(),
            "Selection vector length does not match actions length"
        );
        selection_vector
    }

    // Filter an iterator to only include results with at least one selected item
    fn filter_non_empty_results<T, I>(iter: I) -> impl Iterator<Item = DeltaResult<T>>
    where
        I: Iterator<Item = DeltaResult<T>>,
        T: HasSelectionVector,
    {
        iter.filter(|res| {
            res.as_ref()
                .map_or(true, |result| result.has_selected_rows())
        })
    }
}

/// Trait for types that contain a selection vector
pub trait HasSelectionVector {
    /// Check if the selection vector contains at least one selected row
    fn has_selected_rows(&self) -> bool;
}

/// Applies the given processor to the given iterator of action results,
/// and filters out batches with no selected rows.
///
/// This function abstracts the common pattern used by both checkpoint and scan iterators.
pub fn apply_processor_to_iterator<P>(
    mut processor: P,
    action_iter: impl Iterator<Item = DeltaResult<(Box<dyn EngineData>, bool)>>,
) -> impl Iterator<Item = DeltaResult<P::ProcessingResult>>
where
    P: LogReplayProcessor<ProcessingResult: HasSelectionVector>,
{
    action_iter
        .map(move |action_res| {
            let (batch, is_log_batch) = action_res?;
            processor.process_batch(batch, is_log_batch)
        })
        .filter(|res| {
            res.as_ref()
                .map_or(true, |result| result.has_selected_rows())
        })
}
