//! This module implements log replay functionality specifically for checkpoint writes in delta tables.
//!
//! The primary goal is to process Delta log actions in reverse chronological order (from most recent to
//! least recent) to produce the minimal set of actions required to reconstruct the table state in a checkpoint.
//!
//! ## Key Responsibilities
//! - Filtering: Only the most recent protocol and metadata actions are retained, and for each transaction
//!   (identified by its app ID), only the latest action is kept.
//! - Deduplication: File actions are deduplicated based on file path and deletion vector unique ID so that
//!   duplicate or obsolete actions (including remove actions) are ignored.
//! - Retention Filtering: Tombstones older than the configured `minimum_file_retention_timestamp` are excluded.
//!
//! The module defines the [`CheckpointLogReplayProcessor`] which implements the LogReplayProcessor trait,
//! as well as a [`CheckpointVisitor`] to traverse and process batches of log actions.
//!
//! The processing result is encapsulated in [`CheckpointData`], which includes the log data accompanied with
//! a selection vector indicating which rows should be included in the checkpoint file.
//!
//! For log replay functionality used during table scans (i.e. for reading checkpoints and commit logs), refer to
//! the `scan/log_replay.rs` module.
use std::cell::RefCell;
use std::collections::HashSet;
use std::rc::Rc;
use std::sync::LazyLock;

use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
use crate::log_replay::{
    FileActionDeduplicator, FileActionKey, HasSelectionVector, LogReplayProcessor,
};
use crate::scan::data_skipping::DataSkippingFilter;
use crate::schema::{column_name, ColumnName, ColumnNamesAndTypes, DataType};
use crate::utils::require;
use crate::{DeltaResult, EngineData, Error};

/// TODO!(seb): Change this to `type CheckpointData = FilteredEngineData` once available.
///
/// [`CheckpointData`] represents a batch of actions filtered for checkpoint creation.
/// It wraps a single engine data batch and a corresponding selection vector indicating
/// which rows should be written to the checkpoint file.
pub(crate) struct CheckpointData {
    /// The original engine data containing the actions
    #[allow(dead_code)] // TODO: Remove once checkpoint_v1 API is implemented
    data: Box<dyn EngineData>,
    /// Boolean vector indicating which rows should be included in the checkpoint
    selection_vector: Vec<bool>,
}

impl HasSelectionVector for CheckpointData {
    /// Returns true if any row in the selection vector is marked as selected
    fn has_selected_rows(&self) -> bool {
        self.selection_vector.contains(&true)
    }
}

/// The [`CheckpointLogReplayProcessor`] is an implementation of the [`LogReplayProcessor`]
/// trait that filters log segment actions for inclusion in a V1 spec checkpoint file.
///
/// It processes each action batch via the `process_actions_batch` method, using the
/// [`CheckpointVisitor`] to convert each [`EngineData`] batch into a [`CheckpointData`]
/// instance that reflect only the necessary actions for the checkpoint.
pub(crate) struct CheckpointLogReplayProcessor {
    /// Tracks file actions that have been seen during log replay to avoid duplicates.
    /// Contains (data file path, dv_unique_id) pairs as `FileActionKey` instances.
    seen_file_keys: HashSet<FileActionKey>,

    // Rc<RefCell<i64>> provides shared mutability for our counters, allowing both the
    // iterator to update the values during processing and the caller to observe the final
    // counts afterward. Note that this approach is not thread-safe and only works in
    // single-threaded contexts, which means the iterator cannot be sent across thread
    // boundaries (no Send trait).
    total_actions: Rc<RefCell<i64>>,
    total_add_actions: Rc<RefCell<i64>>,

    /// Indicates whether a protocol action has been seen in the log.
    seen_protocol: bool,
    /// Indicates whether a metadata action has been seen in the log.
    seen_metadata: bool,
    /// Set of transaction app IDs that have been processed to avoid duplicates.
    seen_txns: HashSet<String>,
    /// Minimum timestamp for file retention, used for filtering expired tombstones.
    minimum_file_retention_timestamp: i64,
}

impl LogReplayProcessor for CheckpointLogReplayProcessor {
    type Output = CheckpointData;

    /// This function is applied to each batch of actions read from the log during
    /// log replay in reverse chronological order (from most recent to least recent),
    /// and performs the necessary filtering and deduplication to produce the minimal
    /// set of actions to be written to the checkpoint file.
    ///
    /// # Filtering Rules
    ///
    /// 1. Only the most recent protocol and metadata actions are included
    /// 2. For each app ID, only the most recent transaction action is included
    /// 3. Add and remove actions are deduplicated based on path and unique ID
    /// 4. Remove tombstones older than `minimum_file_retention_timestamp` are excluded
    /// 5. Sidecar, commitInfo, and CDC actions are excluded
    fn process_actions_batch(
        &mut self,
        batch: Box<dyn EngineData>,
        is_log_batch: bool,
    ) -> DeltaResult<Self::Output> {
        let selection_vector = vec![true; batch.len()];
        assert_eq!(
            selection_vector.len(),
            batch.len(),
            "Initial selection vector length does not match actions length"
        );

        // Create the checkpoint visitor to process actions and update selection vector
        let mut visitor = CheckpointVisitor::new(
            &mut self.seen_file_keys,
            is_log_batch,
            selection_vector,
            self.minimum_file_retention_timestamp,
            self.seen_protocol,
            self.seen_metadata,
            &mut self.seen_txns,
        );
        visitor.visit_rows_of(batch.as_ref())?;

        // Update the total actions and add actions counters
        *self.total_actions.borrow_mut() +=
            visitor.total_file_actions + visitor.total_non_file_actions;
        *self.total_add_actions.borrow_mut() += visitor.total_add_actions;

        // Update protocol and metadata seen flags
        self.seen_protocol = visitor.seen_protocol;
        self.seen_metadata = visitor.seen_metadata;

        Ok(CheckpointData {
            data: batch,
            selection_vector: visitor.selection_vector,
        })
    }

    /// Data skipping is not applicable for checkpoint log replay.
    fn data_skipping_filter(&self) -> Option<&DataSkippingFilter> {
        None
    }
}

impl CheckpointLogReplayProcessor {
    pub(crate) fn new(
        total_actions_counter: Rc<RefCell<i64>>,
        total_add_actions_counter: Rc<RefCell<i64>>,
        minimum_file_retention_timestamp: i64,
    ) -> Self {
        Self {
            seen_file_keys: Default::default(),
            total_actions: total_actions_counter,
            total_add_actions: total_add_actions_counter,
            seen_protocol: false,
            seen_metadata: false,
            seen_txns: Default::default(),
            minimum_file_retention_timestamp,
        }
    }
}

/// Given an iterator of (engine_data, bool) tuples, returns an iterator of
/// `(engine_data, selection_vec)`. Each row that is selected in the returned `engine_data` _must_
/// be written to the V1 checkpoint file in order to capture the table version's complete state.
/// Non-selected rows _must_ be ignored. The boolean flag tied to each actions batch indicates
/// whether the batch is a commit batch (true) or a checkpoint batch (false).
///
/// Note: The 'action_iter' parameter is an iterator of (engine_data, bool) tuples that _must_ be
/// sorted by the order of the actions in the log from most recent to least recent.
#[allow(unused)] // TODO: Remove once checkpoint_v1 API is implemented
pub(crate) fn checkpoint_actions_iter(
    action_iter: impl Iterator<Item = DeltaResult<(Box<dyn EngineData>, bool)>>,
    total_actions_counter: Rc<RefCell<i64>>,
    total_add_actions_counter: Rc<RefCell<i64>>,
    minimum_file_retention_timestamp: i64,
) -> impl Iterator<Item = DeltaResult<CheckpointData>> {
    CheckpointLogReplayProcessor::new(
        total_actions_counter,
        total_add_actions_counter,
        minimum_file_retention_timestamp,
    )
    .process_actions_iter(action_iter)
}

/// A visitor that filters actions for inclusion in a V1 spec checkpoint file.
///
/// This visitor processes actions in newest-to-oldest order (as they appear in log
/// replay) and applies deduplication logic for both file and non-file actions to
/// produce the minimal state representation for the table.
///
/// # File Action Filtering
/// - Keeps only the first occurrence of each unique (path, dvId) pair
/// - Excludes expired tombstone remove actions (where deletionTimestamp ≤ minimumFileRetentionTimestamp)
/// - Add actions represent files present in the table
/// - Unexpired remove actions represent tombstones still needed for consistency
///
/// # Non-File Action Filtering
/// - Keeps only the first protocol action (newest version)
/// - Keeps only the first metadata action (most recent table metadata)
/// - Keeps only the first transaction action for each unique app ID
///
/// # Excluded Actions
/// CommitInfo, CDC, Sidecar, and CheckpointMetadata actions are NOT part of the V1 checkpoint schema
/// and are filtered out.
///
/// The resulting filtered set of actions represents the minimal set needed to reconstruct
/// the latest valid state of the table at the checkpointed version.
pub(crate) struct CheckpointVisitor<'seen> {
    // Desduplicates file actions
    deduplicator: FileActionDeduplicator<'seen>,
    // Tracks which rows to include in the final output
    selection_vector: Vec<bool>,
    // TODO: _last_checkpoint schema should be updated to use u64 instead of i64
    // for fields that are not expected to be negative. (Issue #786)
    // i64 to match the `_last_checkpoint` file schema
    total_file_actions: i64,
    // i64 to match the `_last_checkpoint` file schema
    total_add_actions: i64,
    // i64 for comparison with remove.deletionTimestamp
    minimum_file_retention_timestamp: i64,
    // Flag to keep only the first protocol action
    seen_protocol: bool,
    // Flag to keep only the first metadata action
    seen_metadata: bool,
    // Set of transaction IDs to deduplicate by appId
    seen_txns: &'seen mut HashSet<String>,
    // i64 to match the `_last_checkpoint` file schema
    total_non_file_actions: i64,
}

#[allow(unused)]
impl CheckpointVisitor<'_> {
    // These index positions correspond to the order of columns defined in
    // `selected_column_names_and_types()`, and are used to extract file key information
    // for deduplication purposes
    const ADD_PATH_INDEX: usize = 0; // Position of "add.path" in getters
    const ADD_DV_START_INDEX: usize = 1; // Start position of add deletion vector columns
    const REMOVE_PATH_INDEX: usize = 4; // Position of "remove.path" in getters
    const REMOVE_DV_START_INDEX: usize = 6; // Start position of remove deletion vector columns

    pub(crate) fn new<'seen>(
        seen_file_keys: &'seen mut HashSet<FileActionKey>,
        is_log_batch: bool,
        selection_vector: Vec<bool>,
        minimum_file_retention_timestamp: i64,
        seen_protocol: bool,
        seen_metadata: bool,
        seen_txns: &'seen mut HashSet<String>,
    ) -> CheckpointVisitor<'seen> {
        CheckpointVisitor {
            deduplicator: FileActionDeduplicator::new(
                seen_file_keys,
                is_log_batch,
                Self::ADD_PATH_INDEX,
                Self::REMOVE_PATH_INDEX,
                Self::ADD_DV_START_INDEX,
                Self::REMOVE_DV_START_INDEX,
            ),
            selection_vector,
            total_file_actions: 0,
            total_add_actions: 0,
            minimum_file_retention_timestamp,

            seen_protocol,
            seen_metadata,
            seen_txns,
            total_non_file_actions: 0,
        }
    }

    /// Determines if a remove action tombstone has expired and should be excluded from the checkpoint.
    ///
    /// A remove action includes a timestamp indicating when the deletion occurred. Physical files  
    /// are deleted lazily after a user-defined expiration time, allowing concurrent readers to  
    /// access stale snapshots. A remove action remains as a tombstone in a checkpoint file until
    /// it expires, which happens when the deletion timestamp is less than or equal to the
    /// minimum file retention timestamp.
    ///
    /// Note: When remove.deletion_timestamp is not present (defaulting to 0), the remove action
    /// will be excluded from the checkpoint file as it will be treated as expired.
    fn is_expired_tombstone<'a>(&self, i: usize, getter: &'a dyn GetData<'a>) -> DeltaResult<bool> {
        // Ideally this should never be zero, but we are following the same behavior as Delta
        // Spark and the Java Kernel.
        // Note: When remove.deletion_timestamp is not present (defaulting to 0), the remove action
        // will be excluded from the checkpoint file as it will be treated as expired.
        let mut deletion_timestamp: i64 = 0;
        if let Some(ts) = getter.get_opt(i, "remove.deletionTimestamp")? {
            deletion_timestamp = ts;
        }

        Ok(deletion_timestamp <= self.minimum_file_retention_timestamp)
    }

    /// Processes a potential file action to determine if it should be included in the checkpoint.
    ///
    /// Returns Some(Ok(())) if the row contains a valid file action to be included in the checkpoint.
    /// Returns None if the row doesn't contain a file action or should be skipped.
    /// Returns Some(Err(...)) if there was an error processing the action.
    ///
    /// Note: This function handles both add and remove actions, applying deduplication logic and
    /// tombstone expiration rules as needed.
    fn check_file_action<'a>(
        &mut self,
        i: usize,
        getters: &[&'a dyn GetData<'a>],
    ) -> Option<DeltaResult<()>> {
        // Extract the file action and handle errors immediately
        let (file_key, is_add) = match self.deduplicator.extract_file_action(i, getters, false) {
            Ok(Some(action)) => action,
            Ok(None) => return None, // If no file action is found, skip this row
            Err(e) => return Some(Err(e)),
        };

        // Check if we've already seen this file action
        if self.deduplicator.check_and_record_seen(file_key) {
            return None; // Skip duplicates
        }

        // For remove actions, check if it's an expired tombstone
        if !is_add {
            match self.is_expired_tombstone(i, getters[5]) {
                Ok(true) => return None,       // Skip expired tombstones
                Ok(false) => {}                // Not expired, continue
                Err(e) => return Some(Err(e)), // Error checking expiration
            }
        }

        // Valid, non-duplicate file action
        if is_add {
            self.total_add_actions += 1;
        }
        self.total_file_actions += 1;
        Some(Ok(())) // Include this action
    }

    /// Processes a potential protocol action to determine if it should be included in the checkpoint.
    ///
    /// Returns Some(Ok(())) if the row contains a valid protocol action.
    /// Returns None if the row doesn't contain a protocol action or is a duplicate.
    /// Returns Some(Err(...)) if there was an error processing the action.
    fn check_protocol_action<'a>(
        &mut self,
        i: usize,
        getter: &'a dyn GetData<'a>,
    ) -> Option<DeltaResult<()>> {
        // minReaderVersion is a required field, so we check for its presence to determine if this is a protocol action.
        match getter.get_int(i, "protocol.minReaderVersion") {
            Ok(Some(_)) => (),       // It is a protocol action
            Ok(None) => return None, // Not a protocol action
            Err(e) => return Some(Err(e)),
        };

        // Skip duplicates
        if self.seen_protocol {
            return None;
        }

        // Valid, non-duplicate protocol action to be included
        self.seen_protocol = true;
        self.total_non_file_actions += 1;
        Some(Ok(()))
    }
    /// Processes a potential metadata action to determine if it should be included in the checkpoint.
    ///
    /// Returns Some(Ok(())) if the row contains a valid metadata action.
    /// Returns None if the row doesn't contain a metadata action or is a duplicate.
    /// Returns Some(Err(...)) if there was an error processing the action.
    fn check_metadata_action<'a>(
        &mut self,
        i: usize,
        getter: &'a dyn GetData<'a>,
    ) -> Option<DeltaResult<()>> {
        // id is a required field, so we check for its presence to determine if this is a metadata action.
        match getter.get_str(i, "metaData.id") {
            Ok(Some(_)) => (),       // It is a metadata action
            Ok(None) => return None, // Not a metadata action
            Err(e) => return Some(Err(e)),
        };

        // Skip duplicates
        if self.seen_metadata {
            return None;
        }

        // Valid, non-duplicate metadata action to be included
        self.seen_metadata = true;
        self.total_non_file_actions += 1;
        Some(Ok(()))
    }
    /// Processes a potential txn action to determine if it should be included in the checkpoint.
    ///
    /// Returns Some(Ok(())) if the row contains a valid txn action.
    /// Returns None if the row doesn't contain a txn action or is a duplicate.
    /// Returns Some(Err(...)) if there was an error processing the action.
    fn check_txn_action<'a>(
        &mut self,
        i: usize,
        getter: &'a dyn GetData<'a>,
    ) -> Option<DeltaResult<()>> {
        // Check for txn field
        let app_id = match getter.get_str(i, "txn.appId") {
            Ok(Some(id)) => id,
            Ok(None) => return None, // Not a txn action
            Err(e) => return Some(Err(e)),
        };

        // If the app ID already exists in the set, the insertion will return false,
        // indicating that this is a duplicate.
        if !self.seen_txns.insert(app_id.to_string()) {
            return None;
        }

        // Valid, non-duplicate txn action to be included
        self.total_non_file_actions += 1;
        Some(Ok(()))
    }

    /// Determines if a row in the batch should be included in the checkpoint by checking
    /// if it contains any valid action type for the checkpoint.
    ///
    /// Note: This method checks each action type in sequence, and prioritizes file actions as
    /// they appear most frequently, followed by transaction, protocol, and metadata actions.
    pub(crate) fn is_valid_action<'a>(
        &mut self,
        i: usize,
        getters: &[&'a dyn GetData<'a>],
    ) -> DeltaResult<bool> {
        // Try each action type in sequence, stopping at the first match.
        let is_valid = self
            .check_file_action(i, getters)
            .or_else(|| self.check_txn_action(i, getters[11]))
            .or_else(|| self.check_protocol_action(i, getters[10]))
            .or_else(|| self.check_metadata_action(i, getters[9]))
            .transpose()? // Swap the Result outside and return if Err
            .is_some(); // If we got Some(Ok(())), it's a valid action

        Ok(is_valid)
    }
}

impl RowVisitor for CheckpointVisitor<'_> {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        // The data columns visited must be in the following order:
        // 1. ADD
        // 2. REMOVE
        // 3. METADATA
        // 4. PROTOCOL
        // 5. TXN
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
            const STRING: DataType = DataType::STRING;
            const INTEGER: DataType = DataType::INTEGER;
            let types_and_names = vec![
                // File action columns
                (STRING, column_name!("add.path")),
                (STRING, column_name!("add.deletionVector.storageType")),
                (STRING, column_name!("add.deletionVector.pathOrInlineDv")),
                (INTEGER, column_name!("add.deletionVector.offset")),
                (STRING, column_name!("remove.path")),
                (DataType::LONG, column_name!("remove.deletionTimestamp")),
                (STRING, column_name!("remove.deletionVector.storageType")),
                (STRING, column_name!("remove.deletionVector.pathOrInlineDv")),
                (INTEGER, column_name!("remove.deletionVector.offset")),
                // Non-file action columns
                (STRING, column_name!("metaData.id")),
                (INTEGER, column_name!("protocol.minReaderVersion")),
                (STRING, column_name!("txn.appId")),
            ];
            let (types, names) = types_and_names.into_iter().unzip();
            (names, types).into()
        });
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        require!(
            getters.len() == 12,
            Error::InternalError(format!(
                "Wrong number of visitor getters: {}",
                getters.len()
            ))
        );

        for i in 0..row_count {
            if self.selection_vector[i] {
                self.selection_vector[i] = self.is_valid_action(i, getters)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::array::StringArray;
    use crate::utils::test_utils::{action_batch, parse_json_batch};
    use itertools::Itertools;
    use std::collections::HashSet;

    #[test]
    fn test_checkpoint_visitor() -> DeltaResult<()> {
        let data = action_batch();
        let mut seen_file_keys = HashSet::new();
        let mut seen_txns = HashSet::new();
        let mut visitor = CheckpointVisitor::new(
            &mut seen_file_keys,
            true,
            vec![true; 8],
            0, // minimum_file_retention_timestamp (no expired tombstones)
            false,
            false,
            &mut seen_txns,
        );

        visitor.visit_rows_of(data.as_ref())?;

        // Row 0 is an add action (included)
        // Row 1 is a remove action (included)
        // Row 2 is a commit info action (excluded)
        // Row 3 is a protocol action (included)
        // Row 4 is a metadata action (included)
        // Row 5 is a cdc action (excluded)
        // Row 6 is a sidecar action (excluded)
        // Row 7 is a txn action (included)
        let expected = vec![true, true, false, true, true, false, false, true];

        assert_eq!(visitor.total_file_actions, 2);
        assert_eq!(visitor.total_add_actions, 1);
        assert!(visitor.seen_protocol);
        assert!(visitor.seen_metadata);
        assert_eq!(visitor.seen_txns.len(), 1);
        assert_eq!(visitor.total_non_file_actions, 3);

        assert_eq!(visitor.selection_vector, expected);
        Ok(())
    }

    /// Tests the boundary conditions for tombstone expiration logic.
    /// Specifically checks:
    /// - Remove actions with deletionTimestamp == minimumFileRetentionTimestamp (should be excluded)
    /// - Remove actions with deletionTimestamp < minimumFileRetentionTimestamp (should be excluded)
    /// - Remove actions with deletionTimestamp > minimumFileRetentionTimestamp (should be included)
    /// - Remove actions with missing deletionTimestamp (defaults to 0, should be excluded)
    #[test]
    fn test_checkpoint_visitor_boundary_cases_for_tombstone_expiration() -> DeltaResult<()> {
        let json_strings: StringArray = vec![
            r#"{"remove":{"path":"exactly_at_threshold","deletionTimestamp":100,"dataChange":true,"partitionValues":{}}}"#,
            r#"{"remove":{"path":"one_below_threshold","deletionTimestamp":99,"dataChange":true,"partitionValues":{}}}"#,
            r#"{"remove":{"path":"one_above_threshold","deletionTimestamp":101,"dataChange":true,"partitionValues":{}}}"#,
            // Missing timestamp defaults to 0
            r#"{"remove":{"path":"missing_timestamp","dataChange":true,"partitionValues":{}}}"#, 
        ]
        .into();
        let batch = parse_json_batch(json_strings);

        let mut seen_file_keys = HashSet::new();
        let mut seen_txns = HashSet::new();
        let mut visitor = CheckpointVisitor::new(
            &mut seen_file_keys,
            true,
            vec![true; 4],
            100, // minimum_file_retention_timestamp (threshold set to 100)
            false,
            false,
            &mut seen_txns,
        );

        visitor.visit_rows_of(batch.as_ref())?;

        // Only "one_above_threshold" should be kept
        let expected = vec![false, false, true, false];
        assert_eq!(visitor.selection_vector, expected);
        assert_eq!(visitor.total_file_actions, 1);
        assert_eq!(visitor.total_add_actions, 0);
        assert_eq!(visitor.total_non_file_actions, 0);
        Ok(())
    }

    #[test]
    fn test_checkpoint_visitor_conflicting_file_actions_in_log_batch() -> DeltaResult<()> {
        let json_strings: StringArray = vec![
            r#"{"add":{"path":"file1","partitionValues":{"c1":"6","c2":"a"},"size":452,"modificationTime":1670892998137,"dataChange":true}}"#,
             // Duplicate path
            r#"{"remove":{"path":"file1","deletionTimestamp":100,"dataChange":true,"partitionValues":{}}}"#,
        ]
        .into();
        let batch = parse_json_batch(json_strings);

        let mut seen_file_keys = HashSet::new();
        let mut seen_txns = HashSet::new();
        let mut visitor = CheckpointVisitor::new(
            &mut seen_file_keys,
            true,
            vec![true; 2],
            0,
            false,
            false,
            &mut seen_txns,
        );

        visitor.visit_rows_of(batch.as_ref())?;

        // First file action should be included. The second one should be excluded due to the conflict.
        let expected = vec![true, false];
        assert_eq!(visitor.selection_vector, expected);
        assert_eq!(visitor.total_file_actions, 1);
        assert_eq!(visitor.total_add_actions, 1);
        assert_eq!(visitor.total_non_file_actions, 0);
        Ok(())
    }

    #[test]
    fn test_checkpoint_visitor_file_actions_in_checkpoint_batch() -> DeltaResult<()> {
        let json_strings: StringArray = vec![
            r#"{"add":{"path":"file1","partitionValues":{"c1":"6","c2":"a"},"size":452,"modificationTime":1670892998137,"dataChange":true}}"#,
        ]
        .into();
        let batch = parse_json_batch(json_strings);

        let mut seen_file_keys = HashSet::new();
        let mut seen_txns = HashSet::new();
        let mut visitor = CheckpointVisitor::new(
            &mut seen_file_keys,
            false, // is_log_batch = false (checkpoint batch)
            vec![true; 1],
            0,
            false,
            false,
            &mut seen_txns,
        );

        visitor.visit_rows_of(batch.as_ref())?;

        let expected = vec![true];
        assert_eq!(visitor.selection_vector, expected);
        assert_eq!(visitor.total_file_actions, 1);
        assert_eq!(visitor.total_add_actions, 1);
        assert_eq!(visitor.total_non_file_actions, 0);
        // The action should NOT be added to the seen_file_keys set as it's a checkpoint batch
        // and actions in checkpoint batches do not conflict with each other.
        // This is a key difference from log batches, where actions can conflict.
        assert!(seen_file_keys.is_empty());
        Ok(())
    }

    #[test]
    fn test_checkpoint_visitor_conflicts_with_deletion_vectors() -> DeltaResult<()> {
        let json_strings: StringArray = vec![
            r#"{"add":{"path":"file1","partitionValues":{},"size":635,"modificationTime":100,"dataChange":true,"deletionVector":{"storageType":"one","pathOrInlineDv":"vBn[lx{q8@P<9BNH/isA","offset":1,"sizeInBytes":36,"cardinality":2}}}"#,
            // Same path but different DV, should be included
            r#"{"add":{"path":"file1","partitionValues":{},"size":635,"modificationTime":100,"dataChange":true,"deletionVector":{"storageType":"two","pathOrInlineDv":"vBn[lx{q8@P<9BNH/isA","offset":1,"sizeInBytes":36,"cardinality":2}}}"#, 
            // Duplicate of first entry, should be excluded
            r#"{"add":{"path":"file1","partitionValues":{},"size":635,"modificationTime":100,"dataChange":true,"deletionVector":{"storageType":"one","pathOrInlineDv":"vBn[lx{q8@P<9BNH/isA","offset":1,"sizeInBytes":36,"cardinality":2}}}"#, 
            // Conflicting remove action with DV, should be excluded
            r#"{"remove":{"path":"file1","deletionTimestamp":100,"dataChange":true,"deletionVector":{"storageType":"one","pathOrInlineDv":"vBn[lx{q8@P<9BNH/isA","offset":1,"sizeInBytes":36,"cardinality":2}}}"#,
        ]
        .into();
        let batch = parse_json_batch(json_strings);

        let mut seen_file_keys = HashSet::new();
        let mut seen_txns = HashSet::new();
        let mut visitor = CheckpointVisitor::new(
            &mut seen_file_keys,
            true,
            vec![true; 4],
            0,
            false,
            false,
            &mut seen_txns,
        );

        visitor.visit_rows_of(batch.as_ref())?;

        // Only the first two should be included since they have different (path, DvID) keys
        let expected = vec![true, true, false, false];
        assert_eq!(visitor.selection_vector, expected);
        assert_eq!(visitor.total_file_actions, 2);
        assert_eq!(visitor.total_add_actions, 2);
        assert_eq!(visitor.total_non_file_actions, 0);

        Ok(())
    }

    #[test]
    fn test_checkpoint_visitor_already_seen_non_file_actions() -> DeltaResult<()> {
        let json_strings: StringArray = vec![
            r#"{"txn":{"appId":"app1","version":1,"lastUpdated":123456789}}"#,
            r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["deletionVectors"],"writerFeatures":["deletionVectors"]}}"#,
            r#"{"metaData":{"id":"testId","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1677811175819}}"#,
        ].into();
        let batch = parse_json_batch(json_strings);

        // Pre-populate with txn app1
        let mut seen_file_keys = HashSet::new();
        let mut seen_txns = HashSet::new();
        seen_txns.insert("app1".to_string());

        let mut visitor = CheckpointVisitor::new(
            &mut seen_file_keys,
            true,
            vec![true; 3],
            0,
            true,           // The visior has already seen a protocol action
            true,           // The visitor has already seen a metadata action
            &mut seen_txns, // Pre-populated transaction
        );

        visitor.visit_rows_of(batch.as_ref())?;

        // All actions should be skipped as they have already been seen
        let expected = vec![false, false, false];
        assert_eq!(visitor.selection_vector, expected);
        assert_eq!(visitor.total_non_file_actions, 0);
        assert_eq!(visitor.total_file_actions, 0);

        Ok(())
    }

    #[test]
    fn test_checkpoint_visitor_duplicate_non_file_actions() -> DeltaResult<()> {
        let json_strings: StringArray = vec![
            r#"{"txn":{"appId":"app1","version":1,"lastUpdated":123456789}}"#,
            r#"{"txn":{"appId":"app1","version":1,"lastUpdated":123456789}}"#, // Duplicate txn
            r#"{"txn":{"appId":"app2","version":1,"lastUpdated":123456789}}"#, // Different app ID
            r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7}}"#,
            r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7}}"#, // Duplicate protocol
            r#"{"metaData":{"id":"testId","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1677811175819}}"#,
            // Duplicate metadata
            r#"{"metaData":{"id":"testId","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1677811175819}}"#, 
        ]
        .into();
        let batch = parse_json_batch(json_strings);

        let mut seen_file_keys = HashSet::new();
        let mut seen_txns = HashSet::new();
        let mut visitor = CheckpointVisitor::new(
            &mut seen_file_keys,
            true, // is_log_batch
            vec![true; 7],
            0, // minimum_file_retention_timestamp
            false,
            false,
            &mut seen_txns,
        );

        visitor.visit_rows_of(batch.as_ref())?;

        // First occurrence of each type should be included
        let expected = vec![true, false, true, true, false, true, false];
        assert_eq!(visitor.selection_vector, expected);
        assert_eq!(visitor.seen_txns.len(), 2); // Two different app IDs
        assert_eq!(visitor.total_non_file_actions, 4); // 2 txns + 1 protocol + 1 metadata
        assert_eq!(visitor.total_file_actions, 0);

        Ok(())
    }

    /// Tests the end-to-end processing of multiple batches with various action types.
    /// This tests the integration of the visitors with the main iterator function.
    /// More granular testing is performed in the visitor tests.
    #[test]
    fn test_v1_checkpoint_actions_iter_multi_batch_test() -> DeltaResult<()> {
        // Setup counters
        let total_actions_counter = Rc::new(RefCell::new(0));
        let total_add_actions_counter = Rc::new(RefCell::new(0));

        // Create first batch with protocol, metadata, and some files
        let json_strings1: StringArray = vec![
            r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#,
            r#"{"metaData":{"id":"test2","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"c1\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"c2\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"c3\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":["c1","c2"],"configuration":{},"createdTime":1670892997849}}"#,
            r#"{"add":{"path":"file1","partitionValues":{"c1":"4","c2":"c"},"size":452,"modificationTime":1670892998135,"dataChange":true,"stats":"{\"numRecords\":1,\"minValues\":{\"c3\":5},\"maxValues\":{\"c3\":5},\"nullCount\":{\"c3\":0}}"}}"#,
            r#"{"add":{"path":"file2","partitionValues":{"c1":"4","c2":"c"},"size":452,"modificationTime":1670892998135,"dataChange":true,"stats":"{\"numRecords\":1,\"minValues\":{\"c3\":5},\"maxValues\":{\"c3\":5},\"nullCount\":{\"c3\":0}}"}}"#,
        ].into();
        // Create second batch with some duplicates and new files
        let json_strings2: StringArray = vec![
            // Protocol and metadata should be skipped as duplicates
            r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#,
            r#"{"metaData":{"id":"test1","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"c1\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"c2\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"c3\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":["c1","c2"],"configuration":{},"createdTime":1670892997849}}"#,
            // New files
            r#"{"add":{"path":"file3","partitionValues":{},"size":800,"modificationTime":102,"dataChange":true}}"#,
            // Duplicate file should be skipped
            r#"{"add":{"path":"file1","partitionValues":{"c1":"4","c2":"c"},"size":452,"modificationTime":1670892998135,"dataChange":true,"stats":"{\"numRecords\":1,\"minValues\":{\"c3\":5},\"maxValues\":{\"c3\":5},\"nullCount\":{\"c3\":0}}"}}"#,            // Transaction
            r#"{"txn":{"appId":"app1","version":1,"lastUpdated":123456789}}"#
        ].into();
        // Create third batch with all duplicate actions.
        // The entire batch should be skippped as there are no selected actions to write from this batch.
        let json_strings3: StringArray = vec![
            r#"{"add":{"path":"file1","partitionValues":{"c1":"4","c2":"c"},"size":452,"modificationTime":1670892998135,"dataChange":true,"stats":"{\"numRecords\":1,\"minValues\":{\"c3\":5},\"maxValues\":{\"c3\":5},\"nullCount\":{\"c3\":0}}"}}"#,
            r#"{"add":{"path":"file2","partitionValues":{"c1":"4","c2":"c"},"size":452,"modificationTime":1670892998135,"dataChange":true,"stats":"{\"numRecords\":1,\"minValues\":{\"c3\":5},\"maxValues\":{\"c3\":5},\"nullCount\":{\"c3\":0}}"}}"#,
        ].into();
        let input_batches = vec![
            Ok((parse_json_batch(json_strings1), true)),
            Ok((parse_json_batch(json_strings2), true)),
            Ok((parse_json_batch(json_strings3), true)),
        ];

        let results: Vec<_> = checkpoint_actions_iter(
            input_batches.into_iter(),
            total_actions_counter.clone(),
            total_add_actions_counter.clone(),
            0,
        )
        .try_collect()?;

        // Expect two batches in results (third batch should be filtered out)"
        assert_eq!(results.len(), 2);

        // First batch should have all rows selected
        let checkpoint_data = &results[0];
        assert_eq!(
            checkpoint_data.selection_vector,
            vec![true, true, true, true]
        );

        // Second batch should have only new file and transaction selected
        let checkpoint_data = &results[1];
        assert_eq!(
            checkpoint_data.selection_vector,
            vec![false, false, true, false, true]
        );

        // 6 total actions (4 from batch1 + 2 from batch2 + 0 from batch3)
        assert_eq!(*total_actions_counter.borrow(), 6);
        // 3 add actions (2 from batch1 + 1 from batch2)
        assert_eq!(*total_add_actions_counter.borrow(), 3);

        Ok(())
    }
}
