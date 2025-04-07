//! # Delta Kernel Checkpoint API
//!
//! This module implements the API for writing single-file checkpoints in Delta tables.
//! Checkpoints provide a compact summary of the table state, enabling faster recovery by
//! avoiding full log replay.
//!
//! ## Checkpoint Types
//!
//! 1. **Single-file Classic-named V1 Checkpoint** – for legacy tables that do not support
//!    the v2Checkpoints feature.
//! 2. **Single-file Classic-named V2 Checkpoint** – for backwards compatibility when the
//!    v2Checkpoints feature is enabled.
//! 3. **Single-file UUID-named V2 Checkpoint** – the recommended option for small to medium
//!    tables with v2Checkpoints support.
//!
//! ## Architecture
//!
//! The API is designed using a builder pattern:
//!
//! 1. [`CheckpointBuilder`] performs table feature detection and constructs the writer by:
//!    - Configuring the writer with classic naming (optional)
//!    - Replaying Delta log actions to filter, deduplicate, and select actions
//! 2. [`CheckpointWriter`] is constructed from the builder and handles:
//!    - Returning consolidated checkpoint data for writing to the engine
//!    - Finalizing the checkpoint by generating a `_last_checkpoint` file with metadata
//!
//! ## Example
//!
//! ```ignore
//! let path = "./tests/data/app-txn-no-checkpoint";
//! let engine = Arc::new(SyncEngine::new());
//! let table = Table::try_from_uri(path)?;
//!
//! // Create a checkpoint builder for the table at a specific version
//! let builder = table.checkpoint(&engine, Some(2))?;
//!
//! // Optionally configure the builder (e.g., force classic naming)
//! let writer = builder.with_classic_naming(true);
//!
//! // Build the checkpoint writer
//! let mut writer = builder.build(&engine)?;
//!
//! // Retrieve checkpoint data (ensuring single consumption)
//! let checkpoint_data = writer.get_checkpoint_info()?;
//!
//! /* Write checkpoint data to file and collect metadata about the write */
//! /* The implementation of the write is storage-specific and not shown */
//! /* IMPORTANT: All data must be written before finalizing the checkpoint */
//!
//! // Finalize the checkpoint by writing the _last_checkpoint file
//! writer.finalize_checkpoint(&engine, &checkpoint_metadata)?;
//! ```
//!
//! This module, along with its submodule `checkpoint/log_replay.rs`, provides the full
//! API and implementation for generating checkpoints. See `checkpoint/log_replay.rs` for details
//! on how log replay is used to filter and deduplicate actions for checkpoint creation.

use log_replay::{checkpoint_actions_iter, CheckpointData};
use std::{
    sync::{atomic::AtomicI64, Arc, LazyLock},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use url::Url;

use crate::actions::schemas::GetStructField;
use crate::schema::{SchemaRef, StructType};
use crate::{
    actions::{
        Add, Metadata, Protocol, Remove, SetTransaction, Sidecar, ADD_NAME, METADATA_NAME,
        PROTOCOL_NAME, REMOVE_NAME, SET_TRANSACTION_NAME, SIDECAR_NAME,
    },
    snapshot::Snapshot,
    DeltaResult, Engine, EngineData, Error,
};

mod log_replay;

/// This schema contains all the actions that we care to extract from log
/// files for the purpose of creating a checkpoint.
static CHECKPOINT_READ_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    StructType::new([
        Option::<Add>::get_struct_field(ADD_NAME),
        Option::<Remove>::get_struct_field(REMOVE_NAME),
        Option::<Metadata>::get_struct_field(METADATA_NAME),
        Option::<Protocol>::get_struct_field(PROTOCOL_NAME),
        Option::<SetTransaction>::get_struct_field(SET_TRANSACTION_NAME),
        Option::<Sidecar>::get_struct_field(SIDECAR_NAME),
    ])
    .into()
});

/// This schema is used when reading actions from the Delta log
/// to ensure we capture all necessary action types.
#[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
#[cfg_attr(not(feature = "developer-visibility"), visibility::make(pub(crate)))]
fn get_checkpoint_read_schema() -> &'static SchemaRef {
    &CHECKPOINT_READ_SCHEMA
}

/// Contains the path and data for a single-file checkpoint.
///
/// This struct holds all the necessary information for writing a checkpoint file,
/// including the destination path and the iterator over checkpoint actions.
pub struct SingleFileCheckpointData {
    /// The target URL where the checkpoint file will be written
    pub path: Url,

    /// Iterator over checkpoint actions to be written to the file
    pub data: Box<dyn Iterator<Item = DeltaResult<CheckpointData>>>,
}

/// Writer for creating checkpoint files in Delta tables.
///
/// The CheckpointWriter orchestrates the process of writing checkpoint data to storage.
/// It manages the one-time consumption of checkpoint data and tracks statistics
/// about the actions included in the checkpoint.
pub struct CheckpointWriter {
    // The snapshot from which the checkpoint is created
    snapshot: Snapshot,
    // Flag indicating if the table supports the `v2Checkpoints` reader/write feature
    is_v2_checkpoints_supported: bool,

    // TODO, i dont think arc is necessary here
    total_actions_counter: Arc<AtomicI64>,
    total_add_actions_counter: Arc<AtomicI64>,
}

impl CheckpointWriter {
    /// Creates a new CheckpointWriter with the provided checkpoint data and counters
    fn new(snapshot: Snapshot, is_v2_checkpoints_supported: bool) -> Self {
        Self {
            snapshot,
            is_v2_checkpoints_supported,
            total_actions_counter: Arc::new(AtomicI64::new(0)),
            total_add_actions_counter: Arc::new(AtomicI64::new(0)),
        }
    }

    /// Retrieves the checkpoint data and path information
    ///
    /// This method takes ownership of the checkpoint data, ensuring it can
    /// only be consumed once. It returns an error if the data has already
    /// been consumed.
    pub fn get_checkpoint_info(
        &mut self,
        engine: &dyn Engine,
    ) -> DeltaResult<SingleFileCheckpointData> {
        // Create counters for tracking actions
        let total_actions_counter = Arc::new(AtomicI64::new(0));
        let total_add_actions_counter = Arc::new(AtomicI64::new(0));

        // Create iterator over actions for checkpoint data
        let checkpoint_data = checkpoint_actions_iter(
            self.replay_for_checkpoint_data(engine)?,
            total_actions_counter.clone(),
            total_add_actions_counter.clone(),
            self.deleted_file_retention_timestamp()?,
        );

        // Chain the result of create_checkpoint_metadata_batch to the checkpoint data
        let chained = checkpoint_data.chain(create_checkpoint_metadata_batch(
            self.snapshot.version() as i64,
            engine,
            self.is_v2_checkpoints_supported,
        )?);

        // Generate checkpoint path based on builder configuration
        // Classic naming is required for V1 checkpoints and optional for V2 checkpoints
        // let checkpoint_path = if self.with_classic_naming || !v2_checkpoints_supported {
        //     ParsedLogPath::new_classic_parquet_checkpoint(
        //         self.snapshot.table_root(),
        //         self.snapshot.version(),
        //     )?
        // } else {
        //     ParsedLogPath::new_uuid_parquet_checkpoint(
        //         self.snapshot.table_root(),
        //         self.snapshot.version(),
        //     )?
        // };

        // Create the checkpoint data object
        Ok(SingleFileCheckpointData {
            path: Url::parse("todo://checkpoint_path").unwrap(), // TODO: Replace with actual path
            data: Box::new(chained),
        })
    }

    /// Finalizes the checkpoint writing process
    ///
    /// This method should be only called AFTER writing all checkpoint data to
    /// ensure proper completion of the checkpoint operation. This method
    /// generates the `_last_checkpoint` file with metadata about the checkpoint.
    ///
    /// The metadata parameter is a single-row EngineData batch containing
    /// {size_in_bytes: i64} for the checkpoint file. This method will extend
    /// the EngineData batch with the remaining fields for the `_last_checkpoint`
    /// file.
    #[allow(dead_code)] // TODO: Remove when finalize_checkpoint is implemented
    fn finalize_checkpoint(
        self,
        _engine: &dyn Engine,
        _metadata: &dyn EngineData,
    ) -> DeltaResult<()> {
        todo!("Implement finalize_checkpoint");
    }

    /// Calculates the cutoff timestamp for deleted file cleanup.
    ///
    /// This function determines the minimum timestamp before which deleted files
    /// will be permanently removed during VACUUM operations, based on the table's
    /// deleted_file_retention_duration property.
    ///
    /// Returns the cutoff timestamp in milliseconds since epoch, matching
    /// the remove action's deletion_timestamp format for comparison.
    ///
    /// The default retention period is 7 days, matching delta-spark's behavior.
    fn deleted_file_retention_timestamp(&self) -> DeltaResult<i64> {
        let retention_duration = self
            .snapshot
            .table_properties()
            .deleted_file_retention_duration;

        deleted_file_retention_timestamp_with_time(
            retention_duration,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|e| Error::generic(format!("Failed to calculate system time: {}", e)))?,
        )
    }

    /// Prepares the iterator over actions for checkpoint creation
    ///
    /// This method is factored out to facilitate testing and returns an iterator
    /// over all actions to be included in the checkpoint.
    fn replay_for_checkpoint_data(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<(Box<dyn EngineData>, bool)>> + Send> {
        let read_schema = get_checkpoint_read_schema();
        self.snapshot.log_segment().read_actions(
            engine,
            read_schema.clone(),
            read_schema.clone(),
            None,
        )
    }
}

/// Builder for configuring and creating CheckpointWriter instances
///
/// The CheckpointBuilder provides an interface for configuring checkpoint
/// generation. It handles table feature detection and enforces compatibility
/// between configuration options and table features.
pub struct CheckpointBuilder {
    /// The table snapshot from which to create the checkpoint
    snapshot: Snapshot,

    /// Whether to use classic naming for the checkpoint file
    with_classic_naming: bool,
}

impl CheckpointBuilder {
    pub(crate) fn new(snapshot: Snapshot) -> Self {
        Self {
            snapshot,
            with_classic_naming: false,
        }
    }

    /// Configures the builder to use the classic naming scheme
    ///
    /// Classic naming is required for V1 checkpoints and optional for V2 checkpoints.
    /// - For V1 checkpoints, this method is a no-op.
    /// - For V2 checkpoints, the default is UUID naming unless this method is called.
    pub fn with_classic_naming(mut self, with_classic_naming: bool) -> Self {
        self.with_classic_naming = with_classic_naming;
        self
    }

    /// Builds a [`CheckpointWriter`] based on the builder configuration.
    ///
    /// This method validates the configuration against table features and creates
    /// a [`CheckpointWriter`] for the appropriate checkpoint type. It performs protocol
    /// table feature checks to determine if v2Checkpoints are supported.
    pub fn build(self) -> DeltaResult<CheckpointWriter> {
        let is_v2_checkpoints_supported = self
            .snapshot
            .table_configuration()
            .is_v2_checkpoint_supported();

        Ok(CheckpointWriter::new(
            self.snapshot,
            is_v2_checkpoints_supported,
        ))
    }
}

/// Internal implementation with injectable time parameter for testing
fn deleted_file_retention_timestamp_with_time(
    retention_duration: Option<Duration>,
    now_duration: Duration,
) -> DeltaResult<i64> {
    // Use provided retention duration or default (7 days)
    let retention_duration =
        retention_duration.unwrap_or_else(|| Duration::from_secs(60 * 60 * 24 * 7));

    // Convert to milliseconds for remove action deletion_timestamp comparison
    let now_ms: i64 = now_duration
        .as_millis()
        .try_into()
        .map_err(|_| Error::generic("Current timestamp exceeds i64 millisecond range"))?;

    let retention_ms: i64 = retention_duration
        .as_millis()
        .try_into()
        .map_err(|_| Error::generic("Retention duration exceeds i64 millisecond range"))?;

    // Simple subtraction - will produce negative values if retention > now
    Ok(now_ms - retention_ms)
}

/// Create a batch with a single row containing the [`CheckpointMetadata`] action
/// for the V2 spec checkpoint.
///
/// This method calls the create_one method on the expression handler to create
/// a single-row batch with the checkpoint metadata action. The method returns:
/// - None if the checkpoint is not a V2 checkpoint
/// - Some(Ok(batch)) if the batch was successfully created
fn create_checkpoint_metadata_batch(
    _version: i64,
    _engine: &dyn Engine,
    _is_v2_checkpoint: bool,
) -> DeltaResult<Option<DeltaResult<CheckpointData>>> {
    todo!("Implement create_checkpoint_metadata_batch");
    // if is_v2_checkpoint {
    //     let values: &[Scalar] = &[version.into()];
    //     let checkpoint_metadata_batch = engine.get_expression_handler().create_one(
    //         // TODO: Include checkpointMetadata.tags when maps are supported
    //         Arc::new(CheckpointMetadata::to_schema().project_as_struct(&["version"])?),
    //         &values,
    //     )?;

    //     let result = CheckpointData {
    //         data: checkpoint_metadata_batch,
    //         selection_vector: vec![true],
    //     };

    //     Ok(Some(Ok(result)))
    // } else {
    //     Ok(None)
    // }
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    use std::time::Duration;

    #[test]
    fn test_deleted_file_retention_timestamp() -> DeltaResult<()> {
        let now = Duration::from_secs(1000).as_millis() as i64;

        // Test cases
        let test_cases = [
            // Default case (7 days)
            (None, now - (7 * 24 * 60 * 60 * 1000)),
            // Zero retention
            (Some(Duration::from_secs(0)), now),
            // Custom retention (2000 seconds)
            // This results in a negative timestamp which is valid - as it just means that
            // the retention window extends to before UNIX epoch.
            (Some(Duration::from_secs(2000)), now - (2000 * 1000)),
        ];

        for (retention, expected) in test_cases {
            let result =
                deleted_file_retention_timestamp_with_time(retention, Duration::from_secs(1000))?;
            assert_eq!(result, expected);
        }

        Ok(())
    }
}
