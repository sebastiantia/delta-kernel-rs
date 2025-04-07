//! # Delta Kernel Checkpoint API
//!
//! This module implements the API for writing checkpoints in delta tables.
//! Checkpoints provide a compact summary of the table state, enabling faster recovery by
//! avoiding full log replay. This API supports three checkpoint types:
//!
//! 1. **Single-file Classic-named V1 Checkpoint** – for legacy tables that do not support
//!    the `v2Checkpoints` reader/writer feature.
//! 2. **Single-file Classic-named V2 Checkpoint** – ensures backwards compatibility by
//!    allowing legacy readers to recognize the checkpoint file, read the protocol action, and
//!    fail gracefully.
//! 3. **Single-file UUID-named V2 Checkpoint** – the default and preferred option for small to
//!    medium tables with `v2Checkpoints` reader/writer feature enabled.  
//!
//! ## Architecture
//!
//! ### [`CheckpointBuilder`]
//! The entry point for checkpoint creation with the following methods:
//! - `new(snapshot: Snapshot) -> Self` - Creates a new builder for the given table snapshot
//! - `with_classic_naming() -> Self` - Configures the builder to use classic naming
//! - `build() -> DeltaResult<CheckpointWriter>` - Creates the checkpoint writer
//!
//! ### [`CheckpointWriter`]
//! Handles the actual checkpoint generation with the following methods:
//! - `get_checkpoint_info(engine: &dyn Engine) -> DeltaResult<SingleFileCheckpointData>` -
//!   Retrieves checkpoint data and path
//! - `finalize_checkpoint(engine: &dyn Engine, metadata: &dyn EngineData) -> DeltaResult<()>` -
//!   Writes the _last_checkpoint file
//!
//! ## Checkpoint Type Selection
//!
//! The checkpoint type is determined by two factors:
//! 1. Whether the table supports the `v2Checkpoints` reader/writer feature
//! 2. Whether classic naming is configured on the builder
//!
//! ```text
//! +------------------+---------------------------+-------------------------------+
//! | Table Feature    | Builder Configuration     | Resulting Checkpoint Type     |
//! +==================+===========================+===============================+
//! | No v2Checkpoints | Any                       | Single-file Classic-named V1  |
//! +------------------+---------------------------+-------------------------------+
//! | v2Checkpoints    | with_classic_naming(false)| Single-file UUID-named V2     |
//! +------------------+---------------------------+-------------------------------+
//! | v2Checkpoints    | with_classic_naming(true) | Single-file Classic-named V2  |
//! +------------------+---------------------------+-------------------------------+
//! ```
//!
//! Notes:
//! - Multi-file V2 checkpoints are not supported yet. The API is designed to be extensible for future
//!   multi-file support, but the current implementation only supports single-file checkpoints.
//! - Multi-file V1 checkpoints are DEPRECATED.
//!
//! ## Example: Writing a classic-named V1/V2 checkpoint (depending on `v2Checkpoints` feature support)
//!
//! TODO(seb): unignore example
//! ```ignore
//! let path = "./tests/data/app-txn-no-checkpoint";
//! let engine = Arc::new(SyncEngine::new());
//! let table = Table::try_from_uri(path)?;
//!
//! // Create a checkpoint builder for the table at a specific version
//! let builder = table.checkpoint(&engine, Some(2))?;
//!
//! // Optionally configure the builder (e.g., force classic naming)
//! let writer = builder.with_classic_naming();
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
use crate::{
    actions::{
        schemas::{GetStructField, ToSchema},
        Add, CheckpointMetadata, Metadata, Protocol, Remove, SetTransaction, Sidecar, ADD_NAME,
        METADATA_NAME, PROTOCOL_NAME, REMOVE_NAME, SET_TRANSACTION_NAME, SIDECAR_NAME,
    },
    expressions::Scalar,
    schema::{SchemaRef, StructType},
    snapshot::Snapshot,
    DeltaResult, Engine, EngineData, Error, EvaluationHandlerExtension,
};
use log_replay::{checkpoint_actions_iter, CheckpointData};
use std::{
    cell::RefCell,
    rc::Rc,
    sync::{Arc, LazyLock},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use url::Url;

mod log_replay;

/// Schema for extracting relevant actions from log files during checkpoint creation
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

/// Returns the schema for reading Delta log actions during checkpoint creation
fn get_checkpoint_read_schema() -> &'static SchemaRef {
    &CHECKPOINT_READ_SCHEMA
}

/// Contains the path and data for a single-file checkpoint
pub struct SingleFileCheckpointData {
    /// Target URL where the checkpoint file will be written
    pub path: Url,

    /// Iterator over checkpoint actions to be written to the file
    pub data: Box<dyn Iterator<Item = DeltaResult<CheckpointData>>>,
}

/// Manages the checkpoint writing process for Delta tables
///
/// The [`CheckpointWriter`] orchestrates creating checkpoint data and finalizing
/// the checkpoint file. It tracks statistics about included actions and
/// ensures checkpoint data is consumed only once.
///
/// # Usage Flow
/// 1. Create via `CheckpointBuilder::build()`
/// 2. Call `get_checkpoint_info()` to obtain the [`SingleFileCheckpointData`]
///    containing the path and action iterator for the checkpoint
/// 3. Write the checkpoint data to storage (implementation-specific)
/// 4. Call `finalize_checkpoint()` to create the _last_checkpoint file
///
/// # Internal Process
/// 1. Reads relevant actions from the log segment using the checkpoint read schema
/// 2. Applies selection and deduplication logic with the `CheckpointLogReplayProcessor`
/// 3. Tracks counts of included actions for to be written to the _last_checkpoint file
/// 5. Chains the [`CheckpointMetadata`] action to the actions iterator (for V2 checkpoints)
pub struct CheckpointWriter {
    /// The snapshot from which the checkpoint is created
    snapshot: Snapshot,
    /// Whether the table supports the `v2Checkpoints` feature
    is_v2_checkpoints_supported: bool,
    /// Note: Rc<RefCell<i64>> provides shared mutability for our counters, allowing the
    /// returned actions iterator from `.get_checkpoint_info()` to update the counters,
    /// and the `finalize_checkpoint()` method to read them...
    /// Counter for total actions included in the checkpoint
    total_actions_counter: Rc<RefCell<i64>>,
    /// Counter for Add actions included in the checkpoint
    total_add_actions_counter: Rc<RefCell<i64>>,
    /// Flag to track if checkpoint data has been consumed
    data_consumed: bool,
}

impl CheckpointWriter {
    /// Creates a new CheckpointWriter with the provided checkpoint data and counters
    fn new(snapshot: Snapshot, is_v2_checkpoints_supported: bool) -> Self {
        Self {
            snapshot,
            is_v2_checkpoints_supported,
            total_actions_counter: Rc::new(RefCell::<i64>::new(0.into())),
            total_add_actions_counter: Rc::new(RefCell::<i64>::new(0.into())),
            data_consumed: false,
        }
    }

    /// Retrieves the checkpoint data and path information
    ///
    /// This method is the core of the checkpoint generation process. It:
    ///
    /// 1. Ensures checkpoint data is consumed only once via `data_consumed` flag
    /// 2. Reads actions from the log segment using the checkpoint schema
    /// 3. Filters and deduplicates actions for the checkpoint
    /// 4. Chains the checkpoint metadata action if writing a V2 spec checkpoint
    ///    (i.e., if `v2Checkpoints` feature is supported by table)
    /// 5. Generates the appropriate checkpoint path
    ///
    /// The returned data should be written to persistent storage by the caller
    /// before calling `finalize_checkpoint()` otherwise data loss may occur.
    ///
    /// # Returns
    /// A [`SingleFileCheckpointData`] containing the checkpoint path and action iterator
    pub fn get_checkpoint_info(
        &mut self,
        engine: &dyn Engine,
    ) -> DeltaResult<SingleFileCheckpointData> {
        if self.data_consumed {
            return Err(Error::generic("Checkpoint data has already been consumed"));
        }

        // Create iterator over actions for checkpoint data
        let checkpoint_data = checkpoint_actions_iter(
            self.replay_for_checkpoint_data(engine)?,
            self.total_actions_counter.clone(),
            self.total_add_actions_counter.clone(),
            self.deleted_file_retention_timestamp()?,
        );

        // Chain the checkpoint metadata action if using V2 checkpoints
        let chained = checkpoint_data.chain(create_checkpoint_metadata_batch(
            self.snapshot.version() as i64,
            engine,
            self.is_v2_checkpoints_supported,
        )?);

        // Generate the appropriate checkpoint path
        // let checkpoint_path = self.generate_checkpoint_path()?;

        self.data_consumed = true;

        Ok(SingleFileCheckpointData {
            path: Url::parse("todo!")?,
            data: Box::new(chained),
        })
    }

    /// Finalizes the checkpoint writing process by creating the _last_checkpoint file
    ///
    /// The `LastCheckpointInfo` (`_last_checkpoint`) file is a metadata file that contains
    /// information about the last checkpoint created for the table. It is used as a hint
    /// for the engine to quickly locate the last checkpoint and avoid full log replay when
    /// reading the table.
    ///
    /// # Workflow
    /// 0. IMPORTANT: This method must only be called AFTER successfully writing
    ///    all checkpoint data to storage. Failure to do so may result in
    ///    data loss.
    /// 1. Extracts size information from the provided metadata
    /// 2. Combines with additional metadata collected during checkpoint creation
    /// 3. Writes the _last_checkpoint file to the log
    ///
    /// # Parameters
    /// - `engine`: The engine used for writing the _last_checkpoint file
    /// - `metadata`: A single-row [`EngineData`] batch containing:
    ///   - `size_in_bytes` (i64): The size of the written checkpoint file
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

    /// Retrieves an iterator over all actions to be included in the checkpoint
    ///
    /// This method reads the relevant actions from the table's log segment using
    /// the checkpoint schema, which filters for action types needed in checkpoints.
    ///
    /// The returned iterator yields tuples where:
    /// - The first element is data in engine format
    /// - The second element is a flag that indicates the action's source:
    ///   - `true` if the action came from a commit file
    ///   - `false` if the action came from a previous checkpoint file
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
///
/// # Usage Flow
/// 1. Create a builder via `Table::checkpoint()`
/// 2. Optionally configure with `with_classic_naming()`
/// 3. Call `build()` to create a CheckpointWriter
///
/// # Checkpoint Format Selection Logic
/// - For tables without v2Checkpoints support: Always uses Single-file Classic-named V1
/// - For tables with v2Checkpoints support:
///   - With classic naming = false (default): Single-file UUID-named V2
///   - With classic naming = true: Single-file Classic-named V2
///
/// # Checkpoint Naming Conventions
///
/// ## UUID-named V2 Checkpoints
/// These follow the V2 spec using file name pattern: `n.checkpoint.u.{json/parquet}`, where:
/// - `n` is the snapshot version (zero-padded to 20 digits)
/// - `u` is a UUID
/// e.g. 00000000000000000010.checkpoint.80a083e8-7026-4e79-81be-64bd76c43a11.json
///
/// ## Classic-named Checkpoints
/// A classic checkpoint for version `n` of the table consists of a file named
/// `n.checkpoint.parquet` where `n` is zero-padded to have length 20. These could
/// follow either V1 spec or V2 spec depending on the table's support for the
/// `v2Checkpoints` feature.
/// e.g. 00000000000000000010.checkpoint.parquet
///
/// # Example
/// ```ignore
/// let table = Table::try_from_uri(path)?;
/// let builder = table.checkpoint(&engine, Some(version))?;
/// let writer = builder.with_classic_naming().build()?;
/// ```
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
    /// Classic naming is optional for V2 checkpoints, but the only option for V1 checkpoints.
    /// - For V1 checkpoints, this method is a no-op.
    /// - For V2 checkpoints, the default is UUID-naming unless this method is called.
    pub fn with_classic_naming(mut self) -> Self {
        self.with_classic_naming = true;
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
/// Creates the checkpoint metadata action for V2 checkpoints.
///
/// For V2 checkpoints, this function generates a special [`CheckpointMetadata`] action
/// that must be included in the V2 spec checkpoint file. This action contains metadata
/// about the checkpoint, particularly its version. For V1 checkpoints, this function
/// returns `None`, as the V1 schema does not include this action type.
///
/// # Implementation Details
///
/// The function creates a single-row [`EngineData`] batch containing only the
/// version field of the [`CheckpointMetadata`] action. Future implementations will
/// include additional metadata fields such as tags when map support is added.
///
/// The resulting [`CheckpointData`] includes a selection vector with a single `true`
/// value, indicating this action should always be included in the checkpoint.
fn create_checkpoint_metadata_batch(
    version: i64,
    engine: &dyn Engine,
    is_v2_checkpoint: bool,
) -> DeltaResult<Option<DeltaResult<CheckpointData>>> {
    if is_v2_checkpoint {
        let values: &[Scalar] = &[version.into()];
        let checkpoint_metadata_batch = engine.evaluation_handler().create_one(
            // TODO: Include checkpointMetadata.tags when maps are supported
            Arc::new(CheckpointMetadata::to_schema().project_as_struct(&["version"])?),
            &values,
        )?;

        let result = CheckpointData {
            data: checkpoint_metadata_batch,
            selection_vector: vec![true],
        };

        Ok(Some(Ok(result)))
    } else {
        Ok(None)
    }
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
