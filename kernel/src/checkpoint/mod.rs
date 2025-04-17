//! # Delta Kernel Checkpoint API
//!
//! This module implements the API for writing checkpoints in delta tables.
//! Checkpoints allow readers to short-cut the cost of reading the entire log history by providing
//! the complete replay of all actions, up to and including the checkpointed version, with invalid
//! actions removed. Invalid actions are those that have been canceled out by subsequent ones (for
//! example removing a file that has been added), using the rules for reconciliation.
//!
//! ## Checkpoint Types
//! This API supports two checkpoint types:
//!
//! 1. **Single-file Classic-named V1 Checkpoint** – for legacy tables that do not support the
//!    `v2Checkpoints` reader/writer feature. These checkpoints follow the V1 specification and do not
//!    include a [`CheckpointMetadata`] action.
//! 2. **Single-file Classic-named V2 Checkpoint** – for tables supporting the `v2Checkpoints` feature.
//!    These checkpoints follow the V2 specification and include a [`CheckpointMetadata`] action, while
//!    maintaining backwards compatibility by using classic-naming that legacy readers can recognize.
//!
//! For more information on the V1/V2 specifications, see the following protocol section:
//! <https://github.com/delta-io/delta/blob/master/PROTOCOL.md#checkpoint-specs>
//!
//! ## Checkpoint Selection Logic
//! The checkpoint type is determined by whether the table supports the `v2Checkpoints` reader/writer feature:
//!
//! | Table Feature    | Resulting Checkpoint Type     |
//! |------------------|-------------------------------|
//! | No v2Checkpoints | Single-file Classic-named V1  |
//! | v2Checkpoints    | Single-file Classic-named V2  |
//!
//! ## Architecture
//!
//! - [`CheckpointWriter`] - Core component that manages the checkpoint creation workflow
//! - [`CheckpointData`] - Contains the data to write and destination path information
//!
//! ## Usage Workflow
//!
//! 1. Create a [`CheckpointWriter`] using [`crate::table::Table::checkpoint`]
//! 2. Get checkpoint data and path with [`CheckpointWriter::checkpoint_data`]
//! 3. Write all data to the returned location
//! 4. TODO(#850) Finalize the checkpoint with `CheckpointWriter::finalize`

//!
//! ## Example: Writing a classic-named V1 checkpoint (no `v2Checkpoints` feature on test table)
//!
//! ```
//! use std::sync::Arc;
//! use delta_kernel::{
//!     checkpoint::CheckpointData,
//!     engine::arrow_data::ArrowEngineData,
//!     engine::default::{executor::tokio::TokioBackgroundExecutor, DefaultEngine},
//!     table::Table,
//!     DeltaResult, Error,
//! };
//! use delta_kernel::arrow::array::{Int64Array, RecordBatch};
//! use delta_kernel::arrow::datatypes::{DataType, Field, Schema};
//! use object_store::local::LocalFileSystem;
//!
//! fn mock_write_to_object_store(mut data: CheckpointData) -> DeltaResult<ArrowEngineData> {
//!     /* This should be replaced with actual object store write logic */
//!     /* For demonstration, we manually create an EngineData batch with a dummy size */
//!     let size = data.data.try_fold(0i64, |acc, r| r.map(|_| acc + 1))?;
//!     let batch = RecordBatch::try_new(
//!         Arc::new(Schema::new(vec![Field::new("sizeInBytes", DataType::Int64, false)])),
//!         vec![Arc::new(Int64Array::from(vec![size]))],
//!     )?;
//!     Ok(ArrowEngineData::new(batch))
//! }
//!
//! let engine = DefaultEngine::new(
//!     Arc::new(LocalFileSystem::new()),
//!     Arc::new(TokioBackgroundExecutor::new())
//! );
//! let table = Table::try_from_uri("./tests/data/app-txn-no-checkpoint")?;
//!
//! // Create a checkpoint writer for the table at a specific version
//! let mut writer = table.checkpoint(&engine, Some(1))?;
//!
//! // Write the checkpoint data to the object store and get the metadata
//! let metadata = mock_write_to_object_store(writer.checkpoint_data(&engine)?)?;
//!
//! /* IMPORTANT: All data must be written before finalizing the checkpoint */
//!
//! // TODO(#850): Implement the finalize method
//! // Finalize the checkpoint. This call will write the _last_checkpoint file
//! // writer.finalize(&engine, &metadata)?;
//!
//! # Ok::<_, Error>(())
//! ```
//!
//! ## Future extensions
//! - TODO(#836): Single-file UUID-named V2 checkpoints (using `n.checkpoint.u.{json/parquet}` naming) are to be
//!   implemented in the future. The current implementation only supports classic-named V2 checkpoints.
//! - TODO(#837): Multi-file V2 checkpoints are not supported yet. The API is designed to be extensible for future
//!   multi-file support, but the current implementation only supports single-file checkpoints.
//!
//! Note: Multi-file V1 checkpoints are DEPRECATED and UNSAFE.
//!
//! [`CheckpointMetadata`]: crate::actions::CheckpointMetadata
//! [`LastCheckpointHint`]: crate::snapshot::LastCheckpointHint
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::actions::CHECKPOINT_METADATA_NAME;
use crate::actions::{
    schemas::GetStructField, Add, Metadata, Protocol, Remove, SetTransaction, Sidecar, ADD_NAME,
    METADATA_NAME, PROTOCOL_NAME, REMOVE_NAME, SET_TRANSACTION_NAME, SIDECAR_NAME,
};
use crate::engine_data::FilteredEngineData;
use crate::expressions::Scalar;
use crate::log_replay::LogReplayProcessor;
use crate::path::ParsedLogPath;
use crate::schema::{DataType, SchemaRef, StructField, StructType};
use crate::snapshot::Snapshot;
use crate::{DeltaResult, Engine, EngineData, Error, EvaluationHandlerExtension};
use log_replay::CheckpointLogReplayProcessor;

use url::Url;

mod log_replay;
#[cfg(test)]
mod tests;

const SECONDS_PER_MINUTE: u64 = 60;
const MINUTES_PER_HOUR: u64 = 60;
const HOURS_PER_DAY: u64 = 24;
const DAYS: u64 = 7;
/// The default retention period for deleted files in seconds.
/// This is set to 7 days, which is the default in delta-spark.
const DEFAULT_RETENTION_SECS: u64 = SECONDS_PER_MINUTE * MINUTES_PER_HOUR * HOURS_PER_DAY * DAYS;

/// Schema for extracting relevant actions from log files for checkpoint creation
static CHECKPOINT_ACTIONS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
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

// Schema of the [`CheckpointMetadata`] action that is included in V2 checkpoints
// We cannot use `CheckpointMetadata::to_schema()` as it would include the 'tags' field which
// we're not supporting yet due to the lack of map support.
static CHECKPOINT_METADATA_ACTION_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    StructType::new([StructField::not_null(
        CHECKPOINT_METADATA_NAME,
        DataType::struct_type([StructField::not_null("version", DataType::LONG)]),
    )])
    .into()
});

/// Represents the data needed to create a checkpoint file.
///
/// Obtained from [`CheckpointWriter::checkpoint_data`], this struct provides both the
/// location where the checkpoint file should be written and an iterator over the data
/// that should be included in the checkpoint.
///
/// # Fields
/// - `path`: The URL where the checkpoint file should be written.
/// - `data`: A boxed iterator that yields checkpoint actions as chunks of [`EngineData`].
///
/// # Usage
/// 1. Write every action yielded by `data` to persistent storage at the URL specified by `path`.
/// 2. Ensure that all data is fully persisted before calling `CheckpointWriter::finalize`.
///    This is crucial to avoid data loss or corruption.
pub struct CheckpointData {
    /// The URL where the checkpoint file should be written.
    pub path: Url,

    /// An iterator over the checkpoint data to be written to the file.
    pub data: Box<dyn Iterator<Item = DeltaResult<FilteredEngineData>>>,
}

/// Orchestrates the process of creating and finalizing a checkpoint.
///
/// The [`CheckpointWriter`] is the entry point for generating checkpoint data for a Delta table.
/// It automatically selects the appropriate checkpoint format (V1/V2) based on whether the table
/// supports the `v2Checkpoints` reader/writer feature.
///
/// # Usage Workflow
/// 1. Create a [`CheckpointWriter`] via [`crate::table::Table::checkpoint`].
/// 2. Call [`CheckpointWriter::checkpoint_data`] to obtain a [`CheckpointData`] instance.
/// 3. Write out all actions from the [`CheckpointData::data`] iterator to the destination
///    specified by [`CheckpointData::path`].
/// 4. After successfully writing all data, finalize the checkpoint by calling
///    `CheckpointWriter::finalize`] to write the `_last_checkpoint` file.
///
/// # Important Notes
/// - The checkpoint data must be fully written to persistent storage before calling `finalize()`
///   in step 3. Failing to do so may result in data loss or corruption.
/// - This API automatically selects the appropriate checkpoint format (V1/V2) based on the table's
///   `v2Checkpoints` feature support.
pub struct CheckpointWriter {
    /// Reference to the snapshot of the table being checkpointed
    pub(crate) snapshot: Arc<Snapshot>,
    /// Note: `Arc<AtomicI64>` provides shared mutability for our counters, allowing the
    /// returned actions iterator from `.checkpoint_data()` to update the counters,
    /// and the [`CheckpointWriter`] to read them during `.finalize()`
    /// Counter for total actions included in the checkpoint
    pub(crate) total_actions_counter: Arc<AtomicI64>,
    /// Counter for Add actions included in the checkpoint
    pub(crate) add_actions_counter: Arc<AtomicI64>,
}

impl CheckpointWriter {
    /// Creates a new CheckpointWriter from a snapshot
    pub(crate) fn new(snapshot: Arc<Snapshot>) -> Self {
        Self {
            snapshot,
            total_actions_counter: Arc::new(AtomicI64::new(0)),
            add_actions_counter: Arc::new(AtomicI64::new(0)),
        }
    }

    /// Retrieves the checkpoint data and path information
    ///
    /// This method is the core of the checkpoint generation process. It:
    /// 1. Determines whether to write a V1 or V2 checkpoint based on the table's
    ///    `v2Checkpoints` feature support
    /// 2. Reads actions from the log segment using the checkpoint read schema
    /// 3. Filters and deduplicates actions for the checkpoint
    /// 4. Chains the checkpoint metadata action if writing a V2 spec checkpoint
    ///    (i.e., if `v2Checkpoints` feature is supported by table)
    /// 5. Generates the appropriate checkpoint path
    ///
    /// # Returns: [`CheckpointData`] containing the checkpoint path and data to write
    ///
    /// # Important: The returned data should be written to persistent storage by the
    /// caller before calling `finalize()` otherwise data loss may occur.
    pub fn checkpoint_data(&mut self, engine: &dyn Engine) -> DeltaResult<CheckpointData> {
        let is_v2_checkpoints_supported = self
            .snapshot
            .table_configuration()
            .is_v2_checkpoint_supported();

        let actions = self.snapshot.log_segment().read_actions(
            engine,
            CHECKPOINT_ACTIONS_SCHEMA.clone(),
            CHECKPOINT_ACTIONS_SCHEMA.clone(),
            None,
        )?;

        // Create iterator over actions for checkpoint data
        let checkpoint_data = CheckpointLogReplayProcessor::new(
            self.total_actions_counter.clone(),
            self.add_actions_counter.clone(),
            self.deleted_file_retention_timestamp()?,
        )
        .process_actions_iter(actions);

        let version = self.snapshot.version().try_into().map_err(|e| {
            Error::CheckpointWrite(format!(
                "Failed to convert checkpoint version from u64 {} to i64: {}",
                self.snapshot.version(),
                e
            ))
        })?;

        // Chain the checkpoint metadata action if using V2 checkpoints
        let chained = checkpoint_data.chain(self.create_checkpoint_metadata_batch(
            version,
            engine,
            is_v2_checkpoints_supported,
        )?);

        let checkpoint_path = ParsedLogPath::new_classic_parquet_checkpoint(
            self.snapshot.table_root(),
            self.snapshot.version(),
        )?;

        Ok(CheckpointData {
            path: checkpoint_path.location,
            data: Box::new(chained),
        })
    }

    /// TODO(#850): Implement the finalize method
    ///
    /// Finalizes the checkpoint writing. This function writes the `_last_checkpoint` file
    ///
    /// The `_last_checkpoint` file is a metadata file that contains information about the
    /// last checkpoint created for the table. It is used as a hint for the engine to quickly
    /// locate the last checkpoint and avoid full log replay when reading the table.
    ///
    /// # Important
    /// This method must only be called **after** successfully writing all checkpoint data to storage.
    /// Failure to do so may result in data loss.
    ///
    /// # Parameters
    /// - `engine`: The engine used for writing the `_last_checkpoint` file
    /// - `metadata`: A single-row, single-column [`EngineData`] batch containing:
    ///   - `sizeInBytes` (i64): The size of the written checkpoint file
    ///
    /// # Returns: [`variant@Ok`] if the `_last_checkpoint` file was written successfully
    #[allow(unused)]
    fn finalize(self, _engine: &dyn Engine, _metadata: &dyn EngineData) -> DeltaResult<()> {
        todo!("Implement the finalize method which will write the _last_checkpoint file")
    }

    /// Creates the checkpoint metadata action for V2 checkpoints.
    ///
    /// For V2 checkpoints, this function generates the [`CheckpointMetadata`] action
    /// that must be included in the V2 spec checkpoint file. This action contains metadata
    /// about the checkpoint, particularly its version. For V1 checkpoints, this function
    /// returns `None`, as the V1 checkpoint schema does not include this action type.
    ///
    /// # Implementation Details
    ///
    /// The function creates a single-row [`EngineData`] batch containing only the
    /// version field of the [`CheckpointMetadata`] action. Future implementations will
    /// include the additional metadata field `tags` when map support is added.
    ///
    /// # Returns:
    /// A [`FilteredEngineData`] batch including the single-row [`EngineData`] batch along with
    /// an accompanying selection vector with a single `true` value, indicating the action in
    /// batch should be included in the checkpoint.
    fn create_checkpoint_metadata_batch(
        &self,
        version: i64,
        engine: &dyn Engine,
        is_v2_checkpoint: bool,
    ) -> DeltaResult<Option<DeltaResult<FilteredEngineData>>> {
        if !is_v2_checkpoint {
            return Ok(None);
        }
        let values: &[Scalar] = &[version.into()];

        let checkpoint_metadata_batch = engine
            .evaluation_handler()
            .create_one(CHECKPOINT_METADATA_ACTION_SCHEMA.clone(), values)?;

        let result = FilteredEngineData {
            data: checkpoint_metadata_batch,
            selection_vector: vec![true], // Include the action in the checkpoint
        };

        // Safe to use Relaxed here:
        // "Incrementing a counter can be safely done by multiple threads using a relaxed fetch_add
        // if you're not using the counter to synchronize any other accesses." – Rust Atomics and Locks
        self.total_actions_counter.fetch_add(1, Ordering::Relaxed);

        Ok(Some(Ok(result)))
    }

    /// Calculates the cutoff timestamp for deleted file cleanup.
    ///
    /// This function determines the minimum timestamp before which deleted files
    /// will be permanently removed during VACUUM operations, based on the table's
    /// `deleted_file_retention_duration` property.
    ///
    /// Returns the cutoff timestamp in milliseconds since epoch, matching
    /// the remove action's `deletion_timestamp` field format for comparison.
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
}

/// Calculates the timestamp threshold for deleted file retention based on the provided duration.
/// This is factored out to allow testing with an injectable time and duration parameter.
///
/// # Parameters
/// - `retention_duration`: The duration to retain deleted files. The table property
///   `deleted_file_retention_duration` is passed here. If `None`, defaults to 7 days.
/// - `now_duration`: The current time as a [`Duration`]. This allows for testing with
///   a specific time instead of using `SystemTime::now()`.
///
/// # Returns: The timestamp in milliseconds since epoch
fn deleted_file_retention_timestamp_with_time(
    retention_duration: Option<Duration>,
    now_duration: Duration,
) -> DeltaResult<i64> {
    // Use provided retention duration or default (7 days)
    let retention_duration =
        retention_duration.unwrap_or_else(|| Duration::from_secs(DEFAULT_RETENTION_SECS));

    // Convert to milliseconds for remove action deletion_timestamp comparison
    let now_ms: i64 = now_duration
        .as_millis()
        .try_into()
        .map_err(|_| Error::checkpoint_write("Current timestamp exceeds i64 millisecond range"))?;

    let retention_ms: i64 = retention_duration
        .as_millis()
        .try_into()
        .map_err(|_| Error::checkpoint_write("Retention duration exceeds i64 millisecond range"))?;

    // Simple subtraction - will produce negative values if retention > now
    Ok(now_ms - retention_ms)
}

#[cfg(test)]
mod unit_tests {
    use super::*;
    use crate::engine::{arrow_data::ArrowEngineData, sync::SyncEngine};
    use crate::Table;
    use arrow_53::{array::RecordBatch, datatypes::Field};
    use delta_kernel::arrow::array::create_array;
    use std::path::PathBuf;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use crate::arrow::array::{ArrayRef, StructArray};
    use crate::arrow::datatypes::{DataType, Schema};

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

    fn create_test_snapshot(engine: &dyn Engine) -> DeltaResult<Arc<Snapshot>> {
        let path = std::fs::canonicalize(PathBuf::from("./tests/data/app-txn-no-checkpoint/"));
        let url = url::Url::from_directory_path(path.unwrap()).unwrap();
        let table = Table::new(url);
        Ok(Arc::new(table.snapshot(engine, None)?))
    }

    #[test]
    fn test_create_checkpoint_metadata_batch_when_v2_checkpoints_is_supported() -> DeltaResult<()> {
        let engine = SyncEngine::new();
        let version = 10;
        let writer = CheckpointWriter::new(create_test_snapshot(&engine)?);

        // Test with is_v2_checkpoint = true
        let result = writer.create_checkpoint_metadata_batch(version, &engine, true)?;
        assert!(result.is_some());
        let checkpoint_data = result.unwrap()?;

        // Check selection vector has one true value
        assert_eq!(checkpoint_data.selection_vector, vec![true]);

        // Verify the underlying EngineData contains the expected CheckpointMetadata action
        let arrow_engine_data = ArrowEngineData::try_from_engine_data(checkpoint_data.data)?;
        let record_batch = arrow_engine_data.record_batch();

        // Build the expected RecordBatch
        // Note: The schema is a struct with a single field "checkpointMetadata" of type struct
        // containing a single field "version" of type long
        let expected_schema = Arc::new(Schema::new(vec![Field::new(
            "checkpointMetadata",
            DataType::Struct(vec![Field::new("version", DataType::Int64, false)].into()),
            false,
        )]));
        let expected = RecordBatch::try_new(
            expected_schema,
            vec![Arc::new(StructArray::from(vec![(
                Arc::new(Field::new("version", DataType::Int64, false)),
                create_array!(Int64, [version]) as ArrayRef,
            )]))],
        )
        .unwrap();

        assert_eq!(*record_batch, expected);
        assert_eq!(writer.total_actions_counter.load(Ordering::Relaxed), 1);

        Ok(())
    }

    #[test]
    fn test_create_checkpoint_metadata_batch_when_v2_checkpoints_not_supported() -> DeltaResult<()>
    {
        let engine = SyncEngine::new();
        let writer = CheckpointWriter::new(create_test_snapshot(&engine)?);

        // Test with is_v2_checkpoint = false
        let result = writer.create_checkpoint_metadata_batch(10, &engine, false)?;

        // No checkpoint metadata action should be created for V1 checkpoints
        assert!(result.is_none());
        assert_eq!(writer.total_actions_counter.load(Ordering::Relaxed), 0);

        Ok(())
    }
}
