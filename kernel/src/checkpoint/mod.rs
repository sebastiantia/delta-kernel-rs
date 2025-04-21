//! This module implements the API for writing single-file checkpoints.
//!
//! The entry-point for this API is [`Table::checkpoint`].
//!
//! ## Checkpoint Types and Selection Logic
//! This API supports two checkpoint types, selected based on table features:
//!
//! | Table Feature    | Resulting Checkpoint Type    | Description                                                                 |
//! |------------------|-------------------------------|-----------------------------------------------------------------------------|
//! | No v2Checkpoints | Single-file Classic-named V1 | Follows V1 specification without [`CheckpointMetadata`] action             |
//! | v2Checkpoints    | Single-file Classic-named V2 | Follows V2 specification with [`CheckpointMetadata`] action while maintaining backward compatibility via classic naming |
//!
//! For more information on the V1/V2 specifications, see the following protocol section:
//! <https://github.com/delta-io/delta/blob/master/PROTOCOL.md#checkpoint-specs>
//!
//! ## Architecture
//!
//! - [`CheckpointWriter`] - Core component that manages the checkpoint creation workflow
//! - [`CheckpointData`] - Wraps the [`CheckpointDataIterator`] and destination path information
//! - [`CheckpointDataIterator`] - Iterator over the checkpoint data to be written
//!
//! ## Usage
//!
//! The following steps outline the process of creating a checkpoint:
//!
//! 1. Create a [`CheckpointWriter`] using [`Table::checkpoint`]
//! 2. Get checkpoint data and path with [`CheckpointWriter::checkpoint_data`]
//! 3. Write the [`CheckpointDataIterator`] returned in [`CheckpointData`] to [`CheckpointData::path`]
//! 4. Finalize the checkpoint with `CheckpointWriter::finalize`
//!
//! ```no_run
//! # use std::sync::Arc;
//! # use delta_kernel::checkpoint::CheckpointData;
//! # use delta_kernel::checkpoint::CheckpointWriter;
//! # use delta_kernel::table::Table;
//! # use delta_kernel::DeltaResult;
//! # use delta_kernel::Error;
//! # use delta_kernel::FileMeta;
//! # use url::Url;
//! fn write_checkpoint_file(checkpoint_data: &CheckpointData) -> DeltaResult<FileMeta> {
//!     todo!() /* engine-specific logic to write checkpoint_data.data to checkpoint_data.path */
//! }
//!
//! // Create an engine instance
//! let engine: Arc<dyn Engine> = Arc::new(todo!("create your engine here"));
//!
//! // Create a table instance for the table you want to checkpoint
//! let table = Table::try_from_uri("./tests/data/app-txn-no-checkpoint")?;
//!
//! // Create a checkpoint writer for a version of the table (`None` for latest)
//! let mut writer: CheckpointWriter = table.checkpoint(&engine, Some(1))?;
//!
//! // Get the checkpoint data and path
//! let checkpoint_data = writer.checkpoint_data(&engine)?;
//!
//! // Write the checkpoint data to the object store and collect metadata
//! let metadata: FileMeta = write_checkpoint_file(&checkpoint_data)?;
//!
//! /* IMPORTANT: All data must be written before finalizing the checkpoint */
//!
//! // TODO(#850): Implement the finalize method
//! // writer.finalize(&engine, &metadata, checkpoint_data.data)?;
//!
//! # Ok::<_, Error>(())
//! ```
//!
//! ## Warning
//! Multi-part (V1) checkpoints are DEPRECATED and UNSAFE.
//!
//! [`CheckpointMetadata`]: crate::actions::CheckpointMetadata
//! [`LastCheckpointHint`]: crate::snapshot::LastCheckpointHint
//! [`Table::checkpoint`]: crate::table::Table::checkpoint
// Future extensions
// - TODO(#836): Single-file UUID-named V2 checkpoints (using `n.checkpoint.u.{json/parquet}` naming) are to be
//   implemented in the future. The current implementation only supports classic-named V2 checkpoints.
// - TODO(#837): Multi-file V2 checkpoints are not supported yet. The API is designed to be extensible for future
//   multi-file support, but the current implementation only supports single-file checkpoints.
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
use crate::{DeltaResult, Engine, Error, EvaluationHandlerExtension, FileMeta};
use log_replay::{CheckpointBatch, CheckpointLogReplayProcessor};

use url::Url;

mod log_replay;
#[cfg(test)]
mod tests;

const SECONDS_PER_MINUTE: u64 = 60;
const MINUTES_PER_HOUR: u64 = 60;
const HOURS_PER_DAY: u64 = 24;
/// The default retention period for deleted files in seconds.
/// This is set to 7 days, which is the default in delta-spark.
const DEFAULT_RETENTION_SECS: u64 = 7 * HOURS_PER_DAY * MINUTES_PER_HOUR * SECONDS_PER_MINUTE;

/// Schema for extracting relevant actions from log files for checkpoint creation
static CHECKPOINT_ACTIONS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(StructType::new([
        Option::<Add>::get_struct_field(ADD_NAME),
        Option::<Remove>::get_struct_field(REMOVE_NAME),
        Option::<Metadata>::get_struct_field(METADATA_NAME),
        Option::<Protocol>::get_struct_field(PROTOCOL_NAME),
        Option::<SetTransaction>::get_struct_field(SET_TRANSACTION_NAME),
        Option::<Sidecar>::get_struct_field(SIDECAR_NAME),
    ]))
});

// Schema of the [`CheckpointMetadata`] action that is included in V2 checkpoints
// We cannot use `CheckpointMetadata::to_schema()` as it would include the 'tags' field which
// we're not supporting yet due to the lack of map support.
static CHECKPOINT_METADATA_ACTION_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(StructType::new([StructField::not_null(
        CHECKPOINT_METADATA_NAME,
        DataType::struct_type([StructField::not_null("version", DataType::LONG)]),
    )]))
});

/// An iterator over the checkpoint data to be written to the file.
///
/// This iterator yields filtered checkpoint data batches ([`FilteredEngineData`]) and
/// tracks action statistics required for finalizing the checkpoint.
///
/// # Warning
/// Additionally, all yielded data must be written to the specified path before calling
/// `CheckpointWriter::finalize`, or it may result in data loss and corruption.
pub struct CheckpointDataIterator {
    /// The inner iterator that yields the checkpoint data with counts
    inner: Box<dyn Iterator<Item = DeltaResult<CheckpointBatch>>>,
    /// Running total of actions included in the checkpoint
    actions_count: i64,
    /// Running total of add actions included in the checkpoint
    add_actions_count: i64,
}

impl Iterator for CheckpointDataIterator {
    type Item = DeltaResult<FilteredEngineData>;

    /// Advances the iterator and returns the next value.
    ///
    /// This implementation transforms the `CheckpointBatch` items from the inner iterator into
    /// [`FilteredEngineData`] items for the engine to write, while accumulating action counts for
    /// each batch. The [`CheckpointDataIterator`] is passed back to the kernel on call to
    /// `CheckpointWriter::finalize` for counts to be read and written to the `_last_checkpoint` file
    fn next(&mut self) -> Option<Self::Item> {
        let next_item = self.inner.next();

        next_item.map(|result| {
            result.map(|batch| {
                self.actions_count += batch.actions_count;
                self.add_actions_count += batch.add_actions_count;
                batch.filtered_data
            })
        })
    }
}

/// Represents the data needed to create a single-file checkpoint.
///
/// Obtained from [`CheckpointWriter::checkpoint_data`], this struct provides both the
/// location where the checkpoint file should be written and an iterator over the data
/// that should be included in the checkpoint.
///
/// # Warning
/// The [`CheckpointDataIterator`] must be fully consumed to ensure proper collection of statistics for
/// the checkpoint. Additionally, all yielded data must be written to the specified path before calling
/// `CheckpointWriter::finalize`. Failing to do so may result in data loss or corruption.
pub struct CheckpointData {
    /// The URL where the checkpoint file should be written.
    pub path: Url,
    /// An iterator over the checkpoint data to be written to the file.
    pub data: CheckpointDataIterator,
}

/// Orchestrates the process of creating a checkpoint for a table.
///
/// The [`CheckpointWriter`] is the entry point for generating checkpoint data for a Delta table.
/// It automatically selects the appropriate checkpoint format (V1/V2) based on whether the table
/// supports the `v2Checkpoints` reader/writer feature.
///
/// # Warning
/// The checkpoint data must be fully written to storage before calling `CheckpointWriter::finalize()`.
/// Failing to do so may result in data loss or corruption.
///
/// # See Also
/// See the [module-level documentation](self) for the complete checkpoint workflow
///
/// [`Table::checkpoint`]: [`crate::table::Table::checkpoint`]
pub struct CheckpointWriter {
    /// Reference to the snapshot (i.e. version) of the table being checkpointed
    pub(crate) snapshot: Arc<Snapshot>,
}

impl CheckpointWriter {
    /// Creates a new CheckpointWriter from a snapshot
    pub(crate) fn new(snapshot: Arc<Snapshot>) -> Self {
        Self { snapshot }
    }

    /// Retrieves the checkpoint data and path information.
    ///
    /// This method generates the filtered actions for the checkpoint and determines
    /// the appropriate destination path.
    ///
    /// # Returns
    /// [`CheckpointData`] containing the checkpoint path and data to write.
    ///
    /// # Warning
    /// All data must be written to persistent storage before calling `CheckpointWriter::finalize()`.
    // This method is the core of the checkpoint generation process. It:
    // 1. Determines whether to write a V1 or V2 checkpoint based on the table's
    //    `v2Checkpoints` feature support
    // 2. Reads actions from the log segment using the checkpoint read schema
    // 3. Filters and deduplicates actions for the checkpoint
    // 4. Chains the checkpoint metadata action if writing a V2 spec checkpoint
    //    (i.e., if `v2Checkpoints` feature is supported by table)
    // 5. Generates the appropriate checkpoint path
    pub fn checkpoint_data(&mut self, engine: &dyn Engine) -> DeltaResult<CheckpointData> {
        let is_v2_checkpoints_supported = self
            .snapshot
            .table_configuration()
            .is_v2_checkpoint_write_supported();

        let actions = self.snapshot.log_segment().read_actions(
            engine,
            CHECKPOINT_ACTIONS_SCHEMA.clone(),
            CHECKPOINT_ACTIONS_SCHEMA.clone(),
            None,
        )?;

        // Create iterator over actions for checkpoint data
        let checkpoint_data =
            CheckpointLogReplayProcessor::new(self.deleted_file_retention_timestamp()?)
                .process_actions_iter(actions);

        let version = self.snapshot.version().try_into().map_err(|e| {
            Error::CheckpointWrite(format!(
                "Failed to convert checkpoint version from u64 {} to i64: {}",
                self.snapshot.version(),
                e
            ))
        })?;

        // Chain the checkpoint metadata action if using V2 checkpoints
        let chained = checkpoint_data.chain(
            is_v2_checkpoints_supported
                .then(|| self.create_checkpoint_metadata_batch(version, engine)),
        );

        let checkpoint_path = ParsedLogPath::new_classic_parquet_checkpoint(
            self.snapshot.table_root(),
            self.snapshot.version(),
        )?;

        // Wrap the data iterator to send counts to the CheckpointWriter when dropped
        let wrapped_iterator = CheckpointDataIterator {
            inner: Box::new(chained),
            actions_count: 0,
            add_actions_count: 0,
        };

        Ok(CheckpointData {
            path: checkpoint_path.location,
            data: wrapped_iterator,
        })
    }

    /// TODO(#850): Implement the finalize method
    ///
    /// Finalizes checkpoint creation after verifying all data is persisted.
    ///
    /// This method **must** be called only after:
    /// 1. The checkpoint data iterator has been fully consumed
    /// 2. All data has been successfully written to object storage
    ///
    /// # Parameters
    /// - `engine`: Implementation of [`Engine`] apis.
    /// - `metadata`: The metadata of the written checkpoint file
    /// - `checkpoint_data_iter`: The exhausted checkpoint data iterator (must be fully consumed)

    ///
    /// # Returns: [`variant@Ok`] if the checkpoint was successfully finalized
    #[allow(unused)]
    fn finalize(
        self,
        _engine: &dyn Engine,
        _metadata: &FileMeta,
        _checkpoint_data_iter: CheckpointDataIterator,
    ) -> DeltaResult<()> {
        // Verify the iterator is exhausted (optional)
        // Implementation will use checkpoint_data.actions_count and checkpoint_data.add_actions_count

        // TODO(#850): Implement the actual finalization logic
        todo!("Implement finalize method for checkpoint writer")
    }

    /// Creates the checkpoint metadata action for V2 checkpoints.
    ///
    /// This function generates the [`CheckpointMetadata`] action that must be included in the
    /// V2 spec checkpoint file. This action contains metadata about the checkpoint, particularly
    /// its version.
    ///
    /// # Implementation Details
    ///
    /// The function creates a single-row [`EngineData`] batch containing only the
    /// version field of the [`CheckpointMetadata`] action. Future implementations will
    /// include the additional metadata field `tags` when map support is added.
    ///
    /// # Returns:
    /// A [`CheckpointBatch`] batch including the single-row [`EngineData`] batch along with
    /// an accompanying selection vector with a single `true` value, indicating the action in
    /// batch should be included in the checkpoint.
    fn create_checkpoint_metadata_batch(
        &self,
        version: i64,
        engine: &dyn Engine,
    ) -> DeltaResult<CheckpointBatch> {
        let checkpoint_metadata_batch = engine.evaluation_handler().create_one(
            CHECKPOINT_METADATA_ACTION_SCHEMA.clone(),
            &[Scalar::from(version)],
        )?;

        let filtered_data = FilteredEngineData {
            data: checkpoint_metadata_batch,
            selection_vector: vec![true], // Include the action in the checkpoint
        };

        Ok(CheckpointBatch {
            filtered_data,
            actions_count: 1,
            add_actions_count: 0,
        })
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
