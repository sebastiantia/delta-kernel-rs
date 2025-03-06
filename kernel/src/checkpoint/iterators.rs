use crate::actions::{Metadata, Protocol, Sidecar};
use crate::engine::arrow_data::ArrowEngineData;
use crate::path::ParsedLogPath;
use crate::snapshot::{self, CheckpointMetadata, Snapshot};
use crate::{error, DeltaResult, Engine, EngineData, Error, Version};
use arrow_53::array::{Int64Array, RecordBatch, StringArray, UInt64Array};
use std::iter::Peekable;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use url::Url;

use super::log_replay::v2_checkpoint_actions_iter;
use super::{
    get_checkpoint_metadata_schema, get_last_checkpoint_schema, get_sidecar_schema,
    get_v2_checkpoint_schema,
};

/// The filtered version of engine data with a boolean vector indicating which actions to process
pub type FilteredEngineData = (Box<dyn EngineData>, Vec<bool>);

/// Iterator for file data
pub type FileData = Box<dyn Iterator<Item = DeltaResult<FilteredEngineData>> + Send>;

/// A tuple containing file data and its associated URL
pub type FileDataWithUrl = (FileData, Url);

/// Iterator for file data with URLs
pub type FileDataIter = Box<dyn Iterator<Item = DeltaResult<FileDataWithUrl>> + Send>;

/// Trait defining operations for checkpoint iterators.
/// Checkpoint iterators are responsible for generating checkpoint files
/// for Delta tables in both V1 and V2 formats.
pub trait CheckpointIterator: Iterator<Item = DeltaResult<FileDataWithUrl>> + Send {
    /// Creates a new checkpoint iterator that includes sidecar metadata.
    /// Used for V2 checkpoint creation.
    fn sidecar_metadata(
        &self,
        sidecar_metadata: Vec<Sidecar>,
        engine: &dyn Engine,
    ) -> DeltaResult<Box<dyn CheckpointIterator>>;

    /// Writes the checkpoint metadata to the _last_checkpoint.json file.
    fn checkpoint_metadata(
        &self,
        metadata: &dyn EngineData,
        engine: &dyn Engine,
    ) -> DeltaResult<()>;
}

/// An iterator that lazily groups incoming actions into chunks based on a threshold.
/// This is used to create sidecar files for V2 checkpoints.
pub struct SidecarIterator<I>
where
    I: Iterator<Item = DeltaResult<FilteredEngineData>> + Send + 'static,
{
    iter: Peekable<I>,                           // Underlying iterator for actions
    threshold: usize,                            // Maximum number of actions per sidecar file
    table_location: Url,                         // Root location of the Delta table
    num_sidecars: usize,                         // Counter for generated sidecar files
    snapshot: Snapshot,                          // Snapshot of the Delta table
    total_actions_counter: Arc<AtomicUsize>,     // Counter for total actions processed
    total_add_actions_counter: Arc<AtomicUsize>, // Counter for ADD actions processed
}

impl<I> SidecarIterator<I>
where
    I: Iterator<Item = DeltaResult<FilteredEngineData>> + Send + 'static,
{
    pub fn new(
        iter: I,
        threshold: usize,
        table_location: Url,
        snapshot: Snapshot,
        total_actions_counter: Arc<AtomicUsize>,
        total_add_actions_counter: Arc<AtomicUsize>,
    ) -> Self {
        SidecarIterator {
            iter: iter.peekable(),
            threshold,
            table_location,
            num_sidecars: 0,
            snapshot,
            total_actions_counter,
            total_add_actions_counter,
        }
    }

    /// Helper to count the number of true flags in a vector.
    fn count_true_flags(bools: &[bool]) -> usize {
        bools.iter().filter(|&&flag| flag).count()
    }

    /// Creates record batches for sidecar metadata.
    ///
    /// Converts a vector of Sidecar objects into filtered engine data that can be
    /// written to the checkpoint file.
    fn create_sidecar_batches(
        &self,
        metadata: Vec<Sidecar>,
    ) -> DeltaResult<Vec<DeltaResult<FilteredEngineData>>> {
        let mut result = vec![];

        for sidecar in metadata {
            // TODO: Replace with create_one() expression API when available

            // Construct the record batch with sidecar metadata fields
            let path = Arc::new(StringArray::from(vec![sidecar.path]));
            let size_in_bytes = Arc::new(Int64Array::from(vec![sidecar.size_in_bytes]));
            let modification_time = Arc::new(Int64Array::from(vec![sidecar.modification_time]));

            // Create a record batch with the sidecar schema
            let record_batch = RecordBatch::try_new(
                Arc::new(get_sidecar_schema().as_ref().try_into()?),
                vec![path, size_in_bytes, modification_time],
            )?;

            // Add the record batch to results with all flags set to true (process all)
            result.push(Ok((
                Box::new(ArrowEngineData::new(record_batch)) as Box<dyn EngineData>,
                vec![true],
            )));
        }

        Ok(result)
    }
}

impl<I> CheckpointIterator for SidecarIterator<I>
where
    I: Iterator<Item = DeltaResult<FilteredEngineData>> + Send + 'static,
{
    fn sidecar_metadata(
        &self,
        metadata: Vec<Sidecar>,
        engine: &dyn Engine,
    ) -> DeltaResult<Box<dyn CheckpointIterator>> {
        let schema = get_v2_checkpoint_schema();

        let iter =
            self.snapshot
                .log_segment()
                .replay(engine, schema.clone(), schema.clone(), None)?;

        let actions_iter = v2_checkpoint_actions_iter(iter, self.total_actions_counter.clone());

        // Combine the action iterator with geneerated sidecar action batches
        let total_actions_iter =
            actions_iter.chain(self.create_sidecar_batches(metadata)?.into_iter());

        Ok(Box::new(V2CheckpointFileIterator {
            iter: Some(total_actions_iter.peekable()),
            table_root: self.table_location.clone(),
            checkpoint_version: self.snapshot.version(),
            total_actions_counter: self.total_actions_counter.clone(),
            total_add_actions_counter: self.total_add_actions_counter.clone(),
        }))
    }

    fn checkpoint_metadata(
        &self,
        metadata: &dyn EngineData,
        engine: &dyn Engine,
    ) -> DeltaResult<()> {
        // For V2 checkpoints, sidecar_metadata must be called first
        return Err(Error::GenericError {
        source: "Writing a V2 checkpoint, and still need to write top-level V2 checkpoint file. First call sidecar() instead!".to_string().into(),
    });
    }
}

impl<I> Iterator for SidecarIterator<I>
where
    I: Iterator<Item = DeltaResult<FilteredEngineData>> + Send + 'static,
{
    type Item = DeltaResult<(FileData, Url)>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut chunk = Vec::new();
        let mut actions_in_chunk = 0;

        // Keep collecting action batches until we reach the threshold or exhaust the iterator
        while self.iter.peek().is_some() {
            // Count how many action batches in the next item would be selected
            let num_selected_actions = self
                .iter
                .peek()
                .and_then(|res| res.as_ref().ok())
                .map(|(_, bools)| Self::count_true_flags(bools))
                .unwrap_or(0);

            // If adding the next batch would exceed the threshold, stop collecting
            if !chunk.is_empty() && actions_in_chunk + num_selected_actions > self.threshold {
                break;
            }
            // Get the next item and add it to the chunk
            // Should not panic as we've checked for None.
            let item = self.iter.next()?;
            actions_in_chunk += num_selected_actions;
            chunk.push(item);
        }

        // If no actions were collected, we're done
        if chunk.is_empty() {
            return None;
        }

        // If we've exhausted the iterator and haven't yet written a sidecar file,
        // we can move the file actions into the top-level v2 checkpoint file
        // if self.iter.peek().is_none() && self.num_sidecars == 0 {
        //     // Optimization
        //     todo!();
        // }

        // Process chunk and create a sidecar file.
        // TODO: Follow sidecar naming convention
        let file_name = format!("{:020}.parquet", self.num_sidecars);
        self.num_sidecars += 1;
        let sidecar_path_res = self
            .table_location
            .join("_delta_log/sidecars/")
            .and_then(|url| url.join(&file_name));

        match sidecar_path_res {
            Ok(sidecar_path) => {
                // Wrap the collected Vec into an iterator for FileData.
                let sidecar_iter = Box::new(chunk.into_iter());
                Some(Ok((sidecar_iter, sidecar_path)))
            }
            Err(e) => Some(Err(error::Error::InvalidUrl(e))),
        }
    }
}

/// Iterator for generating V2 checkpoint files.
/// This is created after sidecar files have been generated
/// and contains the top-level checkpoint information.
pub struct V2CheckpointFileIterator<I>
where
    I: Iterator<Item = DeltaResult<FilteredEngineData>> + Send + 'static,
{
    iter: Option<Peekable<I>>, // Iterator wrapped in Option to allow moving out
    table_root: Url,           // Root location of the Delta table
    checkpoint_version: Version, // Version number for the checkpoint
    total_actions_counter: Arc<AtomicUsize>, // Counter for total actions
    total_add_actions_counter: Arc<AtomicUsize>, // Counter for ADD actions
}

// Intermediate iterator to handle the V2 checkpoint file creation
impl<I> Iterator for V2CheckpointFileIterator<I>
where
    I: Iterator<Item = DeltaResult<FilteredEngineData>> + Send + 'static,
{
    type Item = DeltaResult<FileDataWithUrl>;

    fn next(&mut self) -> Option<Self::Item> {
        // This iterator should return the V2 checkpoint file as its only item
        // by moving the inner iterator out of self.iter
        let iter = self.iter.take()?;

        // Convert the iterator into a FileData type
        let file_data = Box::new(iter) as FileData;

        // Create the path for the V2 checkpoint file
        let checkpoint_path = match self.table_root.join("_delta_log/") {
            Ok(path) => match path.join(&format!(
                "{:020}.checkpoint.parquet",
                self.checkpoint_version
            )) {
                Ok(full_path) => full_path,
                Err(e) => return Some(Err(error::Error::InvalidUrl(e))),
            },
            Err(e) => return Some(Err(error::Error::InvalidUrl(e))),
        };

        // Return the FileData and URL as a tuple
        Some(Ok((file_data, checkpoint_path)))
    }
}

impl<I> CheckpointIterator for V2CheckpointFileIterator<I>
where
    I: Iterator<Item = DeltaResult<FilteredEngineData>> + Send + 'static,
{
    fn sidecar_metadata(
        &self,
        _sidecar_metadata: Vec<Sidecar>,
        _engine: &dyn Engine,
    ) -> DeltaResult<Box<dyn CheckpointIterator>> {
        // Cannot create another checkpoint iterator from V2CheckpointFileIterator
        Err(Error::GenericError {
            source: "V2CheckpointFileIterator already created. Cannot create another checkpoint iterator.".to_string().into(),
        })
    }

    fn checkpoint_metadata(
        &self,
        metadata: &dyn EngineData,
        engine: &dyn Engine,
    ) -> DeltaResult<()> {
        // TODO: make sure metadata conforms to the correct schema & extract `sizeInBytes`.
        let checkpoint_metadata_schema = get_checkpoint_metadata_schema();

        let last_checkpoint_path = self
            .table_root
            .join("_delta_log/")?
            .join("_last_checkpoint.json")?;

        // TODO: leverage create_one() expression API when available?

        // Get the record batch for the metadata and wrap it into an iterator.
        let data = last_checkpoint_record_batch(
            self.checkpoint_version,
            self.total_actions_counter.clone(),
            self.total_add_actions_counter.clone(),
        )?;
        let iter = Box::new(std::iter::once(Ok(data)));
        engine
            .get_json_handler()
            .write_json_file(&last_checkpoint_path, iter, true)?;

        Ok(())
    }
}

/// Iterator for generating V1 checkpoint files.
/// V1 checkpoints use a single file without sidecars.
pub struct V1CheckpointFileIterator<I>
where
    I: Iterator<Item = DeltaResult<FilteredEngineData>> + Send + 'static,
{
    iter: Option<Peekable<I>>, // Iterator wrapped in Option to allow moving out
    table_root: Url,           // Root location of the Delta table
    checkpoint_version: Version, // Version number for the checkpoint
    total_actions_counter: Arc<AtomicUsize>, // Counter for total actions
    total_add_actions_counter: Arc<AtomicUsize>, // Counter for ADD actions
}

impl<I> V1CheckpointFileIterator<I>
where
    I: Iterator<Item = DeltaResult<FilteredEngineData>> + Send + 'static,
{
    pub fn new(
        iter: I,
        table_root: Url,
        checkpoint_version: Version,
        total_actions_counter: Arc<AtomicUsize>,
        total_add_actions_counter: Arc<AtomicUsize>,
    ) -> Self {
        V1CheckpointFileIterator {
            iter: Some(iter.peekable()), // Wrap in Option for later movement
            table_root,
            checkpoint_version,
            total_actions_counter,
            total_add_actions_counter,
        }
    }
}

impl<I> CheckpointIterator for V1CheckpointFileIterator<I>
where
    I: Iterator<Item = DeltaResult<FilteredEngineData>> + Send + 'static,
{
    fn sidecar_metadata(
        &self,
        _sidecar_metadata: Vec<Sidecar>,
        _engine: &dyn Engine,
    ) -> DeltaResult<Box<dyn CheckpointIterator>> {
        // V1 checkpoints do not support sidecar files
        return Err(Error::GenericError {
            source: "Writing a V1 checkpoint. Cannot transition to V2. Call checkpoint_metadata() instead.".to_string().into(),
        });
    }

    // Write the `_last_checkpoint` metadata file.
    fn checkpoint_metadata(
        &self,
        // Metadata must follow the schema defined in `get_checkpoint_metadata_schema()`.
        metadata: &dyn EngineData,
        engine: &dyn Engine,
    ) -> DeltaResult<()> {
        // TODO: make sure metadata conforms to the correct schema & extract `sizeInBytes`.
        let checkpoint_metadata_schema = get_checkpoint_metadata_schema();

        let last_checkpoint_path = self
            .table_root
            .join("_delta_log/")?
            .join("_last_checkpoint.json")?;

        // TODO: leverage create_one() expression API when available?

        // Get the record batch for the metadata and wrap it into an iterator.
        let data = last_checkpoint_record_batch(
            self.checkpoint_version,
            self.total_actions_counter.clone(),
            self.total_add_actions_counter.clone(),
        )?;
        let iter = Box::new(std::iter::once(Ok(data)));
        engine
            .get_json_handler()
            .write_json_file(&last_checkpoint_path, iter, true)?;

        Ok(())
    }
}

impl<I> Iterator for V1CheckpointFileIterator<I>
where
    I: Iterator<Item = DeltaResult<FilteredEngineData>> + Send + 'static,
{
    type Item = DeltaResult<FileDataWithUrl>;

    // we need to move self.iter out of self, but self.iter is behind a mutable reference (&mut self).
    // Since we don't need self.iter anymore after calling next() once, we can use Option::take() to move it out.
    fn next(&mut self) -> Option<Self::Item> {
        let v1_checkpoint_file_path =
            match ParsedLogPath::new_v1_checkpoint(&self.table_root, self.checkpoint_version) {
                Ok(path) => path,
                Err(_err) => {
                    return Some(Err(Error::Generic(
                        "Error parsing checkpoint file path".to_string(),
                    )))
                }
            };

        // We call take() on self.iter because we expect only a single chunk.
        self.iter
            .take()
            .map(|iter| Ok((Box::new(iter) as FileData, v1_checkpoint_file_path.location)))
    }
}

/// Creates a record batch for the _last_checkpoint.json file.
fn last_checkpoint_record_batch(
    version: Version,
    total_actions_counter: Arc<AtomicUsize>,
    total_add_actions_counter: Arc<AtomicUsize>,
) -> DeltaResult<Box<dyn EngineData>> {
    let total_actions_counter: i64 =
        total_actions_counter.load(std::sync::atomic::Ordering::SeqCst) as i64;

    let total_add_actions_counter: i64 =
        total_add_actions_counter.load(std::sync::atomic::Ordering::SeqCst) as i64;

    // TODO: Return error on failure to cast. Look into correcting the types anyways.
    let version = Arc::new(Int64Array::from(vec![version as i64]));
    let size = Arc::new(Int64Array::from(vec![total_actions_counter]));
    // TODO: Parts is usize but we are using i64
    let parts = Arc::new(Int64Array::from(vec![1]));
    let num_of_add_files = Arc::new(Int64Array::from(vec![total_add_actions_counter]));
    // TODO: Checksum

    // Construct the record batch.
    let record_batch = RecordBatch::try_new(
        Arc::new(get_last_checkpoint_schema().as_ref().try_into()?),
        vec![
            version,
            size,
            parts,
            Arc::new(Int64Array::from(vec![None])), // Optional: sizeInBytes (checksum column)
            num_of_add_files,
            Arc::new(StringArray::from(vec![None::<&str>])),
        ],
    )?;
    Ok(Box::new(ArrowEngineData::new(record_batch)))
}
