use std::{path::PathBuf, sync::Arc};

use itertools::Itertools;
use object_store::{memory::InMemory, path::Path, ObjectStore};
use parquet::arrow::ArrowWriter;
use url::Url;

use crate::actions::visitors::AddVisitor;
use crate::actions::{
    get_log_add_schema, get_log_schema, Add, Sidecar, ADD_NAME, METADATA_NAME, REMOVE_NAME,
    SIDECAR_NAME,
};
use crate::engine::arrow_data::ArrowEngineData;
use crate::engine::default::executor::tokio::TokioBackgroundExecutor;
use crate::engine::default::filesystem::ObjectStoreFileSystemClient;
use crate::engine::default::DefaultEngine;
use crate::engine::sync::SyncEngine;
use crate::log_segment::LogSegment;
use crate::path::ParsedLogPath;
use crate::scan::test_utils::{
    add_batch_simple, add_batch_with_remove, sidecar_batch_with_given_paths,
};
use crate::snapshot::CheckpointMetadata;
use crate::utils::test_utils::{assert_batch_matches, Action};
use crate::{DeltaResult, Engine, EngineData, FileMeta, FileSystemClient, RowVisitor, Table};
use test_utils::delta_path_for_version;

// NOTE: In addition to testing the meta-predicate for metadata replay, this test also verifies
// that the parquet reader properly infers nullcount = rowcount for missing columns. The two
// checkpoint part files that contain transaction app ids have truncated schemas that would
// otherwise fail skipping due to their missing nullcount stat:
//
// Row group 0:  count: 1  total(compressed): 111 B total(uncompressed):107 B
// --------------------------------------------------------------------------------
//              type    nulls  min / max
// txn.appId    BINARY  0      "3ae45b72-24e1-865a-a211-3..." / "3ae45b72-24e1-865a-a211-3..."
// txn.version  INT64   0      "4390" / "4390"
#[test]
fn test_replay_for_metadata() {
    let path = std::fs::canonicalize(PathBuf::from("./tests/data/parquet_row_group_skipping/"));
    let url = url::Url::from_directory_path(path.unwrap()).unwrap();
    let engine = SyncEngine::new();

    let table = Table::new(url);
    let snapshot = table.snapshot(&engine, None).unwrap();
    let data: Vec<_> = snapshot
        .log_segment()
        .replay_for_metadata(&engine)
        .unwrap()
        .try_collect()
        .unwrap();

    // The checkpoint has five parts, each containing one action:
    // 1. txn (physically missing P&M columns)
    // 2. metaData
    // 3. protocol
    // 4. add
    // 5. txn (physically missing P&M columns)
    //
    // The parquet reader should skip parts 1, 3, and 5. Note that the actual `read_metadata`
    // always skips parts 4 and 5 because it terminates the iteration after finding both P&M.
    //
    // NOTE: Each checkpoint part is a single-row file -- guaranteed to produce one row group.
    //
    // WARNING: https://github.com/delta-io/delta-kernel-rs/issues/434 -- We currently
    // read parts 1 and 5 (4 in all instead of 2) because row group skipping is disabled for
    // missing columns, but can still skip part 3 because has valid nullcount stats for P&M.
    assert_eq!(data.len(), 4);
}

// get an ObjectStore path for a checkpoint file, based on version, part number, and total number of parts
fn delta_path_for_multipart_checkpoint(version: u64, part_num: u32, num_parts: u32) -> Path {
    let path =
        format!("_delta_log/{version:020}.checkpoint.{part_num:010}.{num_parts:010}.parquet");
    Path::from(path.as_str())
}

// Utility method to build a log using a list of log paths and an optional checkpoint hint. The
// CheckpointMetadata is written to `_delta_log/_last_checkpoint`.
fn build_log_with_paths_and_checkpoint(
    paths: &[Path],
    checkpoint_metadata: Option<&CheckpointMetadata>,
) -> (Box<dyn FileSystemClient>, Url) {
    let store = Arc::new(InMemory::new());

    let data = bytes::Bytes::from("kernel-data");

    // add log files to store
    tokio::runtime::Runtime::new()
        .expect("create tokio runtime")
        .block_on(async {
            for path in paths {
                store
                    .put(path, data.clone().into())
                    .await
                    .expect("put log file in store");
            }
            if let Some(checkpoint_metadata) = checkpoint_metadata {
                let checkpoint_str =
                    serde_json::to_string(checkpoint_metadata).expect("Serialize checkpoint");
                store
                    .put(
                        &Path::from("_delta_log/_last_checkpoint"),
                        checkpoint_str.into(),
                    )
                    .await
                    .expect("Write _last_checkpoint");
            }
        });

    let client = ObjectStoreFileSystemClient::new(
        store,
        false, // don't have ordered listing
        Path::from("/"),
        Arc::new(TokioBackgroundExecutor::new()),
    );

    let table_root = Url::parse("memory:///").expect("valid url");
    let log_root = table_root.join("_delta_log/").unwrap();
    (Box::new(client), log_root)
}

// Create an in-memory store and return the store and the URL for the store's _delta_log directory.
fn new_in_memory_store() -> (Arc<InMemory>, Url) {
    (
        Arc::new(InMemory::new()),
        Url::parse("memory:///")
            .unwrap()
            .join("_delta_log/")
            .unwrap(),
    )
}

// Writes a record batch obtained from engine data to the in-memory store at a given path.
fn write_parquet_to_store(
    store: &Arc<InMemory>,
    path: String,
    data: Box<dyn EngineData>,
) -> DeltaResult<()> {
    let batch = ArrowEngineData::try_from_engine_data(data)?;
    let record_batch = batch.record_batch();

    let mut buffer = vec![];
    let mut writer = ArrowWriter::try_new(&mut buffer, record_batch.schema(), None)?;
    writer.write(record_batch)?;
    writer.close()?;

    tokio::runtime::Runtime::new()
        .expect("create tokio runtime")
        .block_on(async { store.put(&Path::from(path), buffer.into()).await })?;

    Ok(())
}

/// Writes all actions to a _delta_log parquet checkpoint file in the store.
/// This function formats the provided filename into the _delta_log directory.
fn add_checkpoint_to_store(
    store: &Arc<InMemory>,
    data: Box<dyn EngineData>,
    filename: &str,
) -> DeltaResult<()> {
    let path = format!("_delta_log/{}", filename);
    write_parquet_to_store(store, path, data)
}

/// Writes all actions to a _delta_log/_sidecars file in the store.
/// This function formats the provided filename into the _sidecars subdirectory.
fn add_sidecar_to_store(
    store: &Arc<InMemory>,
    data: Box<dyn EngineData>,
    filename: &str,
) -> DeltaResult<()> {
    let path = format!("_delta_log/_sidecars/{}", filename);
    write_parquet_to_store(store, path, data)
}

/// Writes all actions to a _delta_log json checkpoint file in the store.
/// This function formats the provided filename into the _delta_log directory.
fn write_json_to_store(
    store: &Arc<InMemory>,
    actions: Vec<Action>,
    filename: &str,
) -> DeltaResult<()> {
    let json_lines: Vec<String> = actions
        .into_iter()
        .map(|action| serde_json::to_string(&action).expect("action to string"))
        .collect();
    let content = json_lines.join("\n");
    let checkpoint_path = format!("_delta_log/{}", filename);

    tokio::runtime::Runtime::new()
        .expect("create tokio runtime")
        .block_on(async {
            store
                .put(&Path::from(checkpoint_path), content.into())
                .await
        })?;

    Ok(())
}

fn create_log_path(path: &str) -> ParsedLogPath<FileMeta> {
    ParsedLogPath::try_from(FileMeta {
        location: Url::parse(path).expect("Invalid file URL"),
        last_modified: 0,
        size: 0,
    })
    .unwrap()
    .unwrap()
}

#[test]
fn build_snapshot_with_uuid_checkpoint_parquet() {
    let (client, log_root) = build_log_with_paths_and_checkpoint(
        &[
            delta_path_for_version(0, "json"),
            delta_path_for_version(1, "checkpoint.parquet"),
            delta_path_for_version(2, "json"),
            delta_path_for_version(3, "checkpoint.parquet"),
            delta_path_for_version(4, "json"),
            delta_path_for_version(5, "json"),
            delta_path_for_version(5, "checkpoint.3a0d65cd-4056-49b8-937b-95f9e3ee90e5.parquet"),
            delta_path_for_version(6, "json"),
            delta_path_for_version(7, "json"),
        ],
        None,
    );

    let log_segment = LogSegment::for_snapshot(client.as_ref(), log_root, None, None).unwrap();
    let commit_files = log_segment.ascending_commit_files;
    let checkpoint_parts = log_segment.checkpoint_parts;

    assert_eq!(checkpoint_parts.len(), 1);
    assert_eq!(checkpoint_parts[0].version, 5);

    let versions = commit_files.into_iter().map(|x| x.version).collect_vec();
    let expected_versions = vec![6, 7];
    assert_eq!(versions, expected_versions);
}

#[test]
fn build_snapshot_with_uuid_checkpoint_json() {
    let (client, log_root) = build_log_with_paths_and_checkpoint(
        &[
            delta_path_for_version(0, "json"),
            delta_path_for_version(1, "checkpoint.parquet"),
            delta_path_for_version(2, "json"),
            delta_path_for_version(3, "checkpoint.parquet"),
            delta_path_for_version(4, "json"),
            delta_path_for_version(5, "json"),
            delta_path_for_version(5, "checkpoint.3a0d65cd-4056-49b8-937b-95f9e3ee90e5.json"),
            delta_path_for_version(6, "json"),
            delta_path_for_version(7, "json"),
        ],
        None,
    );

    let log_segment = LogSegment::for_snapshot(client.as_ref(), log_root, None, None).unwrap();
    let commit_files = log_segment.ascending_commit_files;
    let checkpoint_parts = log_segment.checkpoint_parts;

    assert_eq!(checkpoint_parts.len(), 1);
    assert_eq!(checkpoint_parts[0].version, 5);

    let versions = commit_files.into_iter().map(|x| x.version).collect_vec();
    let expected_versions = vec![6, 7];
    assert_eq!(versions, expected_versions);
}

#[test]
fn build_snapshot_with_correct_last_uuid_checkpoint() {
    let checkpoint_metadata = CheckpointMetadata {
        version: 5,
        size: 10,
        parts: Some(1),
        size_in_bytes: None,
        num_of_add_files: None,
        checkpoint_schema: None,
        checksum: None,
    };

    let (client, log_root) = build_log_with_paths_and_checkpoint(
        &[
            delta_path_for_version(0, "json"),
            delta_path_for_version(1, "checkpoint.parquet"),
            delta_path_for_version(1, "json"),
            delta_path_for_version(2, "json"),
            delta_path_for_version(3, "checkpoint.parquet"),
            delta_path_for_version(3, "json"),
            delta_path_for_version(4, "json"),
            delta_path_for_version(5, "checkpoint.3a0d65cd-4056-49b8-937b-95f9e3ee90e5.parquet"),
            delta_path_for_version(5, "json"),
            delta_path_for_version(6, "json"),
            delta_path_for_version(7, "json"),
        ],
        Some(&checkpoint_metadata),
    );

    let log_segment =
        LogSegment::for_snapshot(client.as_ref(), log_root, checkpoint_metadata, None).unwrap();
    let commit_files = log_segment.ascending_commit_files;
    let checkpoint_parts = log_segment.checkpoint_parts;

    assert_eq!(checkpoint_parts.len(), 1);
    assert_eq!(commit_files.len(), 2);
    assert_eq!(checkpoint_parts[0].version, 5);
    assert_eq!(commit_files[0].version, 6);
    assert_eq!(commit_files[1].version, 7);
}
#[test]
fn build_snapshot_with_multiple_incomplete_multipart_checkpoints() {
    let (client, log_root) = build_log_with_paths_and_checkpoint(
        &[
            delta_path_for_version(0, "json"),
            delta_path_for_multipart_checkpoint(1, 1, 3),
            // Part 2 of 3 at version 1 is missing!
            delta_path_for_multipart_checkpoint(1, 3, 3),
            delta_path_for_multipart_checkpoint(2, 1, 2),
            // Part 2 of 2 at version 2 is missing!
            delta_path_for_version(2, "json"),
            delta_path_for_multipart_checkpoint(3, 1, 3),
            // Part 2 of 3 at version 3 is missing!
            delta_path_for_multipart_checkpoint(3, 3, 3),
            delta_path_for_multipart_checkpoint(3, 1, 4),
            delta_path_for_multipart_checkpoint(3, 2, 4),
            delta_path_for_multipart_checkpoint(3, 3, 4),
            delta_path_for_multipart_checkpoint(3, 4, 4),
            delta_path_for_version(4, "json"),
            delta_path_for_version(5, "json"),
            delta_path_for_version(6, "json"),
            delta_path_for_version(7, "json"),
        ],
        None,
    );

    let log_segment = LogSegment::for_snapshot(client.as_ref(), log_root, None, None).unwrap();
    let commit_files = log_segment.ascending_commit_files;
    let checkpoint_parts = log_segment.checkpoint_parts;

    assert_eq!(checkpoint_parts.len(), 4);
    assert_eq!(checkpoint_parts[0].version, 3);

    let versions = commit_files.into_iter().map(|x| x.version).collect_vec();
    let expected_versions = vec![4, 5, 6, 7];
    assert_eq!(versions, expected_versions);
}

#[test]
fn build_snapshot_with_out_of_date_last_checkpoint() {
    let checkpoint_metadata = CheckpointMetadata {
        version: 3,
        size: 10,
        parts: None,
        size_in_bytes: None,
        num_of_add_files: None,
        checkpoint_schema: None,
        checksum: None,
    };

    let (client, log_root) = build_log_with_paths_and_checkpoint(
        &[
            delta_path_for_version(0, "json"),
            delta_path_for_version(1, "checkpoint.parquet"),
            delta_path_for_version(2, "json"),
            delta_path_for_version(3, "checkpoint.parquet"),
            delta_path_for_version(4, "json"),
            delta_path_for_version(5, "checkpoint.parquet"),
            delta_path_for_version(6, "json"),
            delta_path_for_version(7, "json"),
        ],
        Some(&checkpoint_metadata),
    );

    let log_segment =
        LogSegment::for_snapshot(client.as_ref(), log_root, checkpoint_metadata, None).unwrap();
    let commit_files = log_segment.ascending_commit_files;
    let checkpoint_parts = log_segment.checkpoint_parts;

    assert_eq!(checkpoint_parts.len(), 1);
    assert_eq!(commit_files.len(), 2);
    assert_eq!(checkpoint_parts[0].version, 5);
    assert_eq!(commit_files[0].version, 6);
    assert_eq!(commit_files[1].version, 7);
}
#[test]
fn build_snapshot_with_correct_last_multipart_checkpoint() {
    let checkpoint_metadata = CheckpointMetadata {
        version: 5,
        size: 10,
        parts: Some(3),
        size_in_bytes: None,
        num_of_add_files: None,
        checkpoint_schema: None,
        checksum: None,
    };

    let (client, log_root) = build_log_with_paths_and_checkpoint(
        &[
            delta_path_for_version(0, "json"),
            delta_path_for_version(1, "checkpoint.parquet"),
            delta_path_for_version(1, "json"),
            delta_path_for_version(2, "json"),
            delta_path_for_version(3, "checkpoint.parquet"),
            delta_path_for_version(3, "json"),
            delta_path_for_version(4, "json"),
            delta_path_for_multipart_checkpoint(5, 1, 3),
            delta_path_for_multipart_checkpoint(5, 2, 3),
            delta_path_for_multipart_checkpoint(5, 3, 3),
            delta_path_for_version(5, "json"),
            delta_path_for_version(6, "json"),
            delta_path_for_version(7, "json"),
        ],
        Some(&checkpoint_metadata),
    );

    let log_segment =
        LogSegment::for_snapshot(client.as_ref(), log_root, checkpoint_metadata, None).unwrap();
    let commit_files = log_segment.ascending_commit_files;
    let checkpoint_parts = log_segment.checkpoint_parts;

    assert_eq!(checkpoint_parts.len(), 3);
    assert_eq!(commit_files.len(), 2);
    assert_eq!(checkpoint_parts[0].version, 5);
    assert_eq!(commit_files[0].version, 6);
    assert_eq!(commit_files[1].version, 7);
}

#[test]
fn build_snapshot_with_missing_checkpoint_part_from_hint_fails() {
    let checkpoint_metadata = CheckpointMetadata {
        version: 5,
        size: 10,
        parts: Some(3),
        size_in_bytes: None,
        num_of_add_files: None,
        checkpoint_schema: None,
        checksum: None,
    };

    let (client, log_root) = build_log_with_paths_and_checkpoint(
        &[
            delta_path_for_version(0, "json"),
            delta_path_for_version(1, "checkpoint.parquet"),
            delta_path_for_version(1, "json"),
            delta_path_for_version(2, "json"),
            delta_path_for_version(3, "checkpoint.parquet"),
            delta_path_for_version(3, "json"),
            delta_path_for_version(4, "json"),
            delta_path_for_multipart_checkpoint(5, 1, 3),
            // Part 2 of 3 at version 5 is missing!
            delta_path_for_multipart_checkpoint(5, 3, 3),
            delta_path_for_version(5, "json"),
            delta_path_for_version(6, "json"),
            delta_path_for_version(7, "json"),
        ],
        Some(&checkpoint_metadata),
    );

    let log_segment =
        LogSegment::for_snapshot(client.as_ref(), log_root, checkpoint_metadata, None);
    assert!(log_segment.is_err())
}
#[test]
fn build_snapshot_with_bad_checkpoint_hint_fails() {
    let checkpoint_metadata = CheckpointMetadata {
        version: 5,
        size: 10,
        parts: Some(1),
        size_in_bytes: None,
        num_of_add_files: None,
        checkpoint_schema: None,
        checksum: None,
    };

    let (client, log_root) = build_log_with_paths_and_checkpoint(
        &[
            delta_path_for_version(0, "json"),
            delta_path_for_version(1, "checkpoint.parquet"),
            delta_path_for_version(1, "json"),
            delta_path_for_version(2, "json"),
            delta_path_for_version(3, "checkpoint.parquet"),
            delta_path_for_version(3, "json"),
            delta_path_for_version(4, "json"),
            delta_path_for_multipart_checkpoint(5, 1, 2),
            delta_path_for_multipart_checkpoint(5, 2, 2),
            delta_path_for_version(5, "json"),
            delta_path_for_version(6, "json"),
            delta_path_for_version(7, "json"),
        ],
        Some(&checkpoint_metadata),
    );

    let log_segment =
        LogSegment::for_snapshot(client.as_ref(), log_root, checkpoint_metadata, None);
    assert!(log_segment.is_err())
}

#[test]
fn build_snapshot_with_missing_checkpoint_part_no_hint() {
    // Part 2 of 3 is missing from checkpoint 5. The Snapshot should be made of checkpoint
    // number 3 and commit files 4 to 7.
    let (client, log_root) = build_log_with_paths_and_checkpoint(
        &[
            delta_path_for_version(0, "json"),
            delta_path_for_version(1, "checkpoint.parquet"),
            delta_path_for_version(1, "json"),
            delta_path_for_version(2, "json"),
            delta_path_for_version(3, "checkpoint.parquet"),
            delta_path_for_version(3, "json"),
            delta_path_for_version(4, "json"),
            delta_path_for_multipart_checkpoint(5, 1, 3),
            // Part 2 of 3 at version 5 is missing!
            delta_path_for_multipart_checkpoint(5, 3, 3),
            delta_path_for_version(5, "json"),
            delta_path_for_version(6, "json"),
            delta_path_for_version(7, "json"),
        ],
        None,
    );

    let log_segment = LogSegment::for_snapshot(client.as_ref(), log_root, None, None).unwrap();

    let commit_files = log_segment.ascending_commit_files;
    let checkpoint_parts = log_segment.checkpoint_parts;

    assert_eq!(checkpoint_parts.len(), 1);
    assert_eq!(checkpoint_parts[0].version, 3);

    let versions = commit_files.into_iter().map(|x| x.version).collect_vec();
    let expected_versions = vec![4, 5, 6, 7];
    assert_eq!(versions, expected_versions);
}

#[test]
fn build_snapshot_with_out_of_date_last_checkpoint_and_incomplete_recent_checkpoint() {
    // When the _last_checkpoint is out of date and the most recent checkpoint is incomplete, the
    // Snapshot should be made of the most recent complete checkpoint and the commit files that
    // follow it.
    let checkpoint_metadata = CheckpointMetadata {
        version: 3,
        size: 10,
        parts: None,
        size_in_bytes: None,
        num_of_add_files: None,
        checkpoint_schema: None,
        checksum: None,
    };

    let (client, log_root) = build_log_with_paths_and_checkpoint(
        &[
            delta_path_for_version(0, "json"),
            delta_path_for_version(1, "checkpoint.parquet"),
            delta_path_for_version(2, "json"),
            delta_path_for_version(3, "checkpoint.parquet"),
            delta_path_for_version(4, "json"),
            delta_path_for_multipart_checkpoint(5, 1, 3),
            // Part 2 of 3 at version 5 is missing!
            delta_path_for_multipart_checkpoint(5, 3, 3),
            delta_path_for_version(5, "json"),
            delta_path_for_version(6, "json"),
            delta_path_for_version(7, "json"),
        ],
        Some(&checkpoint_metadata),
    );

    let log_segment =
        LogSegment::for_snapshot(client.as_ref(), log_root, checkpoint_metadata, None).unwrap();
    let commit_files = log_segment.ascending_commit_files;
    let checkpoint_parts = log_segment.checkpoint_parts;

    assert_eq!(checkpoint_parts.len(), 1);
    assert_eq!(checkpoint_parts[0].version, 3);

    let versions = commit_files.into_iter().map(|x| x.version).collect_vec();
    let expected_versions = vec![4, 5, 6, 7];
    assert_eq!(versions, expected_versions);
}

#[test]
fn build_snapshot_without_checkpoints() {
    let (client, log_root) = build_log_with_paths_and_checkpoint(
        &[
            delta_path_for_version(0, "json"),
            delta_path_for_version(1, "json"),
            delta_path_for_version(1, "checkpoint.parquet"),
            delta_path_for_version(2, "json"),
            delta_path_for_version(3, "json"),
            delta_path_for_version(3, "checkpoint.parquet"),
            delta_path_for_version(4, "json"),
            delta_path_for_version(5, "json"),
            delta_path_for_version(5, "checkpoint.parquet"),
            delta_path_for_version(6, "json"),
            delta_path_for_version(7, "json"),
        ],
        None,
    );

    ///////// Specify no checkpoint or end version /////////
    let log_segment =
        LogSegment::for_snapshot(client.as_ref(), log_root.clone(), None, None).unwrap();
    let commit_files = log_segment.ascending_commit_files;
    let checkpoint_parts = log_segment.checkpoint_parts;

    assert_eq!(checkpoint_parts.len(), 1);
    assert_eq!(checkpoint_parts[0].version, 5);

    // All commit files should still be there
    let versions = commit_files.into_iter().map(|x| x.version).collect_vec();
    let expected_versions = vec![6, 7];
    assert_eq!(versions, expected_versions);

    ///////// Specify  only end version /////////
    let log_segment = LogSegment::for_snapshot(client.as_ref(), log_root, None, Some(2)).unwrap();
    let commit_files = log_segment.ascending_commit_files;
    let checkpoint_parts = log_segment.checkpoint_parts;

    assert_eq!(checkpoint_parts.len(), 1);
    assert_eq!(checkpoint_parts[0].version, 1);

    // All commit files should still be there
    let versions = commit_files.into_iter().map(|x| x.version).collect_vec();
    let expected_versions = vec![2];
    assert_eq!(versions, expected_versions);
}

#[test]
fn build_snapshot_with_checkpoint_greater_than_time_travel_version() {
    let checkpoint_metadata = CheckpointMetadata {
        version: 5,
        size: 10,
        parts: None,
        size_in_bytes: None,
        num_of_add_files: None,
        checkpoint_schema: None,
        checksum: None,
    };
    let (client, log_root) = build_log_with_paths_and_checkpoint(
        &[
            delta_path_for_version(0, "json"),
            delta_path_for_version(1, "json"),
            delta_path_for_version(1, "checkpoint.parquet"),
            delta_path_for_version(2, "json"),
            delta_path_for_version(3, "json"),
            delta_path_for_version(3, "checkpoint.parquet"),
            delta_path_for_version(4, "json"),
            delta_path_for_version(5, "json"),
            delta_path_for_version(5, "checkpoint.parquet"),
            delta_path_for_version(6, "json"),
            delta_path_for_version(7, "json"),
        ],
        None,
    );

    let log_segment =
        LogSegment::for_snapshot(client.as_ref(), log_root, checkpoint_metadata, Some(4)).unwrap();
    let commit_files = log_segment.ascending_commit_files;
    let checkpoint_parts = log_segment.checkpoint_parts;

    assert_eq!(checkpoint_parts.len(), 1);
    assert_eq!(checkpoint_parts[0].version, 3);

    assert_eq!(commit_files.len(), 1);
    assert_eq!(commit_files[0].version, 4);
}

#[test]
fn build_snapshot_with_start_checkpoint_and_time_travel_version() {
    let checkpoint_metadata = CheckpointMetadata {
        version: 3,
        size: 10,
        parts: None,
        size_in_bytes: None,
        num_of_add_files: None,
        checkpoint_schema: None,
        checksum: None,
    };

    let (client, log_root) = build_log_with_paths_and_checkpoint(
        &[
            delta_path_for_version(0, "json"),
            delta_path_for_version(1, "checkpoint.parquet"),
            delta_path_for_version(2, "json"),
            delta_path_for_version(3, "checkpoint.parquet"),
            delta_path_for_version(4, "json"),
            delta_path_for_version(5, "checkpoint.parquet"),
            delta_path_for_version(6, "json"),
            delta_path_for_version(7, "json"),
        ],
        Some(&checkpoint_metadata),
    );

    let log_segment =
        LogSegment::for_snapshot(client.as_ref(), log_root, checkpoint_metadata, Some(4)).unwrap();

    assert_eq!(log_segment.checkpoint_parts[0].version, 3);
    assert_eq!(log_segment.ascending_commit_files.len(), 1);
    assert_eq!(log_segment.ascending_commit_files[0].version, 4);
}
#[test]
fn build_table_changes_with_commit_versions() {
    let (client, log_root) = build_log_with_paths_and_checkpoint(
        &[
            delta_path_for_version(0, "json"),
            delta_path_for_version(1, "json"),
            delta_path_for_version(1, "checkpoint.parquet"),
            delta_path_for_version(2, "json"),
            delta_path_for_version(3, "json"),
            delta_path_for_version(3, "checkpoint.parquet"),
            delta_path_for_version(4, "json"),
            delta_path_for_version(5, "json"),
            delta_path_for_version(5, "checkpoint.parquet"),
            delta_path_for_version(6, "json"),
            delta_path_for_version(7, "json"),
        ],
        None,
    );

    ///////// Specify start version and end version /////////

    let log_segment =
        LogSegment::for_table_changes(client.as_ref(), log_root.clone(), 2, 5).unwrap();
    let commit_files = log_segment.ascending_commit_files;
    let checkpoint_parts = log_segment.checkpoint_parts;

    // Checkpoints should be omitted
    assert_eq!(checkpoint_parts.len(), 0);

    // Commits between 2 and 5 (inclusive) should be returned
    let versions = commit_files.into_iter().map(|x| x.version).collect_vec();
    let expected_versions = (2..=5).collect_vec();
    assert_eq!(versions, expected_versions);

    ///////// Start version and end version are the same /////////
    let log_segment =
        LogSegment::for_table_changes(client.as_ref(), log_root.clone(), 0, Some(0)).unwrap();

    let commit_files = log_segment.ascending_commit_files;
    let checkpoint_parts = log_segment.checkpoint_parts;
    // Checkpoints should be omitted
    assert_eq!(checkpoint_parts.len(), 0);

    // There should only be commit version 0
    assert_eq!(commit_files.len(), 1);
    assert_eq!(commit_files[0].version, 0);

    ///////// Specify no start or end version /////////
    let log_segment = LogSegment::for_table_changes(client.as_ref(), log_root, 0, None).unwrap();
    let commit_files = log_segment.ascending_commit_files;
    let checkpoint_parts = log_segment.checkpoint_parts;

    // Checkpoints should be omitted
    assert_eq!(checkpoint_parts.len(), 0);

    // Commits between 2 and 7 (inclusive) should be returned
    let versions = commit_files.into_iter().map(|x| x.version).collect_vec();
    let expected_versions = (0..=7).collect_vec();
    assert_eq!(versions, expected_versions);
}

#[test]
fn test_non_contiguous_log() {
    // Commit with version 1 is missing
    let (client, log_root) = build_log_with_paths_and_checkpoint(
        &[
            delta_path_for_version(0, "json"),
            delta_path_for_version(2, "json"),
        ],
        None,
    );

    let log_segment_res = LogSegment::for_table_changes(client.as_ref(), log_root.clone(), 0, None);
    assert!(log_segment_res.is_err());

    let log_segment_res = LogSegment::for_table_changes(client.as_ref(), log_root.clone(), 1, None);
    assert!(log_segment_res.is_err());

    let log_segment_res = LogSegment::for_table_changes(client.as_ref(), log_root, 0, Some(1));
    assert!(log_segment_res.is_err());
}

#[test]
fn table_changes_fails_with_larger_start_version_than_end() {
    // Commit with version 1 is missing
    let (client, log_root) = build_log_with_paths_and_checkpoint(
        &[
            delta_path_for_version(0, "json"),
            delta_path_for_version(1, "json"),
        ],
        None,
    );
    let log_segment_res = LogSegment::for_table_changes(client.as_ref(), log_root, 1, Some(0));
    assert!(log_segment_res.is_err());
}
#[test]
fn test_sidecar_to_filemeta_valid_paths() -> DeltaResult<()> {
    let log_root = Url::parse("file:///var/_delta_log/")?;
    let test_cases = [
        (
            "example.parquet",
            "file:///var/_delta_log/_sidecars/example.parquet",
        ),
        (
            "file:///var/_delta_log/_sidecars/example.parquet",
            "file:///var/_delta_log/_sidecars/example.parquet",
        ),
        (
            "test/test/example.parquet",
            "file:///var/_delta_log/_sidecars/test/test/example.parquet",
        ),
    ];

    for (input_path, expected_url) in test_cases.into_iter() {
        let sidecar = Sidecar {
            path: expected_url.to_string(),
            modification_time: 0,
            size_in_bytes: 1000,
            tags: None,
        };

        let filemeta = LogSegment::sidecar_to_filemeta(&sidecar, &log_root)?;
        assert_eq!(
            filemeta.location.as_str(),
            expected_url,
            "Mismatch for input path: {}",
            input_path
        );
    }
    Ok(())
}

#[test]
fn test_checkpoint_batch_with_no_sidecars_returns_none() -> DeltaResult<()> {
    let (_, log_root) = new_in_memory_store();
    let engine = Arc::new(SyncEngine::new());
    let checkpoint_batch = add_batch_simple(get_log_schema().clone());

    let mut iter = LogSegment::process_sidecars(
        engine.get_parquet_handler(),
        log_root,
        checkpoint_batch.as_ref(),
    )?
    .into_iter()
    .flatten();

    // Assert no batches are returned
    assert!(iter.next().is_none());

    Ok(())
}

#[test]
fn test_checkpoint_batch_with_sidecars_returns_sidecar_batches() -> DeltaResult<()> {
    let (store, log_root) = new_in_memory_store();
    let engine = DefaultEngine::new(
        store.clone(),
        Path::from("/"),
        Arc::new(TokioBackgroundExecutor::new()),
    );

    add_sidecar_to_store(
        &store,
        add_batch_simple(get_log_schema().project(&[ADD_NAME, REMOVE_NAME])?),
        "sidecarfile1.parquet",
    )?;
    add_sidecar_to_store(
        &store,
        add_batch_with_remove(get_log_schema().project(&[ADD_NAME, REMOVE_NAME])?),
        "sidecarfile2.parquet",
    )?;

    let checkpoint_batch = sidecar_batch_with_given_paths(
        vec!["sidecarfile1.parquet", "sidecarfile2.parquet"],
        get_log_schema().project(&[ADD_NAME, SIDECAR_NAME])?,
    );

    let mut iter = LogSegment::process_sidecars(
        engine.get_parquet_handler(),
        log_root,
        checkpoint_batch.as_ref(),
    )?
    .into_iter()
    .flatten();

    // Assert the correctness of batches returned
    let sidecar_schema = get_log_add_schema().clone();
    assert_batch_matches(
        iter.next().unwrap()?,
        add_batch_simple(sidecar_schema.clone()),
    );
    assert_batch_matches(
        iter.next().unwrap()?,
        add_batch_with_remove(sidecar_schema.clone()),
    );
    assert!(iter.next().is_none());

    Ok(())
}

#[test]
fn test_checkpoint_batch_with_sidecar_files_that_do_not_exist() -> DeltaResult<()> {
    let (store, log_root) = new_in_memory_store();
    let engine = DefaultEngine::new(
        store.clone(),
        Path::from("/"),
        Arc::new(TokioBackgroundExecutor::new()),
    );

    let checkpoint_batch = sidecar_batch_with_given_paths(
        vec!["sidecarfile1.parquet", "sidecarfile2.parquet"],
        get_log_schema().clone(),
    );

    let mut iter = LogSegment::process_sidecars(
        engine.get_parquet_handler(),
        log_root,
        checkpoint_batch.as_ref(),
    )?
    .into_iter()
    .flatten();

    // Assert that an error is returned when trying to read sidecar files that do not exist
    let err = iter.next().unwrap();
    assert!(err.is_err());

    Ok(())
}

#[test]
fn test_create_checkpoint_stream_returns_none_if_checkpoint_parts_is_empty() -> DeltaResult<()> {
    let engine = SyncEngine::new();

    let mut iter = LogSegment::create_checkpoint_stream(
        &engine,
        get_log_schema().clone(),
        None,
        vec![],
        Url::parse("s3://example-bucket/logs/")?,
    )
    .into_iter()
    .flatten();

    // Assert no batches are returned
    assert!(iter.next().is_none());

    Ok(())
}

#[test]
fn test_create_checkpoint_stream_errors_when_schema_has_add_but_no_sidecar_action(
) -> DeltaResult<()> {
    let engine = SyncEngine::new();

    // Create the stream over checkpoint batches.
    let result = LogSegment::create_checkpoint_stream(
        &engine,
        get_log_add_schema().clone(),
        None,
        vec![create_log_path("file:///00000000000000000001.parquet")],
        Url::parse("s3://example-bucket/logs/")?,
    );

    // Errors because the schema has an ADD action but no SIDECAR action.
    assert!(result.is_err());

    Ok(())
}

#[test]
fn test_create_checkpoint_stream_returns_checkpoint_batches_as_is_if_schema_has_no_file_actions(
) -> DeltaResult<()> {
    let (store, log_root) = new_in_memory_store();
    let engine = DefaultEngine::new(
        store.clone(),
        Path::from("/"),
        Arc::new(TokioBackgroundExecutor::new()),
    );
    add_checkpoint_to_store(
        &store,
        // Create a checkpoint batch with sidecar actions to verify that the sidecar actions are not read.
        sidecar_batch_with_given_paths(vec!["sidecar1.parquet"], get_log_schema().clone()),
        "00000000000000000001.checkpoint.parquet",
    )?;

    let checkpoint_one_file = log_root
        .join("00000000000000000001.checkpoint.parquet")?
        .to_string();

    let v2_checkpoint_read_schema = get_log_schema().project(&[METADATA_NAME])?;

    let mut iter = LogSegment::create_checkpoint_stream(
        &engine,
        v2_checkpoint_read_schema.clone(),
        None,
        vec![create_log_path(&checkpoint_one_file)],
        log_root,
    )?;

    // Assert that the first batch returned is from reading checkpoint file 1
    let (first_batch, is_log_batch) = iter.next().unwrap()?;
    assert!(!is_log_batch);
    assert_batch_matches(
        first_batch,
        sidecar_batch_with_given_paths(vec!["sidecar1.parquet"], v2_checkpoint_read_schema.clone()),
    );
    assert!(iter.next().is_none());

    Ok(())
}

#[test]
fn test_create_checkpoint_stream_returns_checkpoint_batches_if_checkpoint_is_multi_part(
) -> DeltaResult<()> {
    let (store, log_root) = new_in_memory_store();
    let engine = DefaultEngine::new(
        store.clone(),
        Path::from("/"),
        Arc::new(TokioBackgroundExecutor::new()),
    );

    // Multi-part checkpoints can never have sidecar actions.
    // We place batches with sidecar actions in multi-part checkpoints to verify we do not read the actions, as we
    // should instead short-circuit and return the checkpoint batches as-is when encountering multi-part checkpoints.
    let checkpoint_part_1 = "00000000000000000001.checkpoint.0000000001.0000000002.parquet";
    let checkpoint_part_2 = "00000000000000000001.checkpoint.0000000002.0000000002.parquet";

    add_checkpoint_to_store(
        &store,
        sidecar_batch_with_given_paths(vec!["sidecar1.parquet"], get_log_schema().clone()),
        checkpoint_part_1,
    )?;
    add_checkpoint_to_store(
        &store,
        sidecar_batch_with_given_paths(vec!["sidecar2.parquet"], get_log_schema().clone()),
        checkpoint_part_2,
    )?;

    let checkpoint_one_file = log_root.join(checkpoint_part_1)?.to_string();
    let checkpoint_two_file = log_root.join(checkpoint_part_2)?.to_string();

    let v2_checkpoint_read_schema = get_log_schema().project(&[ADD_NAME, SIDECAR_NAME])?;

    let mut iter = LogSegment::create_checkpoint_stream(
        &engine,
        v2_checkpoint_read_schema.clone(),
        None,
        vec![
            create_log_path(&checkpoint_one_file),
            create_log_path(&checkpoint_two_file),
        ],
        log_root,
    )?;

    // Assert the correctness of batches returned
    for expected_sidecar in ["sidecar1.parquet", "sidecar2.parquet"].iter() {
        let (batch, is_log_batch) = iter.next().unwrap()?;
        assert!(!is_log_batch);
        assert_batch_matches(
            batch,
            sidecar_batch_with_given_paths(
                vec![expected_sidecar],
                v2_checkpoint_read_schema.clone(),
            ),
        );
    }
    assert!(iter.next().is_none());

    Ok(())
}

#[test]
fn test_create_checkpoint_stream_reads_parquet_checkpoint_batch_without_sidecars() -> DeltaResult<()>
{
    let (store, log_root) = new_in_memory_store();
    let engine = DefaultEngine::new(
        store.clone(),
        Path::from("/"),
        Arc::new(TokioBackgroundExecutor::new()),
    );

    add_checkpoint_to_store(
        &store,
        add_batch_simple(get_log_schema().clone()),
        "00000000000000000001.checkpoint.parquet",
    )?;

    let checkpoint_one_file = log_root
        .join("00000000000000000001.checkpoint.parquet")?
        .to_string();

    let v2_checkpoint_read_schema = get_log_schema().project(&[ADD_NAME, SIDECAR_NAME])?;

    let mut iter = LogSegment::create_checkpoint_stream(
        &engine,
        v2_checkpoint_read_schema.clone(),
        None,
        vec![create_log_path(&checkpoint_one_file)],
        log_root,
    )?;

    // Assert that the first batch returned is from reading checkpoint file 1
    let (first_batch, is_log_batch) = iter.next().unwrap()?;
    assert!(!is_log_batch);
    assert_batch_matches(
        first_batch,
        add_batch_simple(v2_checkpoint_read_schema.clone()),
    );
    assert!(iter.next().is_none());

    Ok(())
}

#[test]
fn test_create_checkpoint_stream_reads_json_checkpoint_batch_without_sidecars() -> DeltaResult<()> {
    let (store, log_root) = new_in_memory_store();
    let engine = DefaultEngine::new(
        store.clone(),
        Path::from("/"),
        Arc::new(TokioBackgroundExecutor::new()),
    );

    write_json_to_store(
        &store,
        vec![Action::Add(Add {
            path: "fake_path_1".into(),
            data_change: true,
            ..Default::default()
        })],
        "00000000000000000001.checkpoint.json",
    )?;

    let checkpoint_one_file = log_root
        .join("00000000000000000001.checkpoint.json")?
        .to_string();

    let v2_checkpoint_read_schema = get_log_schema().project(&[ADD_NAME, SIDECAR_NAME])?;

    let mut iter = LogSegment::create_checkpoint_stream(
        &engine,
        v2_checkpoint_read_schema.clone(),
        None,
        vec![create_log_path(&checkpoint_one_file)],
        log_root,
    )?;

    // Assert that the first batch returned is from reading checkpoint file 1
    let (first_batch, is_log_batch) = iter.next().unwrap()?;
    assert!(!is_log_batch);
    let mut visitor = AddVisitor::default();
    visitor.visit_rows_of(&*first_batch)?;
    assert!(visitor.adds.len() == 1);
    assert!(visitor.adds[0].path == "fake_path_1");

    assert!(iter.next().is_none());

    Ok(())
}

// Tests the end-to-end process of creating a checkpoint stream.
// Verifies that:
// - The checkpoint file is read and produces batches containing references to sidecar files.
// - As sidecar references are present, the corresponding sidecar files are processed correctly.
// - Batches from both the checkpoint file and sidecar files are returned.
// - Each returned batch is correctly flagged with is_log_batch set to false
#[test]
fn test_create_checkpoint_stream_reads_checkpoint_file_and_returns_sidecar_batches(
) -> DeltaResult<()> {
    let (store, log_root) = new_in_memory_store();
    let engine = DefaultEngine::new(
        store.clone(),
        Path::from("/"),
        Arc::new(TokioBackgroundExecutor::new()),
    );

    add_checkpoint_to_store(
        &store,
        sidecar_batch_with_given_paths(
            vec!["sidecarfile1.parquet", "sidecarfile2.parquet"],
            get_log_schema().clone(),
        ),
        "00000000000000000001.checkpoint.parquet",
    )?;

    add_sidecar_to_store(
        &store,
        add_batch_simple(get_log_schema().project(&[ADD_NAME, REMOVE_NAME])?),
        "sidecarfile1.parquet",
    )?;
    add_sidecar_to_store(
        &store,
        add_batch_with_remove(get_log_schema().project(&[ADD_NAME, REMOVE_NAME])?),
        "sidecarfile2.parquet",
    )?;

    let checkpoint_file_path = log_root
        .join("00000000000000000001.checkpoint.parquet")?
        .to_string();

    let v2_checkpoint_read_schema = get_log_schema().project(&[ADD_NAME, SIDECAR_NAME])?;

    let mut iter = LogSegment::create_checkpoint_stream(
        &engine,
        v2_checkpoint_read_schema.clone(),
        None,
        vec![create_log_path(&checkpoint_file_path)],
        log_root,
    )?;

    // Assert that the first batch returned is from reading checkpoint file 1
    let (first_batch, is_log_batch) = iter.next().unwrap()?;
    assert!(!is_log_batch);
    assert_batch_matches(
        first_batch,
        sidecar_batch_with_given_paths(
            vec!["sidecarfile1.parquet", "sidecarfile2.parquet"],
            get_log_schema().project(&[ADD_NAME, SIDECAR_NAME])?,
        ),
    );
    // Assert that the second batch returned is from reading sidecarfile1
    let sidecar_schema = get_log_add_schema();
    let (second_batch, is_log_batch) = iter.next().unwrap()?;
    assert!(!is_log_batch);
    assert_batch_matches(second_batch, add_batch_simple(sidecar_schema.clone()));

    // Assert that the second batch returned is from reading sidecarfile2
    let (third_batch, is_log_batch) = iter.next().unwrap()?;
    assert!(!is_log_batch);
    assert_batch_matches(third_batch, add_batch_with_remove(sidecar_schema.clone()));

    assert!(iter.next().is_none());

    Ok(())
}
