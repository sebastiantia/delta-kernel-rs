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
//! TODO!(seb): API is a WIP
//! The API follows a builder pattern using `CheckpointBuilder`, which performs tbale feature
//! detection and configuration validation. Depending on table features and builder options:
//!
//! - Without `v2Checkpoints`: produces a **Classic-named V1** checkpoint.
//! - With `v2Checkpoints`: produces a **UUID-named V2** checkpoint.
//! - With `v2Checkpoints` + `.classic_naming()`: produces a **Classic-named V2** checkpoint.
//!
//! The builder returns the `CheckpointWriter` which is responsible for:
//! - Producing the correct set of actions to be written to the checkpoint file when
//!   `.get_checkpoint_info()` is called.
//! - Writing the _last_checkpoint file when `.finalize_checkpoint()` is called.
//!
//! Notes:
//! - Multi-file V2 checkpoints are not supported yet. The API is designed to be extensible for future
//!   multi-file support, but the current implementation only supports single-file checkpoints.
//! - Multi-file V1 checkpoints are DEPRECATED.
//!
//! ## Example: Writing a classic-named V1/V2 checkpoint (depending on `v2Checkpoints` feature support)
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
//! let writer = builder.with_classic_naming();
//!
//! // Build the checkpoint writer
//! let mut writer = builder.build(&engine)?;
//!
//! // Retrieve checkpoint data (ensuring single consumption)
//! let checkpoint_data = writer.get_checkpoint_info()?;
//!
//! /* Write checkpoint data to file and collect metadata before finalizing */
//!
//! // Write the _last_checkpoint file
//! writer.finalize_checkpoint(&engine, &checkpoint_metadata)?;
//! ```
//!
//! This module, along with its submodule `checkpoint/log_replay.rs`, provides the full
//! API and implementation for generating checkpoints. See `checkpoint/log_replay.rs` for details
//! on how log replay is used to filter and deduplicate actions for checkpoint creation.
mod log_replay;
