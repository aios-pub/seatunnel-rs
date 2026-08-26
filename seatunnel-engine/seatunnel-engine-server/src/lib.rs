/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */

//! Engine Server: Master node + Worker node implementation.
//!
//! - Master: worker registry, job scheduling, heartbeat-driven task dispatch
//! - Worker: connector execution, checkpointing, status reporting

pub mod client_handler;
pub mod job_coordinator;
pub mod master;
pub mod server_config;
pub mod state_store;
pub mod worker;

pub use client_handler::ClientHandler;
pub use job_coordinator::JobCoordinator;
pub use master::{new_worker_registry, MasterHandler, WorkerEntry, WorkerRegistry};
pub use state_store::LocalStateStore;
pub use worker::WorkerNode;
