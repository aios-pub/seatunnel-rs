/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */

//! Engine Server: Master node + Worker node implementation.
//!
//! - Master: leader election, job scheduling, worker registration, heartbeat handling
//! - Worker: task execution, heartbeat reporting, status reporting

pub mod job_manager;
pub mod leader_election;
pub mod master;
pub mod resource_manager;
pub mod worker;
pub use client_handler::ClientHandler;
pub use job_manager::JobManager;
pub use master::MasterHandler;
pub mod client_handler;
