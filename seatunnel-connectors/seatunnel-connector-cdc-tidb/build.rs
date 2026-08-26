/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! Build script: generates kvproto Rust types (cdcpb, pdpb, metapb, ...)
//! from vendored .proto files using prost-build + tonic-build.

use std::path::{Path, PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto");

    let proto_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("proto");
    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);

    // All protos needed for TiKV CDC EventFeed + PD region watching.
    // `compile_protos` resolves transitive imports automatically.
    // Note: eraftpb.proto (under proto/include/) is pulled in transitively
    // by raft_cmdpb.proto, so it must NOT be listed here explicitly.
    let protos: Vec<PathBuf> = [
        "cdcpb.proto",
        "pdpb.proto",
        "tsopb.proto",
        "metapb.proto",
        "errorpb.proto",
        "kvrpcpb.proto",
        "raft_cmdpb.proto",
        "raft_serverpb.proto",
        "import_sstpb.proto",
        "deadlock.proto",
        "tracepb.proto",
        "resource_manager.proto",
        "encryptionpb.proto",
        "replication_modepb.proto",
        "gcpb.proto",
        "logbackuppb.proto",
        "meta_storagepb.proto",
        "recoverdatapb.proto",
        "resource_usage_agent.proto",
        "schedulingpb.proto",
        "keyspacepb.proto",
        "autoid.proto",
        "brpb.proto",
        "configpb.proto",
        "coprocessor.proto",
        "debugpb.proto",
        "disaggregated.proto",
        "disk_usage.proto",
        "enginepb.proto",
        "import_kvpb.proto",
        "mpp.proto",
        "tikvpb.proto",
    ]
    .map(|p| proto_dir.join(p))
    .to_vec();

    // Include paths: proto dir for inter-proto imports, proto/include for
    // gogoproto/gogo.proto and rustproto.proto custom options.
    let includes =
        ["proto", "proto/include"].map(|p| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(p));

    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .out_dir(&out_dir)
        .compile_protos(&protos, &includes)?;

    Ok(())
}

// Keep a reference to Path so imports resolve even if unused above.
#[allow(dead_code)]
fn _assert_path(_: &Path) {}
