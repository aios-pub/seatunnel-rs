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

//! Copy the hybrid+web startup scripts next to the compiled binaries, so
//! `target/<profile>` doubles as a self-contained deployment package: the
//! scripts detect the binaries beside them (package mode) and run from the
//! current directory with plain `./` relative paths — no cargo, no repo.

use std::path::PathBuf;

fn main() {
    let manifest_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    let out_dir = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    // OUT_DIR = <target>/<profile>/build/<pkg>-<hash>/out; three levels up
    // is the profile directory the binaries are linked into.
    let profile_dir = out_dir
        .ancestors()
        .nth(3)
        .expect("OUT_DIR not below target/<profile>")
        .to_path_buf();

    for script in [
        "start-hybrid-web.sh",
        "start-hybrid-web-debug.sh",
        "start-cluster-web.sh",
    ] {
        let src = manifest_dir.join("../../scripts").join(script);
        println!("cargo:rerun-if-changed={}", src.display());
        let dst = profile_dir.join(script);
        std::fs::copy(&src, &dst)
            .unwrap_or_else(|e| panic!("copy {} failed: {}", src.display(), e));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&dst, std::fs::Permissions::from_mode(0o755))
                .expect("chmod startup script");
        }
    }
}
