extern crate walkdir;

use std::{env, path::Path, process::Command};
use walkdir::WalkDir;

fn rerun_if_changed(files: &[&str], directories: &[&str], excludes: &[&str]) {
    let mut all_files: Vec<_> = files.iter().map(|f| f.to_string()).collect();

    for directory in directories {
        let files_in_directory: Vec<_> = WalkDir::new(directory)
            .into_iter()
            .map(|entry| entry.unwrap())
            .filter(|entry| {
                if !entry.file_type().is_file() {
                    return false;
                }
                for exclude in excludes.iter() {
                    if entry.path().to_str().unwrap().contains(exclude) {
                        return false;
                    }
                }
                true
            })
            .map(|f| f.path().to_str().unwrap().to_owned())
            .collect();
        all_files.extend_from_slice(&files_in_directory[..]);
    }

    for file in all_files {
        if !Path::new(&file).is_file() {
            panic!("{} is not a file", file);
        }
        println!("cargo:rerun-if-changed={}", file);
    }
}

fn main() {
    let bpf_c = env::var("CARGO_FEATURE_BPF_C").is_ok();
    if bpf_c {
        let install_dir =
            "OUT_DIR=../target/".to_string() + &env::var("PROFILE").unwrap() + &"/bpf".to_string();

        println!("cargo:warning=(not a warning) Building C-based BPF programs");
        assert!(Command::new("make")
            .current_dir("c")
            .arg("programs")
            .arg(&install_dir)
            .status()
            .expect("Failed to build C-based BPF programs")
            .success());

        rerun_if_changed(&["c/makefile"], &["c/src", "../../sdk"], &["/target/"]);
    }

    let bpf_rust = env::var("CARGO_FEATURE_BPF_RUST").is_ok();
    if bpf_rust {
        let install_dir =
            "target/".to_string() + &env::var("PROFILE").unwrap() + &"/bpf".to_string();

        let rust_programs = [
            "128bit",
            "alloc",
            "call_depth",
            "caller_access",
            "custom_heap",
            "dep_crate",
            "deprecated_loader",
            "dup_accounts",
            "error_handling",
            "external_spend",
            "finalize",
            "instruction_introspection",
            "invoke",
            "invoke_and_error",
            "invoke_and_ok",
            "invoke_and_return",
            "invoked",
            "iter",
            "many_args",
            "mem",
            "membuiltins",
            "noop",
            "panic",
            "param_passing",
            "rand",
            "ro_modify",
            "ro_account_modify",
            "sanity",
            "ed25519_sig_check",
            "sha",
            "spoof1",
            "spoof1_system",
            "upgradeable",
            "upgraded",
        ];
        for program in rust_programs.iter() {
            println!(
                "cargo:warning=(not a warning) Building Rust-based BPF programs: solana_bpf_rust_{}",
                program
            );
            assert!(Command::new("../../cargo-build-bpf")
                .args(&[
                    "--manifest-path",
                    &format!("rust/{}/Cargo.toml", program),
                    "--bpf-out-dir",
                    &install_dir
                ])
                .status()
                .expect("Error calling cargo-build-bpf from build.rs")
                .success());
        }

        rerun_if_changed(&[], &["rust", "../../sdk", &install_dir], &["/target/"]);
    }
}
