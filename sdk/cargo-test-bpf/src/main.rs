use clap::{
    crate_description, crate_name, crate_version, value_t, values_t, App, AppSettings, Arg,
};
use std::{
    env,
    ffi::OsStr,
    fs::File,
    io::{prelude::*, BufWriter},
    path::{Path, PathBuf},
    process::exit,
    process::Command,
};

struct Config {
    bpf_sdk: Option<String>,
    bpf_out_dir: Option<String>,
    cargo: PathBuf,
    cargo_build_bpf: PathBuf,
    extra_cargo_test_args: Vec<String>,
    features: Vec<String>,
    test_name: Option<String>,
    no_default_features: bool,
    no_run: bool,
    offline: bool,
    verbose: bool,
    workspace: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bpf_sdk: None,
            bpf_out_dir: None,
            cargo: PathBuf::from("cargo"),
            cargo_build_bpf: PathBuf::from("cargo-build-bpf"),
            extra_cargo_test_args: vec![],
            features: vec![],
            test_name: None,
            no_default_features: false,
            no_run: false,
            offline: false,
            verbose: false,
            workspace: false,
        }
    }
}

fn spawn<I, S>(program: &Path, args: I)
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let args = args.into_iter().collect::<Vec<_>>();
    print!("CARGO-TEST-BPF Running: {}", program.display());
    for arg in args.iter() {
        print!(" {}", arg.as_ref().to_str().unwrap_or("?"));
    }
    println!();

    let mut child = Command::new(program)
        .args(&args)
        .spawn()
        .unwrap_or_else(|err| {
            eprintln!("Failed to execute {}: {}", program.display(), err);
            exit(1);
        });

    let exit_status = child.wait().expect("failed to wait on child");
    if !exit_status.success() {
        eprintln!("CARGO-TEST-BPF exited on command execution failure.");
        let rerun = File::create("rerun-script.sh").unwrap();
        let mut out = BufWriter::new(rerun);
        for (key, value) in env::vars() {
            writeln!(out, "{}=\"{}\" \\", key, value).unwrap();
        }
        write!(out, "{}", program.display()).unwrap();
        for arg in args.iter() {
            write!(out, " {}", arg.as_ref().to_str().unwrap_or("?")).unwrap();
        }
        writeln!(out).unwrap();
        out.flush().unwrap();
        eprintln!("To rerun the failed command for debugging use rerun-script.sh");
        exit(1);
    }
}

fn test_bpf_package(config: &Config, target_directory: &Path, package: &cargo_metadata::Package) {
    let set_test_bpf_feature = package.features.contains_key("test-bpf");

    let bpf_out_dir = config
        .bpf_out_dir
        .as_ref()
        .cloned()
        .unwrap_or_else(|| format!("{}", target_directory.join("deploy").display()));

    let manifest_path = format!("{}", package.manifest_path);
    let mut cargo_args = vec!["--manifest-path", &manifest_path];
    if config.no_default_features {
        cargo_args.push("--no-default-features");
    }
    for feature in &config.features {
        cargo_args.push("--features");
        cargo_args.push(feature);
    }
    if config.verbose {
        cargo_args.push("--verbose");
    }

    let mut build_bpf_args = cargo_args.clone();
    if let Some(bpf_sdk) = config.bpf_sdk.as_ref() {
        build_bpf_args.push("--bpf-sdk");
        build_bpf_args.push(bpf_sdk);
    }
    build_bpf_args.push("--bpf-out-dir");
    build_bpf_args.push(&bpf_out_dir);

    spawn(&config.cargo_build_bpf, &build_bpf_args);

    // Pass --bpf-out-dir along to the solana-program-test crate
    env::set_var("BPF_OUT_DIR", bpf_out_dir);

    cargo_args.insert(0, "test");

    if let Some(test_name) = &config.test_name {
        cargo_args.push("--test");
        cargo_args.push(test_name);
    }

    if config.no_run {
        cargo_args.push("--no-run");
    }

    // If the program crate declares the "test-bpf" feature, pass it along to the tests so they can
    // distinguish between `cargo test` and `cargo test-bpf`
    if set_test_bpf_feature {
        cargo_args.push("--features");
        cargo_args.push("test-bpf");
    }
    for extra_cargo_test_arg in &config.extra_cargo_test_args {
        cargo_args.push(extra_cargo_test_arg);
    }
    spawn(&config.cargo, &cargo_args);
}

fn test_bpf(config: Config, manifest_path: Option<PathBuf>) {
    let mut metadata_command = cargo_metadata::MetadataCommand::new();
    if let Some(manifest_path) = manifest_path.as_ref() {
        metadata_command.manifest_path(manifest_path);
    }
    if config.offline {
        metadata_command.other_options(vec!["--offline".to_string()]);
    }

    let metadata = metadata_command.exec().unwrap_or_else(|err| {
        eprintln!("Failed to obtain package metadata: {}", err);
        exit(1);
    });

    if let Some(root_package) = metadata.root_package() {
        if !config.workspace {
            test_bpf_package(&config, metadata.target_directory.as_ref(), root_package);
            return;
        }
    }

    let all_bpf_packages = metadata
        .packages
        .iter()
        .filter(|package| {
            if metadata.workspace_members.contains(&package.id) {
                for target in package.targets.iter() {
                    if target.kind.contains(&"cdylib".to_string()) {
                        return true;
                    }
                }
            }
            false
        })
        .collect::<Vec<_>>();

    for package in all_bpf_packages {
        test_bpf_package(&config, metadata.target_directory.as_ref(), package);
    }
}

fn main() {
    let mut args = env::args().collect::<Vec<_>>();
    // When run as a cargo subcommand, the first program argument is the subcommand name.
    // Remove it
    if let Some(arg1) = args.get(1) {
        if arg1 == "test-bpf" {
            args.remove(1);
        }
    }

    let em_dash = "--".to_string();
    let args_contain_dashash = args.contains(&em_dash);

    let matches = App::new(crate_name!())
        .about(crate_description!())
        .version(crate_version!())
        .setting(AppSettings::TrailingVarArg)
        .arg(
            Arg::with_name("bpf_sdk")
                .long("bpf-sdk")
                .value_name("PATH")
                .takes_value(true)
                .help("Path to the Solana BPF SDK"),
        )
        .arg(
            Arg::with_name("features")
                .long("features")
                .value_name("FEATURES")
                .takes_value(true)
                .multiple(true)
                .help("Space-separated list of features to activate"),
        )
        .arg(
            Arg::with_name("no_default_features")
                .long("no-default-features")
                .takes_value(false)
                .help("Do not activate the `default` feature"),
        )
        .arg(
            Arg::with_name("test")
                .long("test")
                .value_name("NAME")
                .takes_value(true)
                .help("Test only the specified test target"),
        )
        .arg(
            Arg::with_name("manifest_path")
                .long("manifest-path")
                .value_name("PATH")
                .takes_value(true)
                .help("Path to Cargo.toml"),
        )
        .arg(
            Arg::with_name("bpf_out_dir")
                .long("bpf-out-dir")
                .value_name("DIRECTORY")
                .takes_value(true)
                .help("Place final BPF build artifacts in this directory"),
        )
        .arg(
            Arg::with_name("no_run")
                .long("no-run")
                .takes_value(false)
                .help("Compile, but don't run tests"),
        )
        .arg(
            Arg::with_name("offline")
                .long("offline")
                .takes_value(false)
                .help("Run without accessing the network"),
        )
        .arg(
            Arg::with_name("verbose")
                .short("v")
                .long("verbose")
                .takes_value(false)
                .help("Use verbose output"),
        )
        .arg(
            Arg::with_name("workspace")
                .long("workspace")
                .takes_value(false)
                .alias("all")
                .help("Test all BPF packages in the workspace"),
        )
        .arg(
            Arg::with_name("extra_cargo_test_args")
                .value_name("extra args for cargo test and the test binary")
                .index(1)
                .multiple(true)
                .help("All extra arguments are passed through to cargo test"),
        )
        .get_matches_from(args);

    let mut config = Config {
        bpf_sdk: value_t!(matches, "bpf_sdk", String).ok(),
        bpf_out_dir: value_t!(matches, "bpf_out_dir", String).ok(),
        extra_cargo_test_args: values_t!(matches, "extra_cargo_test_args", String)
            .ok()
            .unwrap_or_else(Vec::new),
        features: values_t!(matches, "features", String)
            .ok()
            .unwrap_or_else(Vec::new),
        test_name: value_t!(matches, "test", String).ok(),
        no_default_features: matches.is_present("no_default_features"),
        no_run: matches.is_present("no_run"),
        offline: matches.is_present("offline"),
        verbose: matches.is_present("verbose"),
        workspace: matches.is_present("workspace"),
        ..Config::default()
    };

    if let Ok(cargo_build_bpf) = env::var("CARGO_BUILD_BPF") {
        config.cargo_build_bpf = PathBuf::from(cargo_build_bpf);
    }
    if let Ok(cargo_build_bpf) = env::var("CARGO") {
        config.cargo = PathBuf::from(cargo_build_bpf);
    }

    // clap.rs swallows "--" in the case when the user provides it as the first `extra_cargo_test_args`
    //
    // For example, this command-line "cargo-test-bpf -- --nocapture" results in `extra_cargo_test_args` only
    // containing "--nocapture".  This is a problem because `cargo test` will never see the `--`.
    //
    // Whereas "cargo-test-bpf testname --  --nocapture" correctly produces a `extra_cargo_test_args`
    // with "testname -- --nocapture".
    //
    // So if the original cargo-test-bpf arguments contain "--" but `extra_cargo_test_args` does
    // not, then prepend "--".
    //
    if args_contain_dashash && !config.extra_cargo_test_args.contains(&em_dash) {
        config.extra_cargo_test_args.insert(0, em_dash);
    }

    let manifest_path = value_t!(matches, "manifest_path", PathBuf).ok();
    test_bpf(config, manifest_path);
}
