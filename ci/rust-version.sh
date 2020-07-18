#
# This file maintains the rust versions for use by CI.
#
# Obtain the environment variables without any automatic toolchain updating:
#   $ source ci/rust-version.sh
#
# Obtain the environment variables updating both stable and nightly, only stable, or
# only nightly:
#   $ source ci/rust-version.sh all
#   $ source ci/rust-version.sh stable
#   $ source ci/rust-version.sh nightly

# Then to build with either stable or nightly:
#   $ cargo +"$rust_stable" build
#   $ cargo +"$rust_nightly" build
#

if [[ -n $RUST_STABLE_VERSION ]]; then
  stable_version="$RUST_STABLE_VERSION"
else
<<<<<<< HEAD
  stable_version=1.43.0
=======
  stable_version=1.45.0
>>>>>>> 83cc92f3f... Upgade to Rust 1.45.0
fi

if [[ -n $RUST_NIGHTLY_VERSION ]]; then
  nightly_version="$RUST_NIGHTLY_VERSION"
else
  nightly_version=2020-04-23
fi


export rust_stable="$stable_version"
export rust_stable_docker_image=solanalabs/rust:"$stable_version"

export rust_nightly=nightly-"$nightly_version"
export rust_nightly_docker_image=solanalabs/rust-nightly:"$nightly_version"

[[ -z $1 ]] || (

  rustup_install() {
    declare toolchain=$1
    if ! cargo +"$toolchain" -V; then
      rustup install "$toolchain"
      cargo +"$toolchain" -V
    fi
  }

  set -e
  cd "$(dirname "${BASH_SOURCE[0]}")"
  case $1 in
  stable)
     rustup_install "$rust_stable"
     ;;
  nightly)
     rustup_install "$rust_nightly"
    ;;
  all)
     rustup_install "$rust_stable"
     rustup_install "$rust_nightly"
    ;;
  *)
    echo "Note: ignoring unknown argument: $1"
    ;;
  esac
)
