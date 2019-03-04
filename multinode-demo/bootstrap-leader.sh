#!/usr/bin/env bash
#
# Start the bootstrap leader node
#

here=$(dirname "$0")
# shellcheck source=multinode-demo/common.sh
source "$here"/common.sh

# shellcheck source=scripts/oom-score-adj.sh
source "$here"/../scripts/oom-score-adj.sh

[[ -f "$SOLANA_CONFIG_DIR"/bootstrap-leader.json ]] || {
  echo "$SOLANA_CONFIG_DIR/bootstrap-leader.json not found, create it by running:"
  echo
  echo "  ${here}/setup.sh"
  exit 1
}

if [[ -n "$SOLANA_CUDA" ]]; then
  program="$solana_fullnode_cuda"
else
  program="$solana_fullnode"
fi

maybe_blockstream=
maybe_init_complete_file=
maybe_no_leader_rotation=

while [[ -n $1 ]]; do
  if [[ $1 = --init-complete-file ]]; then
    maybe_init_complete_file="--init-complete-file $2"
    shift 2
  elif [[ $1 = --blockstream ]]; then
    maybe_blockstream="$1 $2"
    shift 2
  elif [[ $1 = --no-leader-rotation ]]; then
    maybe_no_leader_rotation="--no-leader-rotation"
    shift
  else
    echo "Unknown argument: $1"
    exit 1
  fi
done

tune_system

trap 'kill "$pid" && wait "$pid"' INT TERM
$solana_ledger_tool --ledger "$SOLANA_CONFIG_DIR"/bootstrap-leader-ledger verify

# shellcheck disable=SC2086 # Don't want to double quote maybe_blockstream or maybe_init_complete_file
$program \
  $maybe_blockstream \
  $maybe_init_complete_file \
  $maybe_no_leader_rotation \
  --identity "$SOLANA_CONFIG_DIR"/bootstrap-leader.json \
  --ledger "$SOLANA_CONFIG_DIR"/bootstrap-leader-ledger \
  --accounts "$SOLANA_CONFIG_DIR"/bootstrap-leader-accounts \
  --rpc-port 8899 \
  > >($bootstrap_leader_logger) 2>&1 &
pid=$!
oom_score_adj "$pid" 1000
wait "$pid"
