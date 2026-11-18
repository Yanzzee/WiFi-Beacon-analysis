#!/bin/bash
# Keep capture_common.sh beside this script when copying it to a Linux host.
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd) || exit 1
source "$SCRIPT_DIR/capture_common.sh" || exit 1
run_direct_capture all "$@"
