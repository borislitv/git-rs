#!/usr/bin/env bash
# Hosted image build controller. Only disposable runner slices belong here.
set -euo pipefail

phase=$1 action=$2
case "$phase" in
    inputs) targets=(codec-base toolchain build-inputs) ;;
    final) targets=(test-fixture-build test release test-server) ;;
    *) exit 2 ;;
esac
tag="${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}"
[[ $tag =~ ^[0-9]+-[0-9]+$ ]]
slice="git-rs-blame-images-$phase-$tag.slice"
directory="$GITHUB_WORKSPACE/.local/blame-images"
build_options=(--prepared "$GITHUB_WORKSPACE/.local/blame-image-inputs")
if [[ $phase == inputs ]]; then
    directory="$GITHUB_WORKSPACE/.local/blame-image-inputs"
    build_options=(--azure-archive)
fi

case "$action" in
    build)
        cpus=$(python3 -c 'import os; print(",".join(map(str, sorted(os.sched_getaffinity(0))[:2])))')
        sudo tee "/run/systemd/system/$slice" > /dev/null <<EOF
[Unit]
Description=Owned prebuilt image workers
[Slice]
CPUQuota=200%
AllowedCPUs=$cpus
MemoryMax=4G
MemorySwapMax=0
TasksMax=256
EOF
        sudo systemctl daemon-reload
        for target in "${targets[@]}"; do
            unit="git-rs-blame-images-$target-$tag.service"
            sudo systemd-run --wait --pipe --unit="$unit" --slice="$slice" \
                --working-directory="$GITHUB_WORKSPACE" \
                --property="User=$(id -un)" --property="Group=$(id -gn)" \
                --property=RuntimeMaxSec=900 --property=TimeoutStopSec=40 \
                --property=KillMode=control-group --property="AllowedCPUs=$cpus" \
                --property="ExecStopPost=/usr/bin/python3 -B $GITHUB_WORKSPACE/tests/blame_images.py cleanup --slice $slice" \
                /usr/bin/python3 -B "$GITHUB_WORKSPACE/tests/blame_images.py" build \
                    --target "$target" --tag "git-rs:blame-$target" --unit "$unit" --slice "$slice" \
                    --directory "$directory" "${build_options[@]}"
        done
        ;;
    cleanup)
        if [[ ! -f /run/systemd/system/$slice ]]; then
            exit 0
        fi
        for target in "${targets[@]}"; do
            unit="git-rs-blame-images-$target-$tag.service"
            if [[ $(systemctl show "$unit" --property=LoadState --value) != not-found ]]; then
                sudo systemctl stop "$unit" || [[ $(systemctl show "$unit" --property=LoadState --value) == not-found ]]
            fi
        done
        python3 -B "$GITHUB_WORKSPACE/tests/blame_images.py" cleanup --slice "$slice"
        sudo systemctl stop "$slice"
        sudo rm -f -- "/run/systemd/system/$slice"
        sudo systemctl daemon-reload
        ;;
    *) exit 2 ;;
esac
