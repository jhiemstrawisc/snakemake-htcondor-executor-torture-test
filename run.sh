#!/bin/bash
set -e

# =============================================================================
# HTCondor Executor Torture Test Runner
# =============================================================================
# Runs the full integration test workflow and performs post-run verification.
#
# What this script does:
#   1. Exports TORTURE_TEST_VAR for the env var injection test
#   2. Runs the workflow with --envvars to pass the variable to EPs
#   3. Verifies re-run idempotency (--dryrun should find nothing to do)
#   4. Verifies htcondor_transfer_output_files sidecar files exist
#   5. Verifies shared-FS output files exist on the AP
# =============================================================================

# Shared filesystem prefix — files under this path are accessible on both AP
# and EP without HTCondor file transfer.  Adjust if your mount differs.
SHARED_FS_PREFIX="/staging/jhiemstra"
SHARED_FS_TEST_DIR="${SHARED_FS_PREFIX}/torture-test"

# Export the test env var.  The env_var_check rule on the EP will verify this
# arrives via HTCondor's native `environment` key (not inline in arguments).
export TORTURE_TEST_VAR="hello_from_ap"

echo "=== Starting torture test workflow ==="
echo ""

snakemake \
    --jobs 10 \
    --executor htcondor \
    --htcondor-jobdir logs \
    --shared-fs-usage none \
    --htcondor-shared-fs-prefixes "$SHARED_FS_PREFIX" \
    --verbose \
    --envvars TORTURE_TEST_VAR

echo ""
echo "=== Workflow completed successfully ==="

# -----------------------------------------------------------------------------
# Post-run verification 1: Re-run idempotency
# -----------------------------------------------------------------------------
# A dry-run immediately after a successful run should report "Nothing to be
# done."  If the executor stomped mtimes during output transfer (the pre-fix
# bug), Snakemake will want to re-run rules — which means the fix is broken.
#
# We use --rerun-triggers mtime to scope the check to what the executor can
# actually break: mtime-based dependency ordering.  Without this flag,
# Snakemake's provenance tracking ("Code has changed since last execution")
# would fire whenever the Snakefile is edited between runs — that's a core
# Snakemake feature, not an executor concern.
echo ""
echo "=== Re-run idempotency check ==="

DRYRUN_OUTPUT=$(snakemake \
    --jobs 10 \
    --executor htcondor \
    --htcondor-jobdir logs \
    --shared-fs-usage none \
    --htcondor-shared-fs-prefixes "$SHARED_FS_PREFIX" \
    --rerun-triggers mtime \
    --dryrun 2>&1) || true

if echo "$DRYRUN_OUTPUT" | grep -qi "nothing to be done"; then
    echo "PASS: dry-run reports nothing to be done"
else
    echo "FAIL: dry-run wants to re-run rules:"
    echo "$DRYRUN_OUTPUT"
    exit 1
fi

# -----------------------------------------------------------------------------
# Post-run verification 2: htcondor_transfer_output_files sidecar check
# -----------------------------------------------------------------------------
# The extra_output_check rule writes a sidecar file that is NOT a declared
# Snakemake output but IS declared via htcondor_transfer_output_files.
# Verify that HTCondor transferred it back to the AP.
echo ""
echo "=== htcondor_transfer_output_files sidecar check ==="

SIDECAR_PASS=true
for sample in sample1 sample2; do
    sidecar="output/${sample}_extra_sidecar.txt"
    if [ -f "$sidecar" ]; then
        echo "PASS: $sidecar exists"
    else
        echo "FAIL: $sidecar missing — htcondor_transfer_output_files may be broken"
        SIDECAR_PASS=false
    fi
done

if [ "$SIDECAR_PASS" = false ]; then
    echo ""
    echo "htcondor_transfer_output_files test FAILED"
    exit 1
fi

# -----------------------------------------------------------------------------
# Post-run verification 3: Shared filesystem output check
# -----------------------------------------------------------------------------
# The shared_fs_write rule writes output directly to /staging/jhiemstra/
# (the shared mount).  Since the executor excludes shared-FS paths from
# transfer_output_files, the file must have been written directly by the EP.
# Verify it exists on the AP (which can see the same shared mount).
echo ""
echo "=== Shared filesystem output check ==="

SHARED_FS_PASS=true
for sample in sample1 sample2; do
    shared_file="${SHARED_FS_TEST_DIR}/${sample}_shared_data.txt"
    if [ -f "$shared_file" ]; then
        echo "PASS: $shared_file exists on shared FS"
    else
        echo "FAIL: $shared_file missing — shared-FS output may have been lost"
        SHARED_FS_PASS=false
    fi
done

if [ "$SHARED_FS_PASS" = false ]; then
    echo ""
    echo "Shared filesystem output test FAILED"
    exit 1
fi

echo ""
echo "=== All checks passed ==="
