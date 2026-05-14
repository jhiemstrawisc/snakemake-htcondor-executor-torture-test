# Snakefile torture test. This file tests:
# - Main Snakefile transfer
# - Job wrapper path handling
# - Script path handling (including wildcard expansion)
# - Shell command handling
# - Input file transfer
# - Output file transfer
# - Additional input file handling via `htcondor_transfer_input_files`
# - Additional output file handling via `htcondor_transfer_output_files`
# - Module Snakefile handling, including nested/recursive modules
# - Group directive (multiple rules executed as single HTCondor job)
#   - Serial grouped jobs (linear chain: A → B → C)
#   - Parallel grouped jobs (fan-out: A splits into B, C, D that run in parallel, then E aggregates)
#   - temp() intermediate within a group (excluded from HTCondor transfer list)
# - Shared-directory mtime correctness (issue #48 regression)
#   - Multiple consecutive rules that read from AND write to the same directory
#   - Verifies that output file transfer does not touch sibling input files
# - Environment variable injection via HTCondor's native `environment` key
#   - Verifies --envvars variables reach the EP when using job_wrapper
# - Partial shared filesystem via --htcondor-shared-fs-prefixes
#   - Files under the shared prefix are NOT transferred by HTCondor
#   - Mixed rules with both shared-FS and local inputs/outputs
# - Multiple samples (concurrent HTCondor jobs for different wildcard values)
# - Re-run idempotency (automated --dryrun verification in run.sh)

SAMPLES = ["sample1", "sample2"]

rule all:
    input:
        expand("output/{sample}_report.txt", sample=SAMPLES),
        expand("output/{sample}_qc.txt", sample=SAMPLES),
        expand("output/{sample}_validated.txt", sample=SAMPLES),
        expand("output/{sample}_grouped_final.txt", sample=SAMPLES),
        expand("output/{sample}_parallel_final.txt", sample=SAMPLES),
        expand("output/{sample}_mtime_check.txt", sample=SAMPLES),
        expand("output/{sample}_env_check.txt", sample=SAMPLES),
        expand("output/{sample}_extra_output_main.txt", sample=SAMPLES),
        expand("output/{sample}_shared_fs_check.txt", sample=SAMPLES)

# Use module for quality checking
module quality_check:
    snakefile: "modules/quality_check/Snakefile"

use rule * from quality_check as qc_*

rule create_data:
    output:
        "data/{sample}_numbers.txt"
    resources:
        container_image="docker://jhiemstra/snakemake-dev-image:v1",
        universe="container",
        request_disk="2GB",
        request_memory="512MB",
        job_wrapper="wrapper.sh",
        stream_output="True",
        stream_error="True"
    shell:
        """
        echo "1" > {output}
        echo "2" >> {output}
        echo "3" >> {output}
        echo "4" >> {output}
        echo "5" >> {output}

        for i in {{1..60}}; do
            timestamp=$(date '+%Y-%m-%d %H:%M:%S')

            echo "hello out ${{timestamp}}"
            echo "hello error ${{timestamp}}" >&2

            sleep 5
        done
        """

rule process_data:
    input:
        "data/{sample}_numbers.txt"
    output:
        "output/{sample}_processed.txt"
    resources:
        container_image="docker://jhiemstra/snakemake-dev-image:v1",
        universe="container",
        request_disk="1GB",
        request_memory="512MB",
        job_wrapper="wrapper.sh",
        htcondor_transfer_input_files="scripts/stats_helpers.py"
    script:
        "scripts/process_{wildcards.sample}.py"

rule make_report:
    input:
        "output/{sample}_processed.txt"
    output:
        "output/{sample}_report.txt"
    resources:
        container_image="docker://jhiemstra/snakemake-dev-image:v1",
        universe="container",
        request_disk="1GB",
        request_memory="512MB",
        job_wrapper="wrapper.sh"
    shell:
        """
        echo "Processing Report for {wildcards.sample}" > {output}
        echo "========================================" >> {output}
        cat {input} >> {output}
        echo "" >> {output}
        echo "Processing complete!" >> {output}
        """

# Grouped rules to test group directive
# These rules will be executed together as a single HTCondor job
rule grouped_step1:
    input:
        "data/{sample}_numbers.txt"
    output:
        # temp() marks this as a temporary file.  Snakemake deletes it on the EP
        # after grouped_step2 consumes it.  The executor must NOT list temp outputs
        # in transfer_output_files, or HTCondor will error when the file is missing
        # at transfer time.  This tests the skip_temp code path in
        # _add_file_if_transferable().
        temp("output/{sample}_grouped_step1.txt")
    group: "data_pipeline"
    resources:
        container_image="docker://jhiemstra/snakemake-dev-image:v1",
        universe="container",
        htcondor_request_disk_mb=1024,
        htcondor_request_mem_mb=512,
        job_wrapper="wrapper.sh"
    shell:
        """
        echo "Grouped Step 1: Doubling values" > {output}
        echo "==============================" >> {output}
        while read num; do
            echo $((num * 2)) >> {output}
        done < {input}
        """

rule grouped_step2:
    input:
        "output/{sample}_grouped_step1.txt"
    output:
        "output/{sample}_grouped_step2.txt"
    group: "data_pipeline"
    resources:
        container_image="docker://jhiemstra/snakemake-dev-image:v1",
        universe="container",
        htcondor_request_disk_mb=2048,
        htcondor_request_mem_mb=512,
        job_wrapper="wrapper.sh",
        htcondor_transfer_input_files="scripts/stats_helpers.py"
    shell:
        """
        echo "Grouped Step 2: Adding header" > {output}
        echo "============================" >> {output}
        cat {input} >> {output}
        """

rule grouped_step3:
    input:
        "output/{sample}_grouped_step2.txt"
    output:
        "output/{sample}_grouped_final.txt"
    group: "data_pipeline"
    resources:
        container_image="docker://jhiemstra/snakemake-dev-image:v1",
        universe="container",
        htcondor_request_disk_mb=1024,
        htcondor_request_mem_mb=1024,
        job_wrapper="wrapper.sh"
    shell:
        """
        echo "Grouped Step 3: Final Summary" > {output}
        echo "=============================" >> {output}
        echo "Pipeline completed successfully" >> {output}
        echo "" >> {output}
        cat {input} >> {output}
        """


# =============================================================================
# PARALLEL GROUPED JOBS
# =============================================================================
# These rules form a fan-out/fan-in pattern within the same group.
# The parallel_split rule creates a file, then three independent rules
# (parallel_worker_a, parallel_worker_b, parallel_worker_c) can run IN PARALLEL,
# and finally parallel_aggregate combines their outputs.
#
# This tests:
# - Resource SUMMING for parallel jobs within a layer (mem, disk, threads)
# - Proper thread/core requests when parallel jobs need multiple cores
#
# Group structure:
#   parallel_split (1 thread, 512MB) 
#         ↓
#   ┌─────┼─────┐
#   ↓     ↓     ↓
#   worker_a (1 thread, 1GB)  |  worker_b (2 threads, 2GB)  |  worker_c (1 thread, 1GB)
#   └─────┼─────┘
#         ↓
#   parallel_aggregate (1 thread, 512MB)
#
# For the parallel layer, Snakemake should SUM:
# - threads: 1 + 2 + 1 = 4 cores
# - memory: 1024 + 2048 + 1024 = 4096 MB = 4GB

rule parallel_split:
    input:
        "data/{sample}_numbers.txt"
    output:
        "output/{sample}_parallel_input.txt"
    group: "parallel_pipeline"
    threads: 1
    resources:
        container_image="docker://jhiemstra/snakemake-dev-image:v1",
        universe="container",
        htcondor_request_disk_mb=512,
        htcondor_request_mem_mb=512,
        job_wrapper="wrapper.sh"
    shell:
        """
        echo "Parallel Split: Preparing data for parallel processing" > {output}
        echo "======================================================" >> {output}
        cat {input} >> {output}
        """

rule parallel_worker_a:
    input:
        "output/{sample}_parallel_input.txt"
    output:
        "output/{sample}_worker_a.txt"
    group: "parallel_pipeline"
    threads: 1
    resources:
        container_image="docker://jhiemstra/snakemake-dev-image:v1",
        universe="container",
        htcondor_request_disk_mb=1024,
        htcondor_request_mem_mb=1024,
        job_wrapper="wrapper.sh"
    shell:
        """
        echo "Worker A: Multiplying by 10" > {output}
        echo "===========================" >> {output}
        # Skip header lines and process numbers
        tail -n +3 {input} | while read num; do
            if [[ "$num" =~ ^[0-9]+$ ]]; then
                echo $((num * 10)) >> {output}
            fi
        done
        """

rule parallel_worker_b:
    input:
        "output/{sample}_parallel_input.txt"
    output:
        "output/{sample}_worker_b.txt"
    group: "parallel_pipeline"
    threads: 2  # This worker needs 2 threads!
    resources:
        container_image="docker://jhiemstra/snakemake-dev-image:v1",
        universe="container",
        htcondor_request_disk_mb=1024,
        htcondor_request_mem_mb=2048,  # Higher memory for this worker
        job_wrapper="wrapper.sh"
    shell:
        """
        echo "Worker B: Multiplying by 100 (using {threads} threads)" > {output}
        echo "=======================================================" >> {output}
        tail -n +3 {input} | while read num; do
            if [[ "$num" =~ ^[0-9]+$ ]]; then
                echo $((num * 100)) >> {output}
            fi
        done
        """

rule parallel_worker_c:
    input:
        "output/{sample}_parallel_input.txt"
    output:
        "output/{sample}_worker_c.txt"
    group: "parallel_pipeline"
    threads: 1
    resources:
        container_image="docker://jhiemstra/snakemake-dev-image:v1",
        universe="container",
        htcondor_request_disk_mb=1024,
        htcondor_request_mem_mb=1024,
        job_wrapper="wrapper.sh"
    shell:
        """
        echo "Worker C: Multiplying by 1000" > {output}
        echo "=============================" >> {output}
        tail -n +3 {input} | while read num; do
            if [[ "$num" =~ ^[0-9]+$ ]]; then
                echo $((num * 1000)) >> {output}
            fi
        done
        """

rule parallel_aggregate:
    input:
        a="output/{sample}_worker_a.txt",
        b="output/{sample}_worker_b.txt",
        c="output/{sample}_worker_c.txt"
    output:
        "output/{sample}_parallel_final.txt"
    group: "parallel_pipeline"
    threads: 1
    resources:
        container_image="docker://jhiemstra/snakemake-dev-image:v1",
        universe="container",
        htcondor_request_disk_mb=512,
        htcondor_request_mem_mb=512,
        job_wrapper="wrapper.sh"
    shell:
        """
        echo "Parallel Aggregate: Combining all worker outputs" > {output}
        echo "=================================================" >> {output}
        echo "" >> {output}
        echo "--- Worker A Results ---" >> {output}
        cat {input.a} >> {output}
        echo "" >> {output}
        echo "--- Worker B Results ---" >> {output}
        cat {input.b} >> {output}
        echo "" >> {output}
        echo "--- Worker C Results ---" >> {output}
        cat {input.c} >> {output}
        echo "" >> {output}
        echo "All parallel processing complete!" >> {output}
        """


# =============================================================================
# SHARED-DIRECTORY MTIME TORTURE TEST  (issue #48 regression)
# =============================================================================
# Three consecutive rules all read from AND write to the same `pipeline/`
# directory.  This is the exact pattern that broke Snakemake's mtime-based
# re-run detection before the executor fix.
#
# Chain:
#   mtime_seed      →  pipeline/{sample}_seed.txt
#         ↓
#   mtime_enrich    reads pipeline/{sample}_seed.txt       (prior rule's OUTPUT)
#                   writes pipeline/{sample}_enriched.txt  (SAME DIRECTORY!)
#         ↓
#   mtime_finalize  reads pipeline/{sample}_enriched.txt   (prior rule's OUTPUT)
#                   writes pipeline/{sample}_done.txt      (SAME DIRECTORY!)
#         ↓
#   mtime_check     reads all three files, verifies strict mtime ordering
#                   (seed < enriched < done), writes output/{sample}_mtime_check.txt
#
# WHY THIS MATTERS – the pre-fix bug
# -----------------------------------
# Before the fix, the executor transferred the *top-level directory* of each
# output file back to the AP (e.g. `pipeline/` for any file under pipeline/).
# After `mtime_enrich` completed, HTCondor would push the entire `pipeline/`
# directory back, overwriting `pipeline/{sample}_seed.txt` on the AP with the
# EP copy and thereby resetting its mtime to roughly T_enrich.  Snakemake
# would then see seed.txt as "just as new" as (or newer than) enriched.txt,
# and on any re-run it would incorrectly re-submit mtime_enrich and everything
# downstream.
#
# The mtime_check rule bakes this verification directly into the workflow.
# IMPORTANT: mtime_check is a localrule — it runs on the AP, not the EP.
#
# Why it must be local:
#   HTCondor does NOT preserve filesystem mtimes when staging input files to
#   the EP.  Every file in transfer_input_files is written to the EP scratch
#   directory at the same moment (the staging time), so all three files would
#   always have identical mtimes on the EP regardless of whether the executor
#   is correct or buggy.  The mtime ordering information only exists on the AP.
#
# How it detects the bug:
#   mtime_check reads from pipeline/, the same directory the remote rules use
#   as both input and output.  After each remote rule completes, the executor
#   transfers back only the explicit output file (e.g. enriched.txt) and NOT
#   the entire pipeline/ directory.  If the old bug were present, HTCondor
#   would push back pipeline/ wholesale, overwriting seed.txt on the AP and
#   resetting its mtime.  Running stat() locally on the AP-side files reveals
#   this: with the fix, seed < enriched < done; with the bug, the ordering
#   is violated.
#
# MANUAL RE-RUN VERIFICATION
# --------------------------
# After a successful run, execute:
#
#   snakemake --dryrun --executor htcondor --shared-fs-usage none
#
# With the fix:     "Nothing to be done."
# Without the fix:  Snakemake re-submits mtime_enrich, mtime_finalize, and
#                   mtime_check for each sample because seed.txt appears newer
#                   than enriched.txt on the AP.

rule mtime_seed:
    """Seed the shared pipeline/ directory."""
    output:
        "pipeline/{sample}_seed.txt"
    resources:
        container_image="docker://jhiemstra/snakemake-dev-image:v1",
        universe="container",
        request_disk="1GB",
        request_memory="512MB",
        job_wrapper="wrapper.sh"
    shell:
        """
        mkdir -p pipeline
        echo "seed data for {wildcards.sample}" > {output}
        echo "line 2" >> {output}
        echo "line 3" >> {output}
        echo "Created at: $(date -Iseconds)" >> {output}
        """

rule mtime_enrich:
    """Read rule 1's output from pipeline/, write enriched output to pipeline/.

    This is the key stress: input and output share the same directory.  If the
    executor transfers the whole pipeline/ directory back after this rule, the
    mtime of seed.txt on the AP will be reset to approximately now, breaking
    Snakemake's dependency tracking for every downstream re-run.
    """
    input:
        seed="pipeline/{sample}_seed.txt"
    output:
        "pipeline/{sample}_enriched.txt"
    resources:
        container_image="docker://jhiemstra/snakemake-dev-image:v1",
        universe="container",
        request_disk="1GB",
        request_memory="512MB",
        job_wrapper="wrapper.sh"
    shell:
        """
        echo "enriched data for {wildcards.sample}" > {output}
        echo "source: {input.seed}" >> {output}
        cat {input.seed} >> {output}
        echo "Enriched at: $(date -Iseconds)" >> {output}
        """

rule mtime_finalize:
    """Read rule 2's output from pipeline/, write final output to pipeline/.

    Second consecutive rule that uses pipeline/ as both input and output
    directory — doubles the mtime-stomping surface area.
    """
    input:
        enriched="pipeline/{sample}_enriched.txt"
    output:
        "pipeline/{sample}_done.txt"
    resources:
        container_image="docker://jhiemstra/snakemake-dev-image:v1",
        universe="container",
        request_disk="1GB",
        request_memory="512MB",
        job_wrapper="wrapper.sh"
    shell:
        """
        echo "finalized data for {wildcards.sample}" > {output}
        echo "source: {input.enriched}" >> {output}
        cat {input.enriched} >> {output}
        echo "Finalized at: $(date -Iseconds)" >> {output}
        """

# mtime_check runs on the AP so it can read AP-side filesystem mtimes directly.
# See the section comment above for why this cannot be a remote rule.
localrules: mtime_check

rule mtime_check:
    """Verify strict AP-side mtime ordering in the shared pipeline/ directory.

    This rule is LOCAL (runs on the AP).  It calls stat() on the pipeline/
    files as they exist on the AP filesystem after each upstream remote rule
    has completed and transferred its output back.  The pipeline/ directory
    is the exact directory exercised by mtime_enrich and mtime_finalize, both
    of which use pipeline/ simultaneously as an input source and output
    destination.

    With the executor fix: the executor transfers only the explicit output file
    (e.g. pipeline/enriched.txt), leaving pipeline/seed.txt untouched on the
    AP.  AP-side mtimes therefore satisfy seed < enriched < done.

    With the pre-fix bug: the executor transferred the entire pipeline/
    directory back after each rule, overwriting seed.txt on the AP with the
    EP's copy and resetting its mtime to ~T_enrich.  The ordering check fails.

    A passing mtime_check also means that `snakemake --dryrun` on a re-run
    will report "Nothing to be done" for these rules.
    """
    input:
        seed="pipeline/{sample}_seed.txt",
        enriched="pipeline/{sample}_enriched.txt",
        done="pipeline/{sample}_done.txt"
    output:
        "output/{sample}_mtime_check.txt"
    shell:
        """
        seed_mtime=$(stat -c %Y {input.seed})
        enriched_mtime=$(stat -c %Y {input.enriched})
        done_mtime=$(stat -c %Y {input.done})

        echo "=== Shared-directory mtime verification ==" > {output}
        echo "sample: {wildcards.sample}" >> {output}
        echo "" >> {output}
        echo "File mtimes (seconds since epoch):" >> {output}
        echo "  seed     ($seed_mtime): $(date -d @$seed_mtime -Iseconds)" >> {output}
        echo "  enriched ($enriched_mtime): $(date -d @$enriched_mtime -Iseconds)" >> {output}
        echo "  done     ($done_mtime): $(date -d @$done_mtime -Iseconds)" >> {output}
        echo "" >> {output}

        # Strict ordering: seed must be strictly older than enriched.
        # If the executor had transferred the entire pipeline/ directory back
        # after mtime_enrich, seed.txt's mtime on the AP would have been reset
        # to approximately enriched_mtime, making this check fail.
        if [ "$seed_mtime" -ge "$enriched_mtime" ]; then
            echo "FAIL: seed mtime ($seed_mtime) is NOT older than enriched mtime ($enriched_mtime)" >> {output}
            echo "      This indicates that pipeline/seed.txt was touched by a" >> {output}
            echo "      directory-level output transfer after mtime_enrich ran." >> {output}
            echo "      See executor issue #48." >> {output}
            cat {output} >&2
            exit 1
        fi

        # Strict ordering: enriched must be strictly older than done.
        if [ "$enriched_mtime" -ge "$done_mtime" ]; then
            echo "FAIL: enriched mtime ($enriched_mtime) is NOT older than done mtime ($done_mtime)" >> {output}
            echo "      This indicates that pipeline/enriched.txt was touched by a" >> {output}
            echo "      directory-level output transfer after mtime_finalize ran." >> {output}
            echo "      See executor issue #48." >> {output}
            cat {output} >&2
            exit 1
        fi

        echo "PASS: mtime ordering is correct (seed < enriched < done)" >> {output}
        echo "" >> {output}
        echo "Re-run note: 'snakemake --dryrun' should report 'Nothing to be done'" >> {output}
        echo "  If it re-submits mtime_enrich or mtime_finalize, the executor is" >> {output}
        echo "  still transferring whole directories instead of explicit files." >> {output}
        """


# =============================================================================
# ENVIRONMENT VARIABLE INJECTION TEST
# =============================================================================
# Tests that environment variables declared via --envvars reach the EP.
#
# The executor sets pass_envvar_declarations_to_cmd=False and instead injects
# env vars via HTCondor's native `environment` submit key using
# _format_htcondor_environment().  This is strictly better than embedding
# "export VAR=val &&" in the arguments string, which broke the job_wrapper
# code path (the args string must start cleanly with "python -m snakemake"
# for the wrapper's prefix-stripping to succeed).
#
# To exercise this:
#   1. run.sh exports TORTURE_TEST_VAR and passes --envvars TORTURE_TEST_VAR
#   2. This rule checks that the variable is set on the EP
#   3. The rule uses job_wrapper="wrapper.sh", so the wrapper + env var
#      combination is tested together

rule env_var_check:
    """Verify that --envvars variables reach the EP via HTCondor's environment key.

    This rule deliberately uses job_wrapper to test the interaction: env vars
    must be injected via HTCondor's `environment` key (not inline in arguments)
    so that the wrapper's prefix-stripping still works.
    """
    input:
        "data/{sample}_numbers.txt"
    output:
        "output/{sample}_env_check.txt"
    resources:
        container_image="docker://jhiemstra/snakemake-dev-image:v1",
        universe="container",
        request_disk="1GB",
        request_memory="512MB",
        job_wrapper="wrapper.sh"
    shell:
        """
        echo "=== Environment Variable Injection Check ===" > {output}
        echo "sample: {wildcards.sample}" >> {output}
        echo "" >> {output}

        if [ -z "$TORTURE_TEST_VAR" ]; then
            echo "FAIL: TORTURE_TEST_VAR is not set on the EP" >> {output}
            echo "      The executor's _format_htcondor_environment() or" >> {output}
            echo "      pass_envvar_declarations_to_cmd setting may be broken." >> {output}
            cat {output} >&2
            exit 1
        fi

        echo "TORTURE_TEST_VAR=$TORTURE_TEST_VAR" >> {output}
        echo "" >> {output}
        echo "PASS: environment variable reached the EP" >> {output}
        """


# =============================================================================
# htcondor_transfer_output_files TEST
# =============================================================================
# Tests the htcondor_transfer_output_files resource, which allows users to
# declare additional output files that HTCondor should transfer back from the
# EP — files that are NOT declared as Snakemake outputs.
#
# The extra_output_check rule produces:
#   1. A declared Snakemake output (enters the DAG normally)
#   2. A "sidecar" file declared only via htcondor_transfer_output_files
#
# Since the sidecar is not a Snakemake output, Snakemake can't verify it.
# run.sh checks for its existence on the AP after the workflow completes.

rule extra_output_check:
    """Test htcondor_transfer_output_files for user-declared additional outputs.

    Produces a sidecar file that is NOT a declared Snakemake output but IS
    declared via htcondor_transfer_output_files.  The sidecar must be
    transferred back to the AP by HTCondor.  run.sh verifies its existence.
    """
    input:
        "data/{sample}_numbers.txt"
    output:
        "output/{sample}_extra_output_main.txt"
    resources:
        container_image="docker://jhiemstra/snakemake-dev-image:v1",
        universe="container",
        request_disk="1GB",
        request_memory="512MB",
        job_wrapper="wrapper.sh",
        htcondor_transfer_output_files=lambda wildcards, **kwargs: f"output/{wildcards.sample}_extra_sidecar.txt"
    shell:
        """
        echo "Main declared output for {wildcards.sample}" > {output}
        echo "Line count from input: $(wc -l < {input})" >> {output}

        echo "Sidecar (undeclared) output for {wildcards.sample}" > output/{wildcards.sample}_extra_sidecar.txt
        echo "This file was transferred via htcondor_transfer_output_files" >> output/{wildcards.sample}_extra_sidecar.txt
        """


# =============================================================================
# PARTIAL SHARED FILESYSTEM TEST  (--htcondor-shared-fs-prefixes)
# =============================================================================
# Tests the executor's ability to handle a *partially* shared filesystem.
# With --shared-fs-usage none, the executor normally transfers everything.
# But --htcondor-shared-fs-prefixes /staging/jhiemstra tells it:
#   "Files under /staging/jhiemstra/ are directly accessible on both AP and EP
#    — do NOT include them in transfer_input_files or transfer_output_files."
#
# This is a common real-world pattern: the AP's local scratch is NOT shared,
# but a specific staging area (e.g. an NFS or CephFS mount) IS shared.
#
# Two rules exercise this:
#
#   shared_fs_write:
#     - Input:  data/{sample}_numbers.txt           (local → transferred)
#     - Output: /staging/jhiemstra/torture-test/...  (shared FS → NOT transferred)
#     The EP writes directly to the shared mount.  The executor must exclude
#     the output from transfer_output_files.
#
#   shared_fs_mixed:
#     - Input:  /staging/jhiemstra/torture-test/...  (shared FS → NOT transferred)
#     - Input:  data/{sample}_numbers.txt            (local → transferred)
#     - Output: output/{sample}_shared_fs_check.txt  (local → transferred)
#     The boundary test: one input is shared (EP reads it directly), the other
#     is local (must be transferred).  If the executor incorrectly transfers
#     the shared input, it would still work — but if it fails to recognize the
#     shared path and skips transferring the local input, the job fails.
#
# run.sh verifies that the shared-FS output exists on the AP after the workflow.

# Staging area path — adjust if your shared mount differs
SHARED_FS_TEST_DIR = "/staging/jhiemstra/torture-test"

rule shared_fs_write:
    """Write output directly to the shared filesystem.

    The local input must be transferred to the EP.  The output lives on the
    shared mount, so the executor must NOT list it in transfer_output_files —
    the EP writes to it directly and the AP can already see it.
    """
    input:
        "data/{sample}_numbers.txt"
    output:
        SHARED_FS_TEST_DIR + "/{sample}_shared_data.txt"
    resources:
        container_image="docker://jhiemstra/snakemake-dev-image:v1",
        universe="container",
        request_disk="1GB",
        request_memory="512MB",
        job_wrapper="wrapper.sh"
    shell:
        """
        mkdir -p {SHARED_FS_TEST_DIR}
        echo "Shared FS data for {wildcards.sample}" > {output}
        echo "Written directly to shared mount — not transferred by HTCondor" >> {output}
        cat {input} >> {output}
        echo "Written at: $(date -Iseconds)" >> {output}
        """

rule shared_fs_mixed:
    """Read from shared FS (not transferred) AND local (transferred), write local output.

    This is the key boundary test:
      - shared_input: under /staging/jhiemstra/ → executor skips transfer,
        EP reads directly from the shared mount
      - local_input: relative path → executor transfers it to the EP
      - output: relative path → executor transfers it back to the AP

    If the executor gets the shared-FS filtering wrong in either direction,
    the job will fail.
    """
    input:
        shared=SHARED_FS_TEST_DIR + "/{sample}_shared_data.txt",
        local="data/{sample}_numbers.txt"
    output:
        "output/{sample}_shared_fs_check.txt"
    resources:
        container_image="docker://jhiemstra/snakemake-dev-image:v1",
        universe="container",
        request_disk="1GB",
        request_memory="512MB",
        job_wrapper="wrapper.sh"
    shell:
        """
        echo "=== Shared Filesystem Prefix Check ===" > {output}
        echo "sample: {wildcards.sample}" >> {output}
        echo "" >> {output}

        echo "Shared input path: {input.shared}" >> {output}
        echo "Local  input path: {input.local}" >> {output}
        echo "" >> {output}

        # Verify the shared input is readable (EP accessed it directly)
        if [ ! -f "{input.shared}" ]; then
            echo "FAIL: shared input not found at {input.shared}" >> {output}
            echo "      The EP could not access the shared filesystem mount." >> {output}
            cat {output} >&2
            exit 1
        fi

        echo "Contents from shared FS:" >> {output}
        cat {input.shared} >> {output}
        echo "" >> {output}

        echo "Contents from local (transferred):" >> {output}
        cat {input.local} >> {output}
        echo "" >> {output}

        echo "PASS: EP successfully read from both shared and local paths" >> {output}
        """
