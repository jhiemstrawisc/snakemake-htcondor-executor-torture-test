# HTCondor Executor Torture Test Workflow

A comprehensive Snakemake workflow for testing the HTCondor executor plugin's file transfer and advanced feature support.

## Features Tested

This workflow exercises the following HTCondor executor capabilities:

| Feature | Description |
|---------|-------------|
| **Snakefile transfer** | Main workflow file detection and transfer |
| **Job wrapper** | Custom `wrapper.sh` used as HTCondor executable |
| **Script transfer** | Python scripts via `script:` directive |
| **Wildcard expansion in scripts** | Script paths like `scripts/process_{wildcards.sample}.py` |
| **Additional input files** | `htcondor_transfer_input_files` resource for helper modules |
| **Additional output files** | `htcondor_transfer_output_files` resource for sidecar outputs |
| **Input/output file transfer** | Standard Snakemake `input:`/`output:` handling |
| **Shell commands** | Rules using `shell:` directive |
| **Snakemake modules** | External Snakefiles via `module:` directive |
| **Nested modules** | Recursive module dependencies |
| **Grouped jobs** | Multiple rules executed as single HTCondor job via `group:` |
| **temp() in groups** | `temp()` intermediate excluded from HTCondor transfer list |
| **Container support** | Docker containers via `universe="container"` |
| **Environment variables** | `--envvars` injection via HTCondor's native `environment` key |
| **Shared-FS prefixes** | `--htcondor-shared-fs-prefixes` skips transfer for files on shared mounts |
| **Shared-dir mtime** | Verifies output transfer does not stomp sibling file mtimes |
| **Multiple samples** | Concurrent HTCondor jobs for different wildcard values |
| **Re-run idempotency** | Automated `--dryrun` check after workflow completes |

## Workflow Structure

```
                    ┌─────────────────────────────────────────────────┐
                    │                  create_data                    │
                    │           (creates sample input data)           │
                    └─────────────────────┬───────────────────────────┘
                                          │
              ┌───────────────────────────┼───────────────────────────┐
              │                           │                           │
              ▼                           ▼                           ▼
┌─────────────────────────┐ ┌─────────────────────────┐ ┌─────────────────────────┐
│      process_data       │ │     grouped_step1       │ │                         │
│  (script + wildcards)   │ │                         │ │                         │
└───────────┬─────────────┘ │      grouped_step2      │ │      quality_check      │
            │               │                         │ │        (module)         │
            ▼               │      grouped_step3      │ │                         │
┌─────────────────────────┐ │                         │ └───────────┬─────────────┘
│      make_report        │ │  (group: data_pipeline) │             │
│        (shell)          │ └───────────┬─────────────┘             ▼
└───────────┬─────────────┘             │             ┌─────────────────────────┐
            │                           │             │    validate_format      │
            ▼                           ▼             │    (nested module)      │
    {sample}_report.txt      {sample}_grouped_final   └───────────┬─────────────┘
                                   .txt                           │
                                                                  ▼
                                                         {sample}_validated.txt
```

## Directory Structure

```
snake-script-testing/
├── Snakefile                      # Main workflow (tests Snakefile transfer)
├── wrapper.sh                     # Job wrapper (tests job_wrapper resource)
├── run.sh                         # Test runner with post-run verification
├── cleanup.sh                     # Reset workflow (remove generated files)
├── README.md                      # This file
├── scripts/
│   ├── process_sample1.py         # Sample1-specific script (tests wildcard expansion)
│   ├── process_sample2.py         # Sample2-specific script (tests per-sample selection)
│   └── stats_helpers.py           # Helper module (tests htcondor_transfer_input_files)
├── modules/
│   └── quality_check/
│       ├── Snakefile              # Module Snakefile (tests module transfer)
│       └── validation/
│           └── Snakefile          # Nested module (tests recursive module detection)
├── data/                          # Generated input data (created by workflow)
├── output/                        # Final outputs
└── logs/                          # HTCondor job logs (--htcondor-jobdir)
```

## Rules

### Main Workflow

| Rule | Type | Purpose |
|------|------|---------|
| `create_data` | shell | Creates input data file with numbers 1-5 |
| `process_data` | script | Computes statistics using wildcard-expanded script path |
| `make_report` | shell | Generates final report from processed data |

### Module: quality_check

| Rule | Type | Purpose |
|------|------|---------|
| `quality_check` | shell | Validates processed data, checks content |

### Nested Module: validation

| Rule | Type | Purpose |
|------|------|---------|
| `validate_format` | shell | Validates QC report format |

### Grouped Jobs (group: data_pipeline)

| Rule | Type | Purpose |
|------|------|---------|
| `grouped_step1` | shell | Doubles input values; output is `temp()` — tests skip_temp |
| `grouped_step2` | shell | Adds header (also tests `htcondor_transfer_input_files` on groups) |
| `grouped_step3` | shell | Creates final summary |

These three rules execute together as a **single HTCondor job**.
`grouped_step1` output is marked `temp()` — it is consumed by `grouped_step2` within the same EP execution and then deleted. The executor must exclude it from `transfer_output_files`.

### Parallel Grouped Jobs (group: parallel_pipeline)

| Rule | Type | Purpose |
|------|------|---------|
| `parallel_split` | shell | Prepares data for parallel processing |
| `parallel_worker_a` | shell | Multiplies by 10 (1 thread, 1GB) |
| `parallel_worker_b` | shell | Multiplies by 100 (2 threads, 2GB) |
| `parallel_worker_c` | shell | Multiplies by 1000 (1 thread, 1GB) |
| `parallel_aggregate` | shell | Combines all worker outputs |

Fan-out/fan-in pattern testing resource summing across parallel jobs.

### Shared-Directory mtime Test

| Rule | Type | Purpose |
|------|------|---------|
| `mtime_seed` | shell (remote) | Seeds `pipeline/` directory |
| `mtime_enrich` | shell (remote) | Reads + writes to `pipeline/` (same dir as input) |
| `mtime_finalize` | shell (remote) | Reads + writes to `pipeline/` (same dir as input) |
| `mtime_check` | shell (**local**) | Verifies strict mtime ordering on the AP |

### Environment & Transfer Tests

| Rule | Type | Purpose |
|------|------|---------|
| `env_var_check` | shell (remote) | Verifies `--envvars` reach EP via HTCondor `environment` key |
| `extra_output_check` | shell (remote) | Tests `htcondor_transfer_output_files` sidecar transfer |

### Partial Shared Filesystem Tests

| Rule | Type | Purpose |
|------|------|---------|
| `shared_fs_write` | shell (remote) | Writes output to shared mount (NOT transferred by HTCondor) |
| `shared_fs_mixed` | shell (remote) | Reads shared input (not transferred) + local input (transferred) |

## Key Test Cases

### 1. Wildcard Expansion in Script Paths
```python
rule process_data:
    script: "scripts/process_{wildcards.sample}.py"
```
The executor must expand `{wildcards.sample}` to `sample1` or `sample2` **before** adding the script to transfer files. Each sample uses a distinct `.py` file.

### 2. Additional Input File Transfer
```python
resources:
    htcondor_transfer_input_files="scripts/stats_helpers.py"
```
The script imports from `stats_helpers.py`, so this file **must** be transferred or the job will fail.

### 3. Grouped Job File Detection
The `grouped_step2` rule includes `htcondor_transfer_input_files` to test that custom transfer resources work correctly when rules are grouped together.

### 4. Nested Module Detection
The `quality_check` module imports the `validation` module, testing that the executor recursively discovers and transfers all module Snakefiles.

### 5. temp() Output Exclusion
`grouped_step1` output is `temp()`.  Snakemake deletes it on the EP after `grouped_step2` consumes it.  The executor must exclude it from `transfer_output_files` or HTCondor will error when the file is missing at transfer time.

### 6. Environment Variable Injection
`run.sh` exports `TORTURE_TEST_VAR` and passes `--envvars TORTURE_TEST_VAR`. The `env_var_check` rule verifies the variable is set on the EP.  This tests the `_format_htcondor_environment()` serialization AND the `job_wrapper` interaction (env vars must not be inlined in the arguments string).

### 7. Additional Output File Transfer
`extra_output_check` writes a sidecar file declared only via `htcondor_transfer_output_files` (not a Snakemake output). `run.sh` verifies the sidecar exists on the AP after the workflow.

### 8. Shared-Directory mtime Correctness
Three consecutive rules read from and write to `pipeline/`.  `mtime_check` (a localrule) verifies strict AP-side mtime ordering: `seed < enriched < done`.  If the executor transferred whole directories instead of explicit files, mtimes would be stomped.

### 9. Re-run Idempotency
After the workflow completes, `run.sh` runs `snakemake --dryrun` and asserts "Nothing to be done."  This catches mtime-stomping that would cause unnecessary re-runs.

### 10. Partial Shared Filesystem
`shared_fs_write` writes output to `/staging/jhiemstra/torture-test/` (the shared mount).  The executor must NOT add this path to `transfer_output_files`.  `shared_fs_mixed` reads from the shared mount AND a local path in the same rule — the shared input is accessed directly, the local input is transferred.  `run.sh` verifies the shared-FS output files exist on the AP.

### 11. Multiple Samples / Concurrent Jobs
`SAMPLES = ["sample1", "sample2"]` exercises concurrent HTCondor job submission, remap correctness when two jobs write to the same directory simultaneously, and wildcard expansion producing different concrete script paths.

## Running the Workflow

### Using the test runner (recommended):
```bash
./run.sh
```
This runs the full workflow and then performs automated post-run verification:
1. Re-run idempotency check (`--dryrun` asserts "Nothing to be done")
2. Sidecar file existence check (`htcondor_transfer_output_files`)
3. Shared filesystem output existence check

### Manual execution (no post-run checks):
```bash
export TORTURE_TEST_VAR="hello_from_ap"
snakemake --executor htcondor --jobs 10 --shared-fs-usage none \
    --htcondor-jobdir logs --htcondor-shared-fs-prefixes /staging/jhiemstra \
    --envvars TORTURE_TEST_VAR
```

### With verbose output:
```bash
export TORTURE_TEST_VAR="hello_from_ap"
snakemake --executor htcondor --jobs 10 --shared-fs-usage none \
    --htcondor-jobdir logs --htcondor-shared-fs-prefixes /staging/jhiemstra \
    --envvars TORTURE_TEST_VAR --verbose
```

## Expected Outputs

After successful execution (for each sample in `sample1`, `sample2`):

| File | Description |
|------|-------------|
| `data/{sample}_numbers.txt` | Input data (1-5) |
| `output/{sample}_processed.txt` | Statistics (count, sum, avg, min, max) |
| `output/{sample}_report.txt` | Final processing report |
| `output/{sample}_qc.txt` | Quality check results |
| `output/{sample}_validated.txt` | Format validation results |
| `output/{sample}_grouped_step1.txt` | ⚠️ **temp()** — deleted after group execution |
| `output/{sample}_grouped_step2.txt` | Grouped pipeline step 2 |
| `output/{sample}_grouped_final.txt` | Grouped pipeline final output |
| `output/{sample}_parallel_input.txt` | Parallel pipeline split output |
| `output/{sample}_worker_{a,b,c}.txt` | Parallel worker outputs |
| `output/{sample}_parallel_final.txt` | Parallel aggregate output |
| `pipeline/{sample}_seed.txt` | mtime test seed |
| `pipeline/{sample}_enriched.txt` | mtime test enriched |
| `pipeline/{sample}_done.txt` | mtime test finalized |
| `output/{sample}_mtime_check.txt` | mtime ordering verification (PASS/FAIL) |
| `output/{sample}_env_check.txt` | Env var injection verification (PASS/FAIL) |
| `output/{sample}_extra_output_main.txt` | Declared Snakemake output |
| `output/{sample}_extra_sidecar.txt` | Sidecar via `htcondor_transfer_output_files` |
| `output/{sample}_shared_fs_check.txt` | Shared FS mixed-input verification (PASS/FAIL) |
| `/staging/jhiemstra/torture-test/{sample}_shared_data.txt` | Output written directly to shared mount |

## Requirements

- Snakemake 8.x+
- HTCondor executor plugin (`snakemake-executor-plugin-htcondor`)
- Access to HTCondor pool with container universe support
- Docker image: `jhiemstra/snakemake-dev-image:v1` (or modify `container_image` resources)
- Shared filesystem mount at `/staging/jhiemstra/` accessible on both AP and EPs

## Cleaning Up

To reset the workflow and remove all generated files:
```bash
./cleanup.sh
# or manually:
rm -rf data/ output/ logs/ pipeline/ .snakemake/ /staging/jhiemstra/torture-test/
```
