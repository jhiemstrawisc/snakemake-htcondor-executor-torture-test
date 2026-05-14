#!/bin/bash

rm -rf logs/ output/ data/ pipeline/ .snakemake/

# Clean up shared filesystem test outputs (adjust path if your mount differs)
rm -rf /staging/jhiemstra/torture-test/
