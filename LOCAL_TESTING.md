# Shopping Sessions local testing

Run the commands below from the top of this workspace on the MINISFORUM. The script uses the unpacked starter, generator, and official answers already present here.

Install Docker Engine with Buildx, GNU Make, Python 3 with Polars, a C linker, and Rust 1.85 or newer. Start Docker and confirm the user running the script can access it.

```bash
bash builders-test.sh setup
bash builders-test.sh fixtures
bash builders-test.sh check-dev
```

`fixtures` builds the image and compares both Parquet tables against each of the 36 official fixture answers. `check-dev` builds the image and checks the published seed 1 dataset with 1,000,000 events and 250,000 clicks. It generates that dataset on the first run and reuses it afterward.

Once both exact comparisons pass, build the image after any pipeline changes and benchmark it.

```bash
bash builders-test.sh build
bash builders-test.sh bench ./work/datasets/seed_1_events_1000000_clicks_250000 3

bash builders-test.sh generate 20000000 6000000 11
bash builders-test.sh bench ./work/datasets/seed_11_events_20000000_clicks_6000000 3

bash builders-test.sh generate 60000000 18000000 23
bash builders-test.sh bench ./work/datasets/seed_23_events_60000000_clicks_18000000 3
```

The 20M and 60M click counts are local stress-test settings. Benchmark wall times include Docker startup and cleanup. Both `test DATASET` and `bench DATASET [RUNS]` check that the image exits successfully and produces both Parquet reports. Exact comparisons are performed by `fixtures` and `check-dev`.

The Docker runner defaults to Linux amd64, 8 vCPUs, and 24 GB of memory with swap disabled. Set `CPUS` and `MEM` before invoking the script if your Docker host needs lower limits. Set `WORK_DIR` to put generated inputs, scratch files, and run outputs on another SSD. Each run keeps its reports in a fresh directory under `work/runs` by default.
