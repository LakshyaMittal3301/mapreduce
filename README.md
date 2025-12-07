# MapReduce Framework

This project is a reorganized version of the MIT 6.5840 MapReduce lab.
It includes:

* The **MapReduce framework** (`mapreduce/`)
* The **program binaries** (`cmd/`)
* The **MapReduce apps** (`apps/`)
* The **input data** (`data/pg/`)
* **Scripts** to run and test everything (`scripts/`)
* **Temporary output** (`tmp/`)
* **Compiled binaries/plugins** (`bin/`) — ignored in git

---

## 🔹 Running a Single MapReduce App

To build and run one MR app (e.g. `wc`):

```bash
cd scripts
./run-single.sh wc
```

What this script does:

1. Builds the plugin (`apps/wc.go → bin/plugins/wc.so`)
2. Builds the coordinator, worker, sequential binaries into `bin/`
3. Creates a clean run folder: `tmp/mr-single/`
4. Runs **mrsequential** → generates expected output
5. Runs **distributed MapReduce** (1 coordinator + 3 workers)
6. Compares output and prints **PASS** or **FAIL**

**Where to see output:**
Look inside:

```
tmp/mr-single/job/<job-id>/output/
```

You’ll see `mr-out-*`, `mr-expected`, and `mr-all`.

**Common flags you can pass (optional):**

* `LOG_LEVEL=debug ./run-single.sh wc` — enable debug logging (default is `info`).
* Coordinator flags (defaulted by scripts): `-n-reduce`, `-job-id`, `-listen`, `-log-level`.
* Worker flags (defaulted by scripts): `-coord-addr`, `-app`, `-log-level`.

---

## 🔹 Running the Full Test Suite

To run **all official tests** (wc, indexer, crash, parallelism, etc.):

```bash
cd scripts
./test-mr.sh
```

This script:

* Rebuilds everything
* Creates a clean test folder: `tmp/mr-test/`
* Runs every MIT-provided test
* Prints PASS/FAIL for each

---

## 🔹 Running Tests Multiple Times

Stress-test your implementation:

```bash
cd scripts
./test-mr-many.sh 10
```

Runs the full test suite 10 times.

---

## 🔹 Running with S3 Storage

The `scripts/run-s3-single.sh` script runs a single app using the S3 storage backend:

```bash
cd scripts
LOG_LEVEL=debug ./run-s3-single.sh wc my-s3-bucket
```

Key flags (set by the script):

* Coordinator: `-map-timeout=30s`, `-reduce-timeout=120s`, `-log-level` (default `info`, override via `LOG_LEVEL`).
* Workers: `-storage=s3`, `-s3-bucket=<bucket>`, `-idle-wait=1s`, `-log-level` (default `info`, override via `LOG_LEVEL`).

Outputs are written to `jobs/<job-id>/output/` in S3; intermediate files live under `jobs/<job-id>/intermediate/`.

---

## 🔹 Where Things Live

* **apps/** — MapReduce plugins (wc, indexer, custom apps)
* **cmd/** — Coordinator, worker, and sequential programs
* **mapreduce/** — Core MR framework (RPC, scheduling, worker logic)
* **data/pg/** — Text files used for processing
* **bin/** — Compiled binaries + `.so` plugin outputs
* **scripts/** — All automation scripts
* **tmp/** — Temporary run & test outputs

---

## 🔹 How to Add Your Own MR App

Create `apps/myapp.go`, then run:

```bash
cd scripts
./run-single.sh myapp
```

It will build the plugin and run everything automatically.
