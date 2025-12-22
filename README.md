# HPC Utilisation Reporter

![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)
![Python](https://img.shields.io/badge/python-3.12-blue)

A utilisation reporting tool for SLURM-based High‑Performance Computing (HPC) clusters. This tool processes raw sacct and sinfo logs to generate more easily interpretable reports and time-series data.

> ⚠️ **Disclaimer**  
This project is currently under active development. Features may change, and bugs may be present. Use at your own discretion, and feel free to report issues or contribute!

## Installation

### Prerequisites

The HPC Utilisation Reporter requires a compatible version of Python (developed using Python 3.12.4) and an HPC system with the usual Slurm command-line tools (`sinfo`, `sacct`).

> ⚠️ This tool has currently only been tested with Python 3.12.4 and Slurm 25.05.2. Compatibility with other versions is not guaranteed.

### Steps

To install the HPC Utilisation Reporter, clone the repository and install the required dependencies:

```bash
git clone https://github.com/stevencousens/hpc-utilisation-reporter.git
cd hpc-utilisation-reporter

# load Python into your environment (if applicable, this is cluster specific)
module load apps/python3/3.12.4/gcc-14.1.0

# create and activate a virtual environment for Python packages (recommended)
python3 -m venv .venv 
source .venv/bin/activate

# install required Python packages
pip install -r requirements.txt
```

## Generating sacct logs

The HPC Utilisation Reporter uses pipe‑delimited sacct logs that contain a specific set of fields. These logs may be supplied to the reporter individually via a `--jobs-file` parameter or as a collection via `--jobs-dir`.

All sacct logs must
- use pipe-delimited output (`-P`)
- exclude job step records (`-X`)
- include the following fields: `jobid,user,partition,submit,start,end,state,elapsedraw,nodelist,reqtres,alloctres`

### Example sacct command

The following command generates and saves a complete log for all users with jobs in November 2025. 

```bash
sacct -a -P -X \
  --starttime=2025-11-01 \
  --endtime=2025-12-01 \
  -o jobid,user,partition,submit,start,end,state,elapsedraw,nodelist,reqtres,alloctres \
  > JobList_2025-11.txt
```

If you are only interested in a subset of the utilisation, you can refine the query using the usual Slurm parameters, for example, replace `-a` with `-u alice,bob` to restrict by user, or add `-r hipri` to restrict by partition.

###  Sacct Log Naming Convention
When using `--jobs-dir`, the reporter automatically loads files in that directory matching:

```bash
JobList_*.txt
```

This pattern is designed for logs named by year and month, such as:

```bash
JobList_2025-01.txt
JobList_2025-02.txt
JobList_2025-03.txt
```

Only files following this naming convention are included. This ensures predictable ordering and correct deduplication of job records where they appear in multiple files. All files in the directory must follow the same sacct format described above.

## General Syntax

```bash
python3 main.py \
  (--jobs-file FILE | --jobs-dir DIR) \
  --report-start YYYY-MM-DD \
  --report-end YYYY-MM-DD \
  [--capacities-dir DIR] \
  [--output-dir DIR]
```

### Command‑line Parameters

The HPC Utilisation Reporter accepts various command-line parameters.

#### `--report-start` and `--report-end`

These parameters define the reporting window for the utilisation and capacity time-series output.

- `--report-start` is inclusive 
- `--report-end` is exclusive

#### `--jobs-file` / `--jobs-dir`

- `--jobs-file` — Path to a single sacct logfile.
- `--jobs-dir` — Path to a directory containing multiple sacct logfiles.  
  The reporter automatically loads and concatenates all files matching `JobList_*.txt` in this directory.


#### `--capacities-dir` (optional)

Path to a directory containing capacity snapshot files.  
If provided, the reporter will:

- read capacity snapshots from the directory  
- expand them into daily capacity snapshots per node and per partition
- generate `CapacityReport.csv`

If omitted, capacity reporting is skipped.

#### `--output-dir` (optional)

Specifies where CSV output files should be written.  
Defaults to the current working directory.

## Output Files

- **`JobReport.csv`** — Per‑job metrics including CPU usage, memory usage, GPU type counts, queueing time, and scheduling efficiency.

- **`UtilisationReport.csv`** — Hourly utilisation across the reporting window (broken down by partition) for CPU, memory, and each GPU type (including indeterminate GPU usage).

- **`CapacityReport.csv`** *(optional)* — Daily capacity snapshots per node and per partition, generated only when `--capacities-dir` is provided.


## License

This project is licensed under the MIT License. See [LICENSE](./LICENSE) for details.

## Contributing

Contributions are welcome! Feel free to open issues or submit pull requests.
