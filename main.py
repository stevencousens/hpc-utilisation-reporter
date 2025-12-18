import argparse
import datetime
import os
import sys

import pandas as pd
from src.capacities import get_capacities, get_capacity_history, expand_capacity_snapshots
from src.timeseries import make_sacct_timeseries
from src.jobs import get_sacct_data

def valid_date(s):
    try:
        return datetime.datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        raise argparse.ArgumentTypeError(f"Not a valid date: '{s}'. Expected format YYYY-MM-DD.")

def parse_args():
    parser = argparse.ArgumentParser(
        description="Process Slurm sacct logs and capacity files."
    )
    parser.add_argument("--jobs-dir", required=True, help="Path to Slurm sacct logs")
    parser.add_argument("--capacities-dir", required=True, help="Path to capacity files")
    parser.add_argument("--output-dir", default=".", help="Path to write output files")
    parser.add_argument("--report-start", type=valid_date, required=True,
                        help="Report start date (YYYY-MM-DD)")
    parser.add_argument("--report-end", type=valid_date, required=True,
                        help="Report end date (YYYY-MM-DD)")
    return parser.parse_args()


def main():
    args = parse_args()

    # Normalize to absolute paths
    jobs_dir = os.path.abspath(os.path.expanduser(args.jobs_dir))
    capacities_dir = os.path.abspath(os.path.expanduser(args.capacities_dir))
    output_dir = os.path.abspath(os.path.expanduser(args.output_dir))

    # Validate directories exist
    for d, name in [(jobs_dir, "job"), (capacities_dir, "capacity"), (output_dir, "output")]:
        if not os.path.isdir(d):
            sys.exit(f"Error: {name} directory does not exist → {d}")

    report_start = args.report_start
    report_end = args.report_end

    if report_start > report_end:
        sys.exit("Error: Report start date must be before or equal to report end date.")

    print(f"Jobs dir: {jobs_dir}")
    print(f"Capacities dir: {capacities_dir}")
    print(f"Output dir: {output_dir}")
    print(f"Report range: {report_start.date()} → {report_end.date()}")
    
    # --- Capacities ---
    capacity_history_df = get_capacity_history(capacities_dir)

    # Expand into a daily time series
    filled_df = expand_capacity_snapshots(capacity_history_df, start=report_start,end=report_end)

    capacity_path = os.path.join(output_dir, "CapacityReport.csv")
    filled_df.to_csv(capacity_path, index=False)

    # --- Jobs ---
    
    # uses current capacity only for gpu assignment
    current_caps = get_capacities()
    sacct_data = get_sacct_data(jobs_dir, current_caps)

    non_gpu_res_list = ['node', 'partition', 'cpu', 'mem_gb']
    res_list = current_caps.columns.tolist()
    gpu_list = [res for res in res_list if res not in non_gpu_res_list] + ["indeterminate_gpu"]

    cols_to_keep = ["jobid", "user", "partition", "submit", "state", "elapsedraw",
                    "queue_length_sec", "scheduling_coeff", "cpu", "mem_gb"] + gpu_list
    output_sacct_data = sacct_data[cols_to_keep].copy()
    jobs_path = os.path.join(output_dir, "JobReport.csv")
    output_sacct_data.to_csv(jobs_path, sep=",", index=False)

    ts_res_list = ["cpu", "mem_gb"] + gpu_list
    time_series_data = make_sacct_timeseries(
        sacct_data,
        ts_res_list,
        report_start,
        report_end,
        freq="h"
    )
    util_path = os.path.join(output_dir, "UtilisationReport.csv")
    time_series_data.to_csv(util_path, sep=",", index=False)


if __name__ == "__main__":
    main()

