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
    jobs_input_group = parser.add_mutually_exclusive_group(required=True)
    jobs_input_group.add_argument("--jobs-file", help="Single sacct log file")
    jobs_input_group.add_argument("--jobs-dir", help="Directory containing JobList_*.txt files")

    parser.add_argument("--capacities-dir", required=True, help="Path to capacity files")
    parser.add_argument("--output-dir", default=".", help="Path to write output files")
    parser.add_argument("--report-start", type=valid_date, required=True,
                        help="Report start date (YYYY-MM-DD)")
    parser.add_argument("--report-end", type=valid_date, required=True,
                        help="Report end date (YYYY-MM-DD)")
    return parser.parse_args()

def validate_paths(args):
    # Jobs path
    if args.jobs_file:
        jobs_path = os.path.abspath(os.path.expanduser(args.jobs_file))
        if not os.path.isfile(jobs_path):
            sys.exit(f"Error: jobs file does not exist → {jobs_path}")
    else:
        jobs_path = os.path.abspath(os.path.expanduser(args.jobs_dir))
        if not os.path.isdir(jobs_path):
            sys.exit(f"Error: jobs directory does not exist → {jobs_path}")

    # Capacities directory
    capacities_dir = os.path.abspath(os.path.expanduser(args.capacities_dir))
    if not os.path.isdir(capacities_dir):
        sys.exit(f"Error: capacity directory does not exist → {capacities_dir}")

    # Output directory
    output_dir = os.path.abspath(os.path.expanduser(args.output_dir))
    if not os.path.isdir(output_dir):
        sys.exit(f"Error: output directory does not exist → {output_dir}")

    return jobs_path, capacities_dir, output_dir

def main():
    args = parse_args()

    jobs_path, capacities_dir, output_dir = validate_paths(args)
    
    report_start = args.report_start
    report_end = args.report_end

    if report_start > report_end:
        sys.exit("Error: Report start date must be before or equal to report end date.")

    print(f"Jobs input: {jobs_path}")
    print(f"Capacities dir: {capacities_dir}")
    print(f"Output dir: {output_dir}")
    print(f"Report range: {report_start.date()} → {report_end.date()}")
    
    # --- Capacities ---
    capacity_history_df = get_capacity_history(capacities_dir)

    # Expand into a daily time series
    filled_df = expand_capacity_snapshots(capacity_history_df, start=report_start,end=report_end)

    capacity_report_path = os.path.join(output_dir, "CapacityReport.csv")
    filled_df.to_csv(capacity_report_path, index=False)

    # --- Jobs ---
    
    # uses current capacity only for gpu assignment
    current_caps = get_capacities()
    sacct_data = get_sacct_data(jobs_path, current_caps)

    non_gpu_res_list = ['node', 'partition', 'cpu', 'mem_gb']
    res_list = current_caps.columns.tolist()
    gpu_list = [res for res in res_list if res not in non_gpu_res_list] + ["indeterminate_gpu"]

    cols_to_keep = ["jobid", "user", "partition", "submit", "state", "elapsedraw",
                    "queue_length_sec", "scheduling_coeff", "cpu", "mem_gb"] + gpu_list
    output_sacct_data = sacct_data[cols_to_keep].copy()
    jobs_report_path = os.path.join(output_dir, "JobReport.csv")
    output_sacct_data.to_csv(jobs_report_path, sep=",", index=False)

    ts_res_list = ["cpu", "mem_gb"] + gpu_list
    time_series_data = make_sacct_timeseries(
        sacct_data,
        ts_res_list,
        report_start,
        report_end,
        freq="h"
    )
    util_report_path = os.path.join(output_dir, "UtilisationReport.csv")
    time_series_data.to_csv(util_report_path, sep=",", index=False)


if __name__ == "__main__":
    main()

