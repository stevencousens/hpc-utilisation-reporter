import pandas as pd
from pathlib import Path

from src.utils import expand_nodelist
from src.capacity_helpers import get_gpu_types, get_node_to_gpu_map, get_partition_to_gpu_map

def concat_sacct_data(directory):
    raw_dir = Path(directory)
    
    files = sorted(raw_dir.glob("JobList_*.txt"), reverse=True) # sorted for later drop_duplicates

    if not files:
        raise FileNotFoundError(f"No JobList_*.txt files found in {raw_dir}")

    # Load each file into a DataFrame
    dfs = []
    for f in files:
        df = pd.read_csv(f, sep="|", dtype=str)
        dfs.append(df)

    # Concatenate all DataFrames
    combined = pd.concat(dfs, ignore_index=True)

    # Normalise JobID and drop duplicates
    combined = (
        combined
        .assign(JobID=lambda df: df.JobID.astype(str))
        .drop_duplicates("JobID", keep="first")
    )

    return combined

def assign_gpus(row, gpu_types, node_to_gpu_map, partition_to_gpu_map):
    """Assign GPU counts to job row using node, TRES, and partition mappings."""
    row = row.copy()
    gpu_total = row["gpu"]
    if gpu_total == 0:
        return row

    assigned_gpu = 0
    gpu_per_node = row["gpu_per_node"]

    # Node-level assignment (only if nodelist is present)
    for node in row.get("nodelist", []):
        gpu_type = node_to_gpu_map.get(node)
        if isinstance(gpu_type, str):
            row[gpu_type] += gpu_per_node
            assigned_gpu += gpu_per_node

    # Calculate remaining GPUs
    remaining_gpu = gpu_total - assigned_gpu
    if remaining_gpu <= 0:
        return row

    # Fallback: TRES-level assignment
    tres_type = row.get("gpu_type_tres_per_node")
    if tres_type in gpu_types:
        row[tres_type] += remaining_gpu
        return row

    # Fallback: Partition-level assignment
    part_type = partition_to_gpu_map.get(row["partition"])
    if part_type:
        row[part_type] += remaining_gpu
        return row

    # Final fallback: mark as indeterminate
    row["indeterminate_gpu"] += remaining_gpu
    return row
   


def preprocess_sacct_data(raw_data_df, capacities_df) -> pd.DataFrame:
    gpu_types = get_gpu_types(capacities_df)
    
    # get node_to_gpu_map, but keep only entries where gpu is uniquely defined by node
    node_to_gpu_map = {
        node: gpus[0]
        for node, gpus in get_node_to_gpu_map(capacities_df).items()
        if len(gpus) == 1
    }

    # get partition_to_gpu_map, but keep only entries where gpu is uniquely defined by partition
    partition_to_gpu_map = {
        part: gpus[0]
        for part, gpus in get_partition_to_gpu_map(capacities_df).items()
        if len(gpus) == 1
    }

    # the following lines are specific to Kelvin2 to account for slurm database error
    error_file = Path("/mnt/scratch2/service-reporting/input_data/db_errors/20250609.txt")
    if error_file.exists():
        with error_file.open() as f:
            affected_jobs = [line.strip() for line in f if line.strip()]
        raw_data_df.loc[raw_data_df['JobID'].isin(affected_jobs), 'State'] = 'COMPLETED'
        raw_data_df.loc[raw_data_df['JobID'].isin(affected_jobs), 'End'] = '2025-06-09T06:00:00'

    
    df = (raw_data_df.rename(columns=str.lower)
            .assign(cpu=lambda df: df['alloctres'].str.extract(r'cpu=(\d+)').fillna(0).astype(int),
                    node=lambda df: df['alloctres'].str.extract(r'node=(\d+)').fillna(0).astype(int),
                    nodelist=lambda df: df['nodelist'].astype(str).apply(expand_nodelist).str.split(','),
                    gpu=lambda df: df['alloctres'].str.extract(r'gpu=(\d+)').fillna(0).astype(int),
                    gpu_per_node=lambda df: df["gpu"].div(df["node"]).fillna(0),
                    mem_gb=lambda df: df['alloctres'].str.extract(r'mem=(\d*\.?\d+)([KMGTP])')
                        .apply(lambda x: float(x[0]) * {'K': 1/(1000**2), 'M': 1/1000, 'G': 1, 'T': 1000}.get(x[1], 1), axis=1).fillna(0).astype(float),
                    partition_list=lambda df:df['partition'].str.split(","),
                    indeterminate_gpu=lambda df:pd.Series([0] * len(df), index=df.index),
                    submit=lambda df:pd.to_datetime(df['submit'], format='%Y-%m-%dT%H:%M:%S',errors="coerce"),
                    start=lambda df:pd.to_datetime(df['start'], format='%Y-%m-%dT%H:%M:%S',errors="coerce"),
                    end=lambda df:pd.to_datetime(df['end'], format='%Y-%m-%dT%H:%M:%S',errors="coerce"),
                    #elapsedraw=lambda x: pd.to_numeric(x["elapsedraw"], errors="coerce"), # elapsedraw not accurate due to db errors
                    )    
            .assign(elapsedraw=lambda x:(x['end'] - x['start']).dt.total_seconds())
            .assign(queue_length_sec=lambda x:(x['start'] - x['submit']).dt.total_seconds())
            .assign(scheduling_coeff=lambda x:(x['elapsedraw'].div(x['elapsedraw'] + x['queue_length_sec'])))
            .assign(**{gpu:0 for gpu in gpu_types})
            .apply(lambda row: assign_gpus(row, gpu_types, node_to_gpu_map, partition_to_gpu_map), axis=1)
            .drop(columns=['alloctres','reqtres', 'gpu_per_node']))
    return df

def get_sacct_data(path, capacities):
    path = Path(path)

    if path.is_file():
        raw_sacct_data = (
            pd.read_csv(path, sep="|", dtype=str)
              .assign(JobID=lambda df: df.JobID.astype(str))
        )

    else:
        raw_sacct_data = concat_sacct_data(path)

    return preprocess_sacct_data(raw_sacct_data, capacities)
