import subprocess
import io
import pandas as pd
import re
import shlex
import os

def _extract_capacity_data() -> io.StringIO:
    """Run `sinfo` and return cleaned node capacity data as a stream."""
    cmd = shlex.split('sinfo -a --format=%N|%P|%c|%m|%G -N')
    raw_output = subprocess.run(cmd, capture_output=True, text=True).stdout
    return io.StringIO(raw_output)

def _read_and_normalise(raw_data_file: str) -> pd.DataFrame:
    """Read Slurm output, sets datatypes, column names and converts mem to gb."""
    df = (
        pd.read_csv(
            raw_data_file,
            sep="|",
            dtype={
                "NODELIST": str,
                "PARTITION": str,
                "CPUS": int,
                "MEMORY": float,
                "GRES": str,
            },
        )
        .rename(columns={
            "NODELIST": "node",
            "PARTITION": "partition",
            "CPUS": "cpu",
            "MEMORY": "mem_mb",
            "GRES": "gres",
        })
        .assign(mem_gb=lambda d: d["mem_mb"] / 1000.0)
        .drop(columns=['mem_mb'])
    )
    return df


def _extract_gpu_entries(df: pd.DataFrame) -> pd.DataFrame:
    """Extract GPU type/count matches from the 'gres' column per row."""
    pattern = r'gpu:(?P<gpu_type>[^:,(]+):(?P<gpu_count>\d+)'
    gpu_entries = df['gres'].fillna('').str.extractall(pattern)
    gpu_entries['gpu_count'] = gpu_entries['gpu_count'].astype(int)
    return gpu_entries

def _unstack_gpu_counts(gpu_entries: pd.DataFrame) -> pd.DataFrame:
    """Aggregate GPU counts per row and pivot gpu_type into columns."""
    gpu_counts = (
        gpu_entries
        .set_index([gpu_entries.index.get_level_values(0), 'gpu_type'])['gpu_count']
        #.groupby([gpu_entries.index.get_level_values(0), 'gpu_type'])['gpu_count']
        #.sum() # this line and above would protect against multiple gpu values of same type on same record
        .unstack(fill_value=0)
        .sort_index(axis=1)
    )
    return gpu_counts.astype(int)

def _process_capacity_data(raw_data_file) -> pd.DataFrame:
    df = _read_and_normalise(raw_data_file)
    gpu_entries = _extract_gpu_entries(df)

    if gpu_entries.empty:
        return df.drop(columns=['gres']).copy()

    gpu_counts = _unstack_gpu_counts(gpu_entries)

    df = df.drop(columns=['gres']).join(gpu_counts).fillna(0)
    df[gpu_counts.columns] = df[gpu_counts.columns].astype(int)
    return df

def get_capacities() -> pd.DataFrame:
    """Return processed Slurm node capacity data as a DataFrame."""
    raw_capacity_data = _extract_capacity_data()
    processed_capacity_data = _process_capacity_data(raw_capacity_data)
    return processed_capacity_data

def get_capacity_history(directory: str) -> pd.DataFrame:
    """
    Read capacity files in the format 'capacities-YYYY_MM_DD.txt',
    process each file, and concatenate into a single DataFrame with a 'date' column.
    Missing resource values are normalized to 0.
    """
    pattern = re.compile(r'^capacities-(\d{4}_\d{2}_\d{2})\.txt$')
    all_dfs = []

    for fname in os.listdir(directory):
        match = pattern.match(fname)
        if not match:
            # Skip files not matching the hard-coded format
            continue

        date_str = match.group(1)  # e.g. "2025_08_26"
        # Convert to datetime
        date = pd.to_datetime(date_str, format="%Y_%m_%d")

        fpath = os.path.join(directory, fname)
        df = _process_capacity_data(fpath)
        df['date'] = date
        all_dfs.append(df)

    if not all_dfs:
        return pd.DataFrame()

    # Concatenate all snapshots
    combined = pd.concat(all_dfs, ignore_index=True)

    # Identify resource columns (everything except identifiers)
    resource_cols = [c for c in combined.columns if c not in ("date", "node", "partition")]

    # Replace NaN with 0 for resource columns
    combined[resource_cols] = combined[resource_cols].fillna(0)

    return combined

def expand_capacity_snapshots(history_df, start, end):
    """
    Expand each node's snapshots into daily rows until the next snapshot date.
    Partition membership changes are reflected exactly when they occur.
    Start is inclusive, end is exclusive.
    """
    filled = []
    start_date = pd.to_datetime(start)
    end_date = pd.to_datetime(end)  # exclusive

    for node, group in history_df.groupby("node"):
        g = group.sort_values("date").reset_index(drop=True)

        for idx in range(len(g)):
            row_date = g.loc[idx, "date"]

            # Determine next cutoff (next snapshot for this node)
            if idx < len(g) - 1:
                next_date = g.loc[idx + 1, "date"]
            else:
                next_date = end_date  # exclusive bound

            # Build daily range: inclusive start, exclusive end
            calendar = pd.date_range(
                start=max(start_date, row_date),
                end=min(end_date - pd.Timedelta(days=1), next_date - pd.Timedelta(days=1)),
                freq="D"
            )

            snapshot = g[g["date"] == row_date]
            for _, row in snapshot.iterrows():
                dup = pd.DataFrame([row.to_dict()] * len(calendar))
                dup["date"] = calendar
                filled.append(dup)

    return pd.concat(filled, ignore_index=True)
