"""
Helper functions for working with cluster capacity DataFrames.

This module provides convenience utilities for extracting GPU type information
from the processed capacity data, and for building lookup maps such as:

- get_gpu_types: returns a sorted list of GPU resource columns
- get_node_to_gpu_map: maps each node to the GPU types it provides
- get_partition_to_gpu_map: maps each partition to the GPU types available
  across its nodes

These helpers are used by the queue preprocessing logic to assign jobs to
specific GPU resources where possible.
"""

import pandas as pd

def get_gpu_types(capacity_df: pd.DataFrame) -> list[str]:
    """
    Return a sorted list of GPU type columns from the processed capacity DataFrame.
    """
    non_gpu_cols = {"node", "partition", "cpu", "mem_gb"}
    return sorted([c for c in capacity_df.columns if c not in non_gpu_cols])


def get_node_to_gpu_map(capacity_df: pd.DataFrame) -> dict[str, list[str]]:
    """
    Map each node to a list of GPU types it has (count > 0).
    """
    gpu_cols = get_gpu_types(capacity_df)
    return {
        row["node"]: [gpu for gpu in gpu_cols if row[gpu] > 0]
        for _, row in capacity_df.iterrows()
    }


def get_partition_to_gpu_map(capacity_df: pd.DataFrame) -> dict[str, list[str]]:
    """
    Map each partition to a list of GPU types present in any node in that partition.
    """
    gpu_cols = get_gpu_types(capacity_df)
    partition_map: dict[str, set[str]] = {}

    for _, row in capacity_df.iterrows():
        p = row["partition"]
        partition_map.setdefault(p, set())
        for gpu in gpu_cols:
            if row[gpu] > 0:
                partition_map[p].add(gpu)

    return {p: sorted(types) for p, types in partition_map.items()}
