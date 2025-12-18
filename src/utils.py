"""
Utility functions for parsing Slurm‑style strings.

Currently includes:
- expand_nodelist: expands compact nodelist syntax (e.g. 'node[01-03]')
  into explicit node names.
"""

import re

def expand_nodelist(nodelist: str) -> str:
    """Expand SLURM-style nodelist (e.g. 'gpu[1-2]') into full node names."""
    # Return unchanged if there's no need for expansion
    if not nodelist or '[' not in nodelist:
        return nodelist

    # Extract base name (e.g. node, gpu, smp) and numeric ranges
    match = re.match(r'(\w+)\[([\d,-]+)\]', nodelist)
    if not match:
        raise ValueError("Invalid nodelist format")

    base_name, ranges = match.groups()

    # Expand numeric ranges
    expanded_nodes = []
    for part in ranges.split(','):
        if '-' in part:
            start, end = part.split('-')
            for i in range(int(start), int(end) + 1):
                expanded_nodes.append(f"{base_name}{i}")
        else:
            expanded_nodes.append(f"{base_name}{part}")

    # Join expanded nodes into a final string
    return ','.join(expanded_nodes)
