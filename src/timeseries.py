import pandas as pd

def make_sacct_timeseries(preprocessed_sacct_data, ts_res_list, report_starttime, report_endtime, freq):
    jobs = preprocessed_sacct_data.dropna(subset=['start']).copy()
    jobs.loc[:, "end"] = jobs["end"].fillna(pd.Timestamp("2100-01-01T00:00:00"))    
    jobs["end"] = jobs["end"].fillna(pd.Timestamp("2100-01-01T00:00:00"))
    util = pd.DataFrame(pd.date_range(start=report_starttime, end=report_endtime, freq = freq, inclusive="left"), columns = ['snapshot time'])
    partition_util_list = []

    for partition in jobs["partition"].unique():
        filtered = jobs[jobs["partition"] == partition]

        part_util = util.copy()
        part_util["partition"] = partition

        for resource in ts_res_list:
            part_util[resource] = (
                part_util['snapshot time']
                .apply(lambda t: filtered[(filtered['start'] <= t) & (filtered['end'] >= t)][resource].sum())
            )

        partition_util_list.append(part_util)

    return pd.concat(partition_util_list, ignore_index=True)

"""

def make_sacct_timeseries_fast(preprocessed_sacct_data, ts_res_list,
                               report_starttime, report_endtime, freq,
                               aggregate=None):
    jobs = preprocessed_sacct_data.dropna(subset=['start']).copy()
    jobs["end"] = jobs["end"].fillna(pd.Timestamp("2100-01-01T00:00:00"))

    # Build event list
    events = []
    for _, row in jobs.iterrows():
        for resource in ts_res_list:
            events.append((row['start'], row['partition'], resource, row[resource]))
            events.append((row['end'], row['partition'], resource, -row[resource]))

    events_df = pd.DataFrame(events, columns=['time','partition','resource','delta'])
    events_df = events_df.sort_values('time')

    # Pivot to wide format and cumulative sum
    util = (
        events_df.pivot_table(index=['time','partition'],
                              columns='resource',
                              values='delta',
                              aggfunc='sum')
        .fillna(0)
        .groupby(level='partition')
        .cumsum()
    )

        # Explicit hourly grid
    hourly_grid = pd.date_range(start=report_starttime, end=report_endtime, freq=freq)

    # Build MultiIndex with same order as util.index (time, partition)
    multi_index = pd.MultiIndex.from_product(
        [hourly_grid, util.index.levels[1]], names=["time", "partition"]
    )

    util_resampled = (
        util.reindex(multi_index)
            .groupby("partition")
            .ffill()
            .reset_index()
    )
    
    # Optional aggregation
    if aggregate == "daily":
        util_resampled["date"] = util_resampled["time"].dt.date
        util_resampled = (
            util_resampled.groupby(["partition","date"])
                          .mean()
                          .reset_index()
        )
    elif aggregate == "weekly":
        util_resampled["week"] = util_resampled["time"].dt.to_period("W").apply(lambda r: r.start_time)
        util_resampled = (
            util_resampled.groupby(["partition","week"])
                          .mean()
                          .reset_index()
        )

    return util_resampled

"""
