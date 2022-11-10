import pandas as pd
import pandasql as ps
from pandas.core.dtypes.common import is_numeric_dtype

from .config import EventLogIDs


def discover_prioritized_instances(
        event_log: pd.DataFrame,
        log_ids: EventLogIDs,
        attributes: list[str]
) -> pd.DataFrame:
    """
    Discover activity instances that are prioritized over others. This means they are not being executed following a FIFO order, i.e., in
    the order they are enabled.

    :param event_log:   event log to analyze.
    :param log_ids:     mapping with the IDs of each column in the dataset.
    :param attributes:  list of column names for the attributes to use as features for the prioritization

    :return: a pd.DataFrame with each of the observations (positive and negative) of prioritization found in the event log.
    """
    columns = [
                  "delayed.{} as delayed_event_{}".format(attribute, attribute) for attribute in attributes
              ] + [
                  "prioritized.{} as priorit_event_{}".format(attribute, attribute) for attribute in attributes
              ]
    output_column_id = 'output'
    prioritizations = ps.sqldf("""
        SELECT {}
        FROM event_log as delayed, event_log as prioritized
        WHERE (delayed.enabled_time < prioritized.enabled_time and 
                delayed.start_time > prioritized.start_time and 
                delayed.Resource = prioritized.Resource)
    """.format(", ".join(columns)), locals())
    prioritizations[output_column_id] = 1
    non_prioritizations = ps.sqldf("""
        SELECT {}
        FROM event_log as delayed, event_log as prioritized
        WHERE (delayed.enabled_time < prioritized.enabled_time and 
                delayed.start_time >= prioritized.enabled_time and 
                delayed.start_time < prioritized.start_time and 
                delayed.Resource = prioritized.Resource)
    """.format(", ".join(columns)), locals())
    non_prioritizations[output_column_id] = 0
    # Concatenate of prioritized and non prioritized
    observations = pd.concat([prioritizations, non_prioritizations]).reset_index(drop=True)
    # Combine number columns between them
    numeric_columns = [
        column
        for column in observations.columns
        if column != output_column_id and is_numeric_dtype(observations[column].dtype)
    ]
    unvisited_columns = list(numeric_columns)
    for column_1 in numeric_columns:
        unvisited_columns.remove(column_1)
        for column_2 in unvisited_columns:
            observations['{} / {}'.format(column_1, column_2)] = observations[column_1] / observations[column_2]
    # Return extended observations
    return observations
