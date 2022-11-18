import pandas as pd
import pandasql as ps

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
                  "delayed.{} as delayed_{}".format(attribute, attribute) for attribute in attributes
              ] + [
                  "prioritized.{} as prioritized_{}".format(attribute, attribute) for attribute in attributes
              ]
    output_column_id = 'output'
    prioritized_instances = ps.sqldf("""
        SELECT {}
        FROM event_log as delayed, event_log as prioritized
        WHERE (delayed.enabled_time < prioritized.enabled_time and 
                delayed.start_time > prioritized.start_time and 
                delayed.Resource = prioritized.Resource)
    """.format(", ".join(columns)), locals())
    # Return extended observations
    return prioritized_instances
