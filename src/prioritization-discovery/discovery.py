import pandas as pd

from .config import EventLogIDs


def discover_prioritized_instances(event_log: pd.DataFrame, log_ids: EventLogIDs) -> pd.DataFrame:
    """
    Discover activity instances that are prioritized over others. This means they are not being executed following a FIFO order, i.e., in
    the order they are enabled.

    :param event_log:   event log to analyze.
    :param log_ids:     mapping with the IDs of each column in the dataset.
    :return: a copy of [event_log] with a column identifying the activities involved in prioritization.
    """
    return event_log.copy()
