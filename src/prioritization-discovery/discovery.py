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
    event_log = event_log.copy()

    for resource, events in event_log.groupby(log_ids.resource):
        # Sort them by enabled time
        events.sort_values([log_ids.enabled_time, log_ids.start_time], inplace=True)
        # Collect events that were delayed due to others being prioritized
        delayed_events = []
        previous_event = None
        for event in events.iterrows():
            # If current event was prioritized w.r.t. previous event
            if (
                    previous_event is not None and
                    previous_event[log_ids.enabled_time] < event[log_ids.enabled_time] and
                    previous_event[log_ids.start_time] > event[log_ids.start_time]
            ):
                # Save event to analyze
                delayed_events += [previous_event]
            # Jump to next event
            previous_event = event
        # For each delayed event, make a group with the ones prioritized to it
        for delayed_event in delayed_events:
            # Get events prioritized w.r.t. the delayed one
            prioritized_events = event_log[
                (event_log[log_ids.enabled_time] > delayed_event[log_ids.enabled_time]) &
                (event_log[log_ids.start_time] < delayed_event[log_ids.start_time])
                ]
            # TODO Save info about the prioritized group
    return event_log
