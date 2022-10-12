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
    prioritizations = []
    # Analyze grouped by resource
    for resource, events in event_log.groupby(log_ids.resource):
        # Sort them by enabled time
        events.sort_values([log_ids.enabled_time, log_ids.start_time], inplace=True)
        # For each event that could have others prioritized
        previous_event = None
        for index, event in events.iterrows():
            # If following events are enabled after current start
            if previous_event is not None and previous_event[log_ids.start_time] > event[log_ids.enabled_time]:
                # Get events prioritized w.r.t. the previous one
                prioritized_events = event_log[
                    (event_log[log_ids.enabled_time] > previous_event[log_ids.enabled_time]) &
                    (event_log[log_ids.start_time] < previous_event[log_ids.start_time])
                    ]
                # Save info about the prioritized group
                print("{} prioritized over {}".format([e[log_ids.activity] for e in prioritized_events], previous_event[log_ids.activity]))
                prioritizations += [(previous_event, prioritized_events)]
            # Jump to next event
            previous_event = event
    return prioritizations
