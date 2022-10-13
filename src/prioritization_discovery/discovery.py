import pandas as pd

from .config import EventLogIDs


def _get_features(current_event: pd.Series, potential_prioritized_event: pd.Series, attributes: list[str]):
    return [
               current_event[attribute] for attribute in attributes
           ] + [
               potential_prioritized_event[attribute] for attribute in attributes
           ]


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
    event_log = event_log.copy()
    columns = [
                  "delayed_event_{}".format(attribute) for attribute in attributes
              ] + [
                  "priorit_event_{}".format(attribute) for attribute in attributes
              ] + ["output"]
    prioritizations, non_prioritizations = [], []
    # Analyze grouped by resource
    for resource, events in event_log.groupby(log_ids.resource):
        # Sort them by enabled time
        events.sort_values([log_ids.enabled_time, log_ids.start_time], inplace=True)
        # For each event that could have others prioritized
        previous_event = None
        for index, event in events.iterrows():
            # If following event is enabled after current start
            if previous_event is not None:
                # Get events prioritized w.r.t. the previous one
                prioritized_events = events[
                    (events[log_ids.enabled_time] > previous_event[log_ids.enabled_time]) &
                    (events[log_ids.start_time] < previous_event[log_ids.start_time])
                    ]
                if len(prioritized_events) > 0:
                    prioritizations += [
                        _get_features(previous_event, prioritized_event, attributes) + [1]
                        for _, prioritized_event in prioritized_events.iterrows()
                    ]
                # Get event that could be prioritized but weren't
                non_prioritized_events = events[
                    (events[log_ids.enabled_time] > previous_event[log_ids.enabled_time]) &
                    (events[log_ids.enabled_time] <= previous_event[log_ids.start_time]) &
                    (events[log_ids.start_time] > previous_event[log_ids.start_time])
                    ]
                if len(non_prioritized_events) > 0:
                    non_prioritizations += [
                        _get_features(previous_event, non_prioritized_event, attributes) + [0]
                        for _, non_prioritized_event in non_prioritized_events.iterrows()
                    ]
            # Jump to next event
            previous_event = event
    # Return a dataframe with the prioritized and not prioritized elements
    return pd.DataFrame(prioritizations + non_prioritizations, columns=columns)
