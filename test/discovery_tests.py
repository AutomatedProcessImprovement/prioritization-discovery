import pandas as pd

from prioritization_discovery.config import DEFAULT_CSV_IDS
from prioritization_discovery.discovery import discover_prioritized_instances


def test_discover_prioritized_instances():
    # Read event log
    event_log = pd.read_csv("./assets/event_log_1.csv")
    event_log[DEFAULT_CSV_IDS.enabled_time] = pd.to_datetime(event_log[DEFAULT_CSV_IDS.enabled_time], utc=True)
    event_log[DEFAULT_CSV_IDS.start_time] = pd.to_datetime(event_log[DEFAULT_CSV_IDS.start_time], utc=True)
    event_log[DEFAULT_CSV_IDS.end_time] = pd.to_datetime(event_log[DEFAULT_CSV_IDS.end_time], utc=True)
    # Discover prioritization
    attributes = [DEFAULT_CSV_IDS.activity]
    prioritizations = discover_prioritized_instances(event_log, DEFAULT_CSV_IDS, attributes)
    prioritizations.sort_values(['delayed_event_Activity', 'priorit_event_Activity', 'output'], inplace=True)
    prioritizations.reset_index(drop=True, inplace=True)
    assert prioritizations.equals(pd.DataFrame(
        [["A", "B", 0], ["A", "B", 0], ["A", "B", 0],
         ["A", "B", 0], ["A", "B", 0], ["A", "B", 0],
         ["B", "B", 0], ["B", "B", 0], ["B", "B", 0],
         ["B", "B", 0], ["B", "B", 0], ["B", "B", 0],
         ["B", "C", 1], ["B", "C", 1], ["B", "C", 1],
         ["B", "C", 1], ["B", "C", 1], ["B", "C", 1]],
        columns=['delayed_event_Activity', 'priorit_event_Activity', 'output']
    ))


def test_discover_prioritized_instances_with_extra_attribute():
    # Read event log
    event_log = pd.read_csv("./assets/event_log_2.csv")
    event_log[DEFAULT_CSV_IDS.enabled_time] = pd.to_datetime(event_log[DEFAULT_CSV_IDS.enabled_time], utc=True)
    event_log[DEFAULT_CSV_IDS.start_time] = pd.to_datetime(event_log[DEFAULT_CSV_IDS.start_time], utc=True)
    event_log[DEFAULT_CSV_IDS.end_time] = pd.to_datetime(event_log[DEFAULT_CSV_IDS.end_time], utc=True)
    # Discover prioritization
    attributes = [DEFAULT_CSV_IDS.activity, 'loan_amount']
    prioritizations = discover_prioritized_instances(event_log, DEFAULT_CSV_IDS, attributes)
    prioritizations.sort_values(
        ['delayed_event_Activity', 'priorit_event_Activity', 'delayed_event_loan_amount', 'priorit_event_loan_amount'],
        inplace=True
    )
    prioritizations.reset_index(drop=True, inplace=True)
    assert prioritizations.equals(pd.DataFrame(
        [
            ['A', 500, 'B', 1000, 1],
            ['A', 1000, 'B', 100, 0],
            ['A', 500, 'C', 1000, 1],
            ['B', 100, 'A', 500, 1],
            ['B', 100, 'B', 500, 1],
            ['B', 100, 'B', 1000, 1],
            ['B', 100, 'C', 500, 1],
            ['B', 100, 'C', 1000, 1]
        ],
        columns=['delayed_event_Activity', 'delayed_event_loan_amount', 'priorit_event_Activity', 'priorit_event_loan_amount', 'output']
    ))
