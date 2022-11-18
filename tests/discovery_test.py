import pandas as pd

from prioritization_discovery.config import DEFAULT_CSV_IDS
from prioritization_discovery.discovery import discover_prioritized_instances


def test_discover_prioritized_instances():
    # Read event log
    event_log = pd.read_csv("./tests/assets/event_log_1.csv")
    event_log[DEFAULT_CSV_IDS.enabled_time] = pd.to_datetime(event_log[DEFAULT_CSV_IDS.enabled_time], utc=True)
    event_log[DEFAULT_CSV_IDS.start_time] = pd.to_datetime(event_log[DEFAULT_CSV_IDS.start_time], utc=True)
    event_log[DEFAULT_CSV_IDS.end_time] = pd.to_datetime(event_log[DEFAULT_CSV_IDS.end_time], utc=True)
    # Discover prioritization
    attributes = [DEFAULT_CSV_IDS.activity]
    prioritizations = discover_prioritized_instances(event_log, DEFAULT_CSV_IDS, attributes)
    prioritizations.sort_values(['delayed_Activity', 'prioritized_Activity'], inplace=True)
    prioritizations.reset_index(drop=True, inplace=True)
    assert prioritizations.equals(pd.DataFrame(
        [["B", "C"], ["B", "C"], ["B", "C"], ["B", "C"], ["B", "C"], ["B", "C"]],
        columns=['delayed_Activity', 'prioritized_Activity']
    ))


def test_discover_prioritized_instances_with_extra_attribute():
    # Read event log
    event_log = pd.read_csv("./tests/assets/event_log_2.csv")
    event_log[DEFAULT_CSV_IDS.enabled_time] = pd.to_datetime(event_log[DEFAULT_CSV_IDS.enabled_time], utc=True)
    event_log[DEFAULT_CSV_IDS.start_time] = pd.to_datetime(event_log[DEFAULT_CSV_IDS.start_time], utc=True)
    event_log[DEFAULT_CSV_IDS.end_time] = pd.to_datetime(event_log[DEFAULT_CSV_IDS.end_time], utc=True)
    # Discover prioritization
    attributes = [DEFAULT_CSV_IDS.activity, 'loan_amount']
    prioritizations = discover_prioritized_instances(event_log, DEFAULT_CSV_IDS, attributes)
    prioritizations.sort_values(
        ['delayed_Activity', 'prioritized_Activity', 'delayed_loan_amount', 'prioritized_loan_amount'],
        inplace=True
    )
    prioritizations.reset_index(drop=True, inplace=True)
    assert prioritizations.equals(
        pd.DataFrame(
            [
                ['A', 500, 'B', 1000],
                ['A', 500, 'C', 1000],
                ['B', 100, 'A', 500],
                ['B', 100, 'B', 500],
                ['B', 100, 'B', 1000],
                ['B', 100, 'C', 500],
                ['B', 100, 'C', 1000]
            ],
            columns=[
                'delayed_Activity',
                'delayed_loan_amount',
                'prioritized_Activity',
                'prioritized_loan_amount'
            ]
        )
    )
