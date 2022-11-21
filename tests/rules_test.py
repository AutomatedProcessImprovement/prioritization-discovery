import pandas as pd

from prioritization_discovery.rules import discover_prioritization_rules


def test_discover_prioritization_rules():
    # Given a set of prioritizations
    prioritizations = pd.DataFrame(
        data=[
            ['B', 0], ['B', 0], ['B', 0],
            ['B', 0], ['B', 0], ['B', 0],
            ['C', 1], ['C', 1], ['C', 1],
            ['C', 1], ['C', 1], ['C', 1]
        ],
        index=[0, 1, 2, 3, 4, 5, 0, 1, 2, 3, 4, 5],
        columns=['Activity', 'outcome']
    )
    # Discover their rules
    prioritization_rules = discover_prioritization_rules(prioritizations, 'outcome')
    # Assert the rules
    assert prioritization_rules == [
        {
            'priority_level': 1,
            'rules': [
                [
                    {
                        'attribute': 'Activity',
                        'condition': '=',
                        'value': 'C'
                    }
                ]
            ]
        }
    ]


def test_discover_prioritization_rules_with_extra_attribute():
    # Given a set of prioritizations
    prioritizations = pd.DataFrame(
        pd.DataFrame(
            data=[
                ['A', 500, 0],
                ['A', 500, 0],
                ['A', 500, 1],
                ['B', 100, 0],
                ['B', 100, 0],
                ['B', 100, 0],
                ['B', 100, 0],
                ['B', 100, 0],
                ['B', 500, 1],
                ['B', 1000, 1],
                ['B', 1000, 1],
                ['C', 100, 0],
                ['C', 500, 1],
                ['C', 500, 1],
                ['C', 1000, 1],
                ['C', 1000, 1]
            ],
            index=[0, 1, 2, 2, 3, 4, 5, 6, 4, 0, 3, 7, 6, 7, 1, 5],
            columns=['Activity', 'loan_amount', 'outcome']
        )
    )
    # Discover their rules
    prioritization_rules = discover_prioritization_rules(prioritizations, 'outcome')
    # Assert the rules
    assert prioritization_rules == prioritization_rules == [
        {
            'priority_level': 2,
            'rules': [
                [
                    {
                        'attribute': 'loan_amount',
                        'condition': '=',
                        'value': '1000'
                    }
                ]
            ]
        },
        {
            'priority_level': 1,
            'rules': [
                [
                    {
                        'attribute': 'loan_amount',
                        'condition': '=',
                        'value': '500'
                    }
                ]
            ]
        }
    ]
