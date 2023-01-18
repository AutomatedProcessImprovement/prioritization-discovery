import re

import pandas as pd
from sklearn.tree import DecisionTreeClassifier, _tree


def discover_prioritization_rules(
        data: pd.DataFrame,
        outcome: str
) -> list:
    """
    Discover, incrementally, rules to set the priority level of an activity instance in such a way that; when two activity instances are
    waiting to be executed (enabled), the one with the highest priority goes first. To do this, first discover the cases in the event log
    that have been prioritized (an activity enabled after another got executed first). Then, discover the rules (based on their attributes)
    that best describe the observed prioritizations.

    :param data:    pd.DataFrame with the observations of delayed and prioritized activity instances. The two activity instances
                    of the same prioritization (e.g. a specific instance of A prioritized over a specific instance of B) share
                    the same index in the DataFrame.
    :param outcome: ID of the column with the variable to predict (1 positive, 0 negative).

    :return: a list of dicts with the priority level and the corresponding rules.
    """
    # Create empty list for the incremental models
    models = []
    # Get the data we'll be using in each iteration
    filtered_data = pd.get_dummies(data)
    dummy_columns = {column: list(data[column].unique()) for column in data.columns if column not in filtered_data.columns}
    # Extract rules level by level
    continue_search = True
    while continue_search:
        # Discover a new model for the current observations
        model = _get_rules(filtered_data, outcome)
        # If any rule has been discovered
        if len(model) > 0:
            # Save model for this priority level
            models += [model]
            # Remove all observations covered by these rules (also negative ones)
            predictions = _predict(model, filtered_data.drop([outcome], axis=1))
            true_positive_indexes = filtered_data[(filtered_data[outcome] == 1) & predictions].index
            filtered_data = filtered_data.loc[filtered_data.index.difference(true_positive_indexes)]
            # If no more prioritizations pending end search
            if len(filtered_data[filtered_data[outcome] == 1]) == 0:
                continue_search = False
        else:
            # If no rules have been discovered, end search
            continue_search = False
    # Create empty list for priority levels
    priority_levels = []
    current_lvl = 0
    for model in models:
        parsed_model = _reverse_one_hot_encoding(model, dummy_columns)
        priority_levels += [{'priority_level': current_lvl, 'rules': parsed_model}]
        current_lvl += 1
    # Return list of level rules
    return priority_levels


def _get_rules(
        data: pd.DataFrame,
        outcome: str
) -> list:
    """
    Discover one rule that lead to the positive outcome in the observations passed as argument in [data]. To do this, it uses a decision
    tree classifier to discover a rule 5 times, and gets the one with the highest confidence.

    :param data:    pd.DataFrame with one observation per row.
    :param outcome: ID of the column with the variable to predict (1 positive, 0 negative).
    :return: the discovered rules with the highest confidence.
    """
    # Discover 5 times and get the one with more confidence
    best_confidence = 0
    best_rules = []
    for i in range(5):
        # Train new model to extract 1 rule
        new_model = DecisionTreeClassifier()
        new_model.fit(data[[column for column in data.columns if column is not outcome]], data[outcome])
        best_rules = _tree_to_best_rules(new_model, [column for column in data.columns if column is not outcome])
        # If any rule has been discovered
        if len(best_rules) > 0:
            # Measure confidence
            predictions = _predict(best_rules, data.drop([outcome], axis=1))
            true_positives = [
                p and a
                for (p, a) in zip(predictions, data[outcome])
            ]
            confidence = sum(true_positives) / sum(predictions)
            # Retain if it's better than the previous one
            if confidence > best_confidence:
                best_confidence = confidence
                best_rules = best_rules
    # Return the best one, or None if no rules found in any iteration
    return best_rules


def _tree_to_best_rules(tree, feature_names) -> list:
    # Extract tree structure
    tree_ = tree.tree_
    # Get the feature used in each non-leaf node
    feature_name = [
        feature_names[i] if i != _tree.TREE_UNDEFINED else "undefined!"
        for i in tree_.feature
    ]
    # Go depth-first over the rules storing the best one
    best_rule = {'impurity': 1.0, 'sample_size': 0, 'rules': []}
    missing_nodes = [0]
    current_rules = [[]]
    while len(missing_nodes) > 0:
        current_node = missing_nodes.pop()
        current_rule = current_rules.pop()
        if tree_.feature[current_node] != _tree.TREE_UNDEFINED:
            # Decision node: add rule and keep search through each child
            name = feature_name[current_node]
            threshold = tree_.threshold[current_node]
            # Add ID and rule for left and right children
            missing_nodes += [tree_.children_left[current_node], tree_.children_right[current_node]]
            current_rules += [
                current_rule + [{'attribute': name, 'condition': '<=', 'value': threshold}],
                current_rule + [{'attribute': name, 'condition': '>', 'value': threshold}]
            ]
        else:
            # Leaf node
            current_impurity = tree_.impurity[current_node]
            current_sample_sizes = tree_.value[current_node][0]  # Number of positive samples
            # If it is the best leaf node, save it
            if (
                    current_sample_sizes[0] < current_sample_sizes[1] and  # Less samples with negative outcome
                    (current_impurity < best_rule['impurity'] or
                     (current_impurity == best_rule['impurity'] and current_sample_sizes[1] > best_rule['sample_size']))
            ):
                best_rule['impurity'] = current_impurity
                best_rule['sample_size'] = current_sample_sizes[1]
                best_rule['rules'] = _summarize_rules(current_rule)
    # Return best rules (wrapped in a list)
    return [best_rule['rules']]


def _summarize_rules(rules: list) -> list:
    filtered_rules = []
    # Merge rules by same feature
    attributes = {rule['attribute'] for rule in rules}
    for attribute in attributes:
        operators = {rule['condition'] for rule in rules if rule['attribute'] == attribute}
        if len(operators) > 1:
            # Add interval rule
            filtered_rules += [{
                'attribute': attribute,
                'condition': 'in',
                'value': "({},{}]".format(
                    max([rule['value'] for rule in rules if rule['attribute'] == attribute and rule['condition'] == ">"]),
                    min([rule['value'] for rule in rules if rule['attribute'] == attribute and rule['condition'] == "<="])
                )
            }]
        else:
            # Add single rule
            operator = operators.pop()
            values = [rule['value'] for rule in rules if rule['attribute'] == attribute]
            filtered_rules += [{
                'attribute': attribute,
                'condition': operator,
                'value': str(min(values)) if operator == "<=" else str(max(values))
            }]
    # Return rules
    return filtered_rules


def _predict(rules: list, data: pd.DataFrame) -> list:
    predictions = []
    # Predict each observation
    for index, observation in data.iterrows():
        prediction = False
        for ruleset in rules:
            if _fulfill_ruleset(ruleset, observation):
                prediction = True
        predictions += [prediction]
    # Return predictions
    return predictions


def _fulfill_ruleset(rules: list, observation: pd.Series):
    fulfills = True
    for rule in rules:
        values = [float(value) for value in re.findall(r'[\d.]+', rule['value'])]
        if (
                (rule['condition'] == "<=" and observation[rule['attribute']] > values[0]) or
                (rule['condition'] == ">" and observation[rule['attribute']] <= values[0]) or
                (rule['condition'] == "in" and observation[rule['attribute']] <= values[0]) or
                (rule['condition'] == "in" and observation[rule['attribute']] > values[1])
        ):
            fulfills = False
    return fulfills


def _reverse_one_hot_encoding(model: list, dummy_columns: dict) -> list:
    # Process dummy columns
    dummy_map = {"{}_{}".format(column, value): (column, value) for column in dummy_columns for value in dummy_columns[column]}
    for ruleset in model:
        for rule in ruleset:
            if rule['attribute'] in dummy_map:
                (orig_name, orig_value) = dummy_map[rule['attribute']]
                rule['attribute'] = orig_name
                if rule['condition'] == ">":
                    # Rule indicates that it is this value, so save it
                    rule['condition'] = "="
                    rule['value'] = orig_value
                elif len(dummy_columns[orig_name]) == 2:
                    # Only two value options, and rule indicates "not this value", so save the other
                    rule['condition'] = "="
                    rule['value'] = [value for value in dummy_columns[orig_name] if value != orig_value][0]
                else:
                    # More than two options, and rule indicates "not this value", so keep the "different that" the current one
                    rule['condition'] = "!="
                    rule['value'] = orig_value
    # Return parsed rules
    return model
