import pandas as pd
import wittgenstein as lw
from wittgenstein import RIPPER


def discover_prioritization_rules(
        data: pd.DataFrame,
        outcome: str
) -> list:
    """
    Discover, incrementally, rules to set the priority level of an activity instance in such a way that; when two activity instances are
    waiting to be executed (enabled), the one with highest priority goes first. To do this, first discover the cases in the event log
    that have been prioritized (an activity enabled after another got executed first). Then, discover the rules (based on their attributes)
    that best describe the observed prioritizations.

    :param data:                pd.DataFrame with the observations of delayed and prioritized activity instances. The two activity instances
                                of the same prioritization (e.g. a specific instance of A prioritized over a specific instance of B) share
                                the same index in the DataFrame.
    :param outcome              ID of the column with the variable to predict (1 positive, 0 negative).
    :param max_rules:           Maximum number of rules to extract for a specific priority level.

    :return: a list of dicts with the priority level and the corresponding rules.
    """
    # Create empty list for the incremental models
    ripper_models = []
    # Get the data we'll be using in each iteration
    filtered_data = data
    # Extract rules level by level
    continue_search = True
    while continue_search:
        # Reset the index to not have duplicated ones, RIPPER crashes with that
        clean_filtered_data = filtered_data.reset_index(drop=True)
        # Discover a new model for the current observations
        ripper_model = _get_rules(clean_filtered_data, outcome)
        # --ripper_model = lw.RIPPER(max_rules=max_rules)
        # --ripper_model.fit(clean_filtered_data, class_feat=outcome)
        # If any rule has been discovered
        if len(ripper_model.ruleset_.rules) > 0:
            # Save model for this priority level
            ripper_models += [ripper_model]
            # Remove all observations covered by these rules (also negative ones)
            predictions = ripper_model.predict(filtered_data.drop([outcome], axis=1).reset_index())  # Don't drop old index on purpose
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
    current_lvl = len(ripper_models)
    for ripper_model in ripper_models:
        priority_levels += [{'priority_level': current_lvl, 'rules': _parse_rules(ripper_model)}]
        current_lvl -= 1
    # Return list of level rules
    return priority_levels


def _get_rules(
        data: pd.DataFrame,
        outcome: str
) -> RIPPER:
    """
    Discover one rule that lead to the positive outcome in the observations passed as argument in [data]. To do this, it uses RIPPER to
    discover a rule 5 times, and gets the one with the highest confidence.

    :param data:                pd.DataFrame with one observation per row.
    :param outcome              ID of the column with the variable to predict (1 positive, 0 negative).
    :return: the RIPPER model with the discovered rule with the highest confidence.
    """
    # Discover 5 times and get the one with more confidence
    best_confidence = 0
    best_model = None
    for i in range(5):
        # Train new model to extract 1 rule
        new_model = lw.RIPPER(max_rules=5)
        new_model.fit(data, class_feat=outcome)
        # If any rule has been discovered
        if len(new_model.ruleset_.rules) > 0:
            # Measure confidence
            predictions = new_model.predict(data.drop([outcome], axis=1))
            true_positives = [
                p and a
                for (p, a) in zip(predictions, data[outcome])
            ]
            confidence = sum(true_positives) / sum(predictions)
            # Retain if it's better than the previous one
            if confidence > best_confidence:
                best_confidence = confidence
                best_model = new_model
    # Return the best one, or None if no rules found in any iteration
    return best_model


def _parse_rules(model) -> list:
    """
    Transform the rules from a RIPPER model into a list of sublists (OR of ANDs), where the rule is fulfilled when one
    of the sublists (OR) have all its rules met (AND).

    :param model: RIPPER model to transform.
    :return: list of sublists with the rules.
    """
    rules = []
    # Go over the rules transforming them
    for ruleset in model.ruleset_.rules:
        # For each set of rules (sublist)
        sublist = []
        for condition in ruleset.conds:
            operator, value = None, None
            if _is_number(condition.val):
                # Single number
                operator = "="
                value = str(condition.val)
            elif condition.val[0] == "<" and _is_number(condition.val[1:]):
                # If starts with '<' and the rest of the string is a number, set it as "lower than" that number
                operator = "<="
                value = condition.val[1:]
            elif condition.val[0] == ">" and _is_number(condition.val[1:]):
                # If starts with '>' and the rest of the string is a number, set it as "greater than" that number
                operator = ">="
                value = condition.val[1:]
            else:
                # Try interval
                indexes = [i for i, char in enumerate(condition.val) if i > 0 and char == "-" and condition.val[i - 1] != 'e']
                if len(indexes) == 1:
                    index = indexes[0]
                    if _is_number(condition.val[:index]) and _is_number(condition.val[index + 1:]):
                        # Interval
                        operator = "in"
                        value = [condition.val[:index], condition.val[index + 1:]]
            # Couldn't get any number format, set as "equals" to a string
            if not operator or not value:
                # String
                operator = "="
                value = condition.val
            sublist += [{'attribute': condition.feature, 'condition': operator, 'value': value}]
        # Add sublist of rules to complete
        rules += [sublist]
    # Return the rules
    return rules


def _is_number(value: str) -> bool:
    """
    Check if the string passed is a number (integer, float, scientific notation, etc.).

    :param value: string to check.

    :return: True if [value] is a number.
    """
    try:
        float(value)
        return True
    except ValueError:
        return False
