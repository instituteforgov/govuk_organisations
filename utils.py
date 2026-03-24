# %%
import pandas as pd

# %%
# Functions to be used for editing parent/child org columns and matching to UUIDs from df_sql


def flatten_list_of_dicts(df: pd.DataFrame, col: str, key: str) -> pd.DataFrame:

    """
    'Flatten' a specified column of a DataFrame which is a list of dictionaries with the same key
    E.g., entries in a column may be of a form like [{key:A}, {key:B}, {key:C}, ...]
    This function edits the entries to be of the form [A,B,C, ...]

    Parameters
        - df: the DataFrame in question
        - col: columns of the dataframe containing lists of dicts
        - key: The key shared across all dicts in the column

    Returns:
        - df: Original DataFrame with formatted column
    """

    df[col] = df[col].apply(
        lambda x: [item[key] for item in x]
    )

    return df


def remove_prefixes(df: pd.DataFrame, col: str, prefix: str) -> pd.DataFrame:

    """
    Get rid of a prefix substring from a column containing lists of strings
    E.g., [https://www.gov.uk/api/organisations/ministy_of_justice] -> [ministry_of_justice]

    Parameters:
        - df: Dataframe in question
        - col: Column of df comprising lists of strings
        - prefix: String prefix/substring to be removed

    Returns:
        - df: Oringinal DataFrame with formatted col
    """

    df[col] = df[col].apply(
        lambda lst: [x.replace(prefix, '') for x in lst if prefix in x]
    )

    return df


def match_and_replace(
        df1: pd.DataFrame,
        df2: pd.DataFrame,
        edit_col: str, key_col: str,
        val_col: str
        ) -> pd.DataFrame:
    """
    For the entries in df2[edit_col], find the matching entry in df1[key_col],
    find the entry in val_col in that row, then replace the original entry in df2[edit_col]
    with the matched entry from df1[val_col]

    Parameters:
        - df1: The Dataframe containing info to copy
        - df2: The DataFrame to be edited using info from df1
        - edit_col: The column in df2 to be replaced
        - key_col: The column in df1 matching edit_col
        - val_col: The column in df1 containing the info to copy

    Returns:
        - df2
    """

    name_to_id = dict(zip(df1[key_col], df1[val_col]))
    df2[edit_col] = df2[edit_col].apply(
        lambda lst: [name_to_id.get(name) for name in lst]
    )

    return df2
