from pyspark import sql
from functools import reduce
import re
from typing import Union

"""Transform dataframe with passbands into array-valued columns."""


def _cast_by_regex(
    df: sql.DataFrame, pattern: re.Pattern[str], cast_to: sql.types.DataType
):
    """Cast columns that match pattern to desired type"""
    df = df.select(
        *(
            (
                sql.functions.col(c).cast(cast_to).alias(c)
                if re.match(pattern, c)
                else sql.functions.col(c)
            )
            for c in df.columns
        )
    )
    return df


def _cast_columns(df: sql.DataFrame, columns: list[str], cast_to: sql.types.DataType):
    """Cast columns that match pattern to desired type"""
    df = df.select(
        *(
            (
                sql.functions.col(c).cast(cast_to).alias(c)
                if c in columns
                else sql.functions.col(c)
            )
            for c in df.columns
        )
    )
    return df


def _transform_passbands(
    df: sql.DataFrame, filter_col: str = "filterID", new_col_name: str = "passband"
):
    """Convert numeric filter references to letter-encoded passbands"""
    passbands_dict = {
        "1": "zEpoch",
        "2": "yEpoch",
        "3": "jEpoch",
        "4": "hEpoch",
        "5": "ksEpoch",
        "8": "blank",
    }
    return df.withColumn(new_col_name, df[filter_col].cast("string")).replace(
        passbands_dict, subset=[new_col_name]
    )


def _rename_pivoted_columns(df: sql.DataFrame, key: str, col_name: str):
    """Append feature names to passband column names e.g. j -> jEpochmjd"""
    old_columns = [col for col in df.columns if col != key]
    new_columns = [col + f"{col_name}" for col in old_columns]
    return reduce(
        lambda df, idx: df.withColumnRenamed(old_columns[idx], new_columns[idx]),
        range(len(old_columns)),
        df,
    )


def _pivot_aggregate_col(
    grouped_df: sql.group.GroupedData,
    key: str,
    col_name: str,
    pivot_on: str = "passband",
) -> sql.DataFrame:
    """Aggregate column by key into lists and pivot on other column e.g. passband"""
    aggregated = grouped_df.pivot(pivot_on).agg(sql.functions.collect_list(col_name))
    return _rename_pivoted_columns(df=aggregated, key=key, col_name=col_name)


def _merge_all(aggregated_dfs: list[sql.DataFrame], join_key: str) -> sql.DataFrame:
    """Sequentially merge all dataframes in list to single df."""
    return reduce(lambda df1, df2: df1.join(df2, on=join_key), aggregated_dfs)


def make_array_cols(
    df: sql.DataFrame,
    key: str,
    filter_col: str,
    cols_to_transform: list[str],
    order_by: Union[str, None] = None,
) -> sql.DataFrame:
    """Transform df to array-valued columns.

    Args:
        df (sql.DataFrame): Dataframe in wide format.
        key (str): Unique key to group by.
        filter_col (str): Column (int) containing passbands to pivot on.
        cols_to_transform: Columns to transform
        order_by (str): Column to sort data by (thus sorting resulting arrays on this column).

    Returns:
        sql.DataFrame: Dataframe with array-valued columns.
    """

    if df.count() < 1:
        raise ValueError("Dataframe is empty")

    for col in [key, filter_col]:
        if col not in df.columns:
            raise ValueError(f"Column {col} not in dataframe {df}")

    if order_by:
        df = df.orderBy(order_by)
        if order_by not in df.columns:
            raise ValueError(f"Column {order_by} not in dataframe {df}")

    grouped = (
        _transform_passbands(df=df, filter_col=filter_col, new_col_name="passband")
        .drop(filter_col)
        .groupBy(key)
    )

    aggregated_dfs = [
        _pivot_aggregate_col(
            grouped_df=grouped,
            key=key,
            col_name=col_name[0].upper() + col_name[1:],
            pivot_on="passband",
        )
        for col_name in cols_to_transform
        if col_name not in [key, filter_col, "passband"]
    ]

    merged = _merge_all(aggregated_dfs, key)
    return merged
