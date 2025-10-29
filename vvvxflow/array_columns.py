from pyspark import sql
from pyspark.sql import functions as F
from functools import reduce
import re
from typing import Union
from pyspark.sql import Window

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


def _rename_filters_to_passbands(df: sql.DataFrame, filter_col: str = "filterID", passband_col: str = "passband"):
    mapping = {
            1: "zEpoch",
            2: "yEpoch",
            3: "jEpoch",
            4: "hEpoch",
            5: "ksEpoch",
            8: "blank",
        }
    
    mapping_expr = F.create_map(*[F.lit(x) for kv in mapping.items() for x in kv])
    
    return df.withColumn(
        passband_col,
        mapping_expr[F.col(filter_col)])


def _rename_pivoted_columns(df: sql.DataFrame, key: str, col_name: str):
    """Append feature names to passband column names e.g. j -> jEpochMjd"""
    old_columns = [col for col in df.columns if col != key]
    new_columns = [col + f"{col_name[0].upper() + col_name[1:]}" for col in old_columns]
    return reduce(
        lambda df, idx: df.withColumnRenamed(old_columns[idx], new_columns[idx]),
        range(len(old_columns)),
        df,
    )


def _pivot_aggregate_col(
    df: sql.DataFrame,
    key: str,
    filter_col: str,
    col_name: str,
    pivot_on: str = "passband",
    order_by: str = "mjd"
) -> sql.DataFrame:
    """Aggregate column by key into lists and pivot on other column (e.g. passband)
       while preserving order.
    """

    w = Window.partitionBy(key, filter_col).orderBy(order_by)
    
    aggregated = (
        df.withColumn('sorted_list',
                             F.collect_list(col_name).over(w))
            .groupBy(key, filter_col)
            .agg(F.max("sorted_list").alias("sorted_list"))
    ).groupBy(key).pivot(filter_col).agg(F.first("sorted_list"))

    agg_renamed = _rename_pivoted_columns(df = aggregated,
                                          key=key, col_name=col_name)

    return agg_renamed


def _merge_all(aggregated_dfs: list[sql.DataFrame], join_key: str) -> sql.DataFrame:
    """Sequentially merge all dataframes in list to single df."""
    return reduce(lambda df1, df2: df1.join(df2, on=join_key), aggregated_dfs)


def make_array_cols(
    df: sql.DataFrame,
    key: str,
    order_by: str,
    filter_col: str,
    cols_to_transform: list[str],
) -> sql.DataFrame:
    """Transform df to array-valued columns.

    Args:
        df (sql.DataFrame): Dataframe in wide format.
        key (str): Unique key to group by.
        filter_col (str): Column (int) containing passbands to pivot on.
        cols_to_transform: Columns to transform

    Returns:
        sql.DataFrame: Dataframe with array-valued columns.
    """

    if df.count() < 1:
        raise ValueError("Dataframe is empty")

    for col in [key, filter_col]:
        if col not in df.columns:
            raise ValueError(f"Column {col} not in dataframe {df}")

    df = df.orderBy(key)

    # re-map filters to their passbands
    df = _rename_filters_to_passbands(df)

    aggregated_dfs = [
        _pivot_aggregate_col(
            df = df,
            key=key,
            order_by = "mjd",
            filter_col = "passband",
            col_name=col_name[0].upper() + col_name[1:],
            pivot_on="passband"
        )
        for col_name in cols_to_transform
        if col_name not in [key, filter_col, "passband"]
    ]

    merged = _merge_all(aggregated_dfs, key)
    
    return merged
