import pytest
from pyspark.testing import assertDataFrameEqual
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    FloatType,
    StringType,
    ArrayType,
)
from vvx_etl.array_columns import (
    _transform_passbands,
    _pivot_aggregate_col,
    _merge_all,
    make_array_cols,
)
from spark_fixture import spark_fixture


@pytest.fixture
def sample_data(spark_fixture):

    schema = StructType(
        [
            StructField("sourceID", IntegerType(), False),
            StructField("filterID", IntegerType(), False),
            StructField("mjd", DoubleType(), False),
            StructField("aperMag1", FloatType(), False),
        ]
    )

    return spark_fixture.createDataFrame(
        [
            (1, 1, 1.234, 2.567),
            (1, 2, 3.234, 4.567),
            (1, 3, 5.234, 6.567),
            (1, 4, 7.234, 8.567),
            (1, 5, 9.234, 10.567),
            (2, 1, 1.234, 2.567),
            (2, 2, 3.234, 4.567),
            (2, 3, 5.234, 6.567),
            (2, 4, 7.234, 8.567),
            (2, 5, 9.234, 10.567),
            (3, 1, 1.234, 2.567),
            (3, 2, 3.234, 4.567),
            (3, 3, 5.234, 6.567),
            (3, 4, 7.234, 8.567),
            (3, 5, 9.234, 10.567),
        ],
        schema=schema,
    )


def test_transform_passbands(sample_data, spark_fixture):

    actual = _transform_passbands(
        sample_data, filter_col="filterID", new_col_name="passband"
    )

    expected_schema = StructType(
        [
            StructField("sourceID", IntegerType(), False),
            StructField("filterID", IntegerType(), False),
            StructField("mjd", DoubleType(), False),
            StructField("aperMag1", FloatType(), False),
            StructField("passband", StringType(), False),
        ]
    )
    expected = spark_fixture.createDataFrame(
        [
            (1, 1, 1.234, 2.567, "zEpoch"),
            (1, 2, 3.234, 4.567, "yEpoch"),
            (1, 3, 5.234, 6.567, "jEpoch"),
            (1, 4, 7.234, 8.567, "hEpoch"),
            (1, 5, 9.234, 10.567, "ksEpoch"),
            (2, 1, 1.234, 2.567, "zEpoch"),
            (2, 2, 3.234, 4.567, "yEpoch"),
            (2, 3, 5.234, 6.567, "jEpoch"),
            (2, 4, 7.234, 8.567, "hEpoch"),
            (2, 5, 9.234, 10.567, "ksEpoch"),
            (3, 1, 1.234, 2.567, "zEpoch"),
            (3, 2, 3.234, 4.567, "yEpoch"),
            (3, 3, 5.234, 6.567, "jEpoch"),
            (3, 4, 7.234, 8.567, "hEpoch"),
            (3, 5, 9.234, 10.567, "ksEpoch"),
        ],
        schema=expected_schema,
    )

    assertDataFrameEqual(actual, expected)


def test_pivot_aggregate_col(sample_data, spark_fixture):
    df = _transform_passbands(
        sample_data, filter_col="filterID", new_col_name="passband"
    )
    grouped = df.groupBy("sourceID")
    actual = _pivot_aggregate_col(
        grouped, key="sourceID", col_name="mjd", pivot_on="passband"
    )

    expected_schema = StructType(
        [
            StructField("sourceID", IntegerType(), False),
            StructField("hEpochmjd", ArrayType(DoubleType(), True), False),
            StructField("jEpochmjd", ArrayType(DoubleType(), True), False),
            StructField("ksEpochmjd", ArrayType(DoubleType(), True), False),
            StructField("yEpochmjd", ArrayType(DoubleType(), True), False),
            StructField("zEpochmjd", ArrayType(DoubleType(), True), False),
        ]
    )
    expected = spark_fixture.createDataFrame(
        [
            (1, [7.234], [5.234], [9.234], [3.234], [1.234]),
            (2, [7.234], [5.234], [9.234], [3.234], [1.234]),
            (3, [7.234], [5.234], [9.234], [3.234], [1.234]),
        ],
        schema=expected_schema,
    )

    assertDataFrameEqual(actual, expected)


def test_merge_all(spark_fixture):

    agg_df = spark_fixture.createDataFrame(
        [
            (1, [0.1, 0.2, 0.3], [0.1, 0.2, 0.3], [0.1, 0.2, 0.3]),
            (2, [0.4, 0.5, 0.6], [0.1, 0.2, 0.3], [0.1, 0.2, 0.3]),
        ],
        ["id", "col1", "col2", "col3"],
    )

    df1 = agg_df.select("id", "col1")
    df2 = agg_df.select("id", "col2")
    df3 = agg_df.select("id", "col3")

    merged = _merge_all([df1, df2, df3], "id")

    assertDataFrameEqual(merged, agg_df)


def test_make_array_cols(spark_fixture, sample_data):

    expected_schema = StructType(
        [
            StructField("sourceID", IntegerType(), False),
            StructField("hEpochMjd", ArrayType(DoubleType()), False),
            StructField("jEpochMjd", ArrayType(DoubleType()), False),
            StructField("ksEpochMjd", ArrayType(DoubleType()), False),
            StructField("yEpochMjd", ArrayType(DoubleType()), False),
            StructField("zEpochMjd", ArrayType(DoubleType()), False),
            StructField("hEpochAperMag1", ArrayType(FloatType()), False),
            StructField("jEpochAperMag1", ArrayType(FloatType()), False),
            StructField("ksEpochAperMag1", ArrayType(FloatType()), False),
            StructField("yEpochAperMag1", ArrayType(FloatType()), False),
            StructField("zEpochAperMag1", ArrayType(FloatType()), False),
        ]
    )

    expected = spark_fixture.createDataFrame(
        [
            (
                1,
                [7.234],
                [5.234],
                [9.234],
                [3.234],
                [1.234],
                [8.567],
                [6.567],
                [10.567],
                [4.567],
                [2.567],
            ),
            (
                2,
                [7.234],
                [5.234],
                [9.234],
                [3.234],
                [1.234],
                [8.567],
                [6.567],
                [10.567],
                [4.567],
                [2.567],
            ),
            (
                3,
                [7.234],
                [5.234],
                [9.234],
                [3.234],
                [1.234],
                [8.567],
                [6.567],
                [10.567],
                [4.567],
                [2.567],
            ),
        ],
        schema=expected_schema,
    )
    actual = make_array_cols(
        sample_data,
        key="sourceID",
        filter_col="filterID",
        cols_to_transform=["mjd", "aperMag1"],
    )
    assertDataFrameEqual(actual, expected)


def test_make_array_cols_with_bad_colname(spark_fixture, sample_data):
    with pytest.raises(ValueError):
        make_array_cols(
            sample_data,
            key="this_key_should_throw_an_exception",
            filter_col="filterID",
            cols_to_transform=["mjd", "aperMag1"],
            order_by="mjd",
        )
