"""
Function level adapters
"""
import os
import pendulum
from pyspark.ml.feature import VectorAssembler, MinMaxScalerModel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import FloatType, ArrayType
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import logging

__all__ = [
    "get_conf_from_evn",
    "parse_spark_extra_conf",
    "load_data",
    "transform_data_into_sliding_window",
    "filter_window_containing_null_out",
    "save_sliding_window_to_dwh",
]

logger = logging.getLogger()


def get_conf_from_evn():
    """
        Get conn info from env variables
    :return:
    """
    conf = dict()
    try:
        # Feature id
        conf["FEATURE_ID"] = os.getenv("FEATURE_ID")
        # Raw data period
        start_datetime = os.getenv("APP_TIME_START")  # yyyy-MM-dd'T'HH:mm:ss
        end_datetime = os.getenv("APP_TIME_END")  # yyyy-MM-dd'T'HH:mm:ss
        conf["APP_TIMEZONE"] = os.getenv("APP_TIMEZONE", default="UTC")
        conf["APP_TIME_STEP"] = int(os.getenv("APP_TIME_STEP"))
        conf["APP_N_OUT"] = int(os.getenv("APP_N_OUT"))

        conf["SPARK_EXTRA_CONF_PATH"] = os.getenv(
            "SPARK_EXTRA_CONF_PATH", default=""
        )  # [AICNS-61]
        conf["start"] = pendulum.parse(start_datetime).in_timezone(conf["APP_TIMEZONE"])
        conf["end"] = pendulum.parse(end_datetime).in_timezone(conf["APP_TIMEZONE"])

        # todo: temp patch for day resolution parsing, so later with [AICNS-59] resolution will be subdivided.
        conf["end"] = conf["end"].subtract(minutes=1)

    except Exception as e:
        print(e)
        raise e
    return conf


def parse_spark_extra_conf(app_conf):
    """
    Parse spark-default.xml style config file.
    It is for [AICNS-61] that is spark operator take only spark k/v confs issue.
    :param app_conf:
    :return: Dict (key: conf key, value: conf value)
    """
    with open(app_conf["SPARK_EXTRA_CONF_PATH"], "r") as cf:
        lines = cf.read().splitlines()
        config_dict = dict(
            list(
                filter(
                    lambda splited: len(splited) == 2,
                    (map(lambda line: line.split(), lines)),
                )
            )
        )
    return config_dict


def load_data(app_conf, time_col_name, data_col_name, source_table: str) -> DataFrame:
    """
    Load the data to be transformed.
    TIME_STEP + N_OUT - 1 additional data is loaded because not only the data of [start date, end date]
    but also the data that crosses the start date is responsible for transforming.
    :param app_conf:
    :param time_col_name:
    :param data_col_name:
    :param source_table:
    :return:
    """
    # Inconsistent cache
    # https://stackoverflow.com/questions/63731085/you-can-explicitly-invalidate-the-cache-in-spark-by-running-refresh-table-table
    SparkSession.getActiveSession().sql(f"REFRESH TABLE {source_table}")
    query_prev = f"""
        SELECT v.{time_col_name}, v.{data_col_name}  
            FROM (
                SELECT {time_col_name}, {data_col_name}, concat(concat(cast(year as string), lpad(cast(month as string), 2, '0')), lpad(cast(day as string), 2, '0')) as date 
                FROM {source_table}
                ) v 
            WHERE v.date  < {app_conf['start'].format('YYYYMMDD')}
            ORDER BY {time_col_name} DESC
            LIMIT {app_conf["APP_TIME_STEP"] + app_conf["APP_N_OUT"] - 1} 
        """
    ts_prev = SparkSession.getActiveSession().sql(query_prev).sort(time_col_name)
    query_now = f"""
    SELECT v.{time_col_name}, v.{data_col_name}  
        FROM (
            SELECT {time_col_name}, {data_col_name}, concat(concat(cast(year as string), lpad(cast(month as string), 2, '0')), lpad(cast(day as string), 2, '0')) as date 
            FROM {source_table}
            ) v 
        WHERE v.date  >= {app_conf['start'].format('YYYYMMDD')} AND v.date <= {app_conf['end'].format('YYYYMMDD')} 
    """
    ts_now = SparkSession.getActiveSession().sql(query_now)
    ts = ts_prev.unionByName(ts_now)
    logger.info("loaded data")
    logger.info(ts.show())
    return ts.sort(F.col(time_col_name).desc())


def transform_data_into_sliding_window(ts: DataFrame, time_col_name: str, data_col_name: str, time_step: int, n_out: int, x_col_name: str="X", y_col_name: str="Y") -> DataFrame:
    """
    Transform data timseries data column to additional columns sliding window X[size: time_step], Y[size: n_out].
    Column {data_col_name} will be dropped and time_step, n_out, {x_col_name}, {y_col_name} will be added.
    :param ts:
    :param time_col_name:
    :param data_col_name:
    :param time_step:
    :param n_out:
    :param x_col_name:
    :param y_col_name:
    :return: DataFrame added data_col_name_vec, removed data_col_name
    """
    sliding_window = Window.orderBy(time_col_name).rowsBetween(-(time_step + n_out - 1), Window.currentRow)  # inclusive
    transformed_df = ts.withColumn("collect", F.collect_list(data_col_name).over(sliding_window)).withColumn("index",
                                                                                                  F.monotonically_increasing_id()).drop(data_col_name)
    # cut-off data smaller than window size
    sliding_window_df = transformed_df.filter(F.col("index") >= time_step + n_out)

    # Split X, Y
    x_udf = F.udf(lambda l: l[:time_step], ArrayType(FloatType()))
    y_udf = F.udf(lambda l: l[time_step:], ArrayType(FloatType()))
    splitted_df = sliding_window_df.withColumn(x_col_name, x_udf("collect")).withColumn(y_col_name, y_udf("collect")).drop("collect")

    return splitted_df


def filter_window_containing_null_out(window_df: DataFrame, x_col_name: str = "X", y_col_name: str = "Y"):
    """
    Filter out that window containing null value both X and Y
    :param window_df:
    :param x_col_name:
    :param y_col_name:
    :return:
    """
    # null in array https://stackoverflow.com/questions/69939157/pyspark-filter-arraytype-rows-which-contain-null-value
    # nan in array https://stackoverflow.com/questions/43526225/how-to-filter-out-rows-with-nan-values-in-hive
    null_statement = f"""forall({x_col_name}, x -> x is not null) AND forall({y_col_name}, y -> y is not null) AND
                     forall({x_col_name}, x -> !isNaN(x)) AND forall({y_col_name}, y -> !isNaN(y))"""

    return window_df.filter(F.expr(null_statement))


def __append_partition_cols(ts: DataFrame, time_col_name: str):
    return (
        ts.withColumn("datetime", F.from_unixtime(F.col(time_col_name) / 1000))
        .select(
            "*",
            F.year("datetime").alias("year"),
            F.month("datetime").alias("month"),
            F.dayofmonth("datetime").alias("day"),
        )
        .sort(time_col_name)
    )


def save_sliding_window_to_dwh(window_df: DataFrame, time_col_name: str, app_conf, feature_col_name: str = "feature_id", x_col_name: str = "X", y_col_name: str = "Y") -> DataFrame:
    """

    :param window_df:
    :param time_col_name:
    :param app_conf:
    :param feature_col_name:
    :param x_col_name:
    :param y_col_name:
    :return:
    """
    # todo: transaction
    # todo: seperate table now.. because spark 'AnalysisException: DELETE is only supported with v2 tables.'

    table_name = f"transformed_sliding_window_timestep{app_conf['APP_TIME_STEP']}_out{app_conf['APP_N_OUT']}_{app_conf['FEATURE_ID']}"
    SparkSession.getActiveSession().sql(
        f"CREATE TABLE IF NOT EXISTS {table_name} ({time_col_name} BIGINT, {x_col_name} ARRAY<DOUBLE>, {y_col_name} ARRAY<DOUBLE>) PARTITIONED BY (year int, month int, day int) STORED AS PARQUET"
    )
    period = pendulum.period(app_conf["start"], app_conf["end"])

    # Create partition columns(year, month, day) from timestamp
    partition_df = __append_partition_cols(window_df, time_col_name)

    # for idempotent task, delete previous corresponding data
    for date in period.range("days"):
        SparkSession.getActiveSession().sql(
            f"ALTER TABLE {table_name} DROP IF EXISTS PARTITION(year={date.year}, month={date.month}, day={date.day})"
        )

    # Save
    partition_df.select(time_col_name, x_col_name, y_col_name, "year", "month", "day").write.format("hive").mode("append").insertInto(table_name)


