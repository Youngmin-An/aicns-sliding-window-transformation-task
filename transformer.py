"""
Transformation data into sliding window task
It yield sliding window data with X(size: time step) and corresponding Y(size: n out).

"""

from func import *
from pyspark.sql import SparkSession


if __name__ == "__main__":
    # Initialize app
    app_conf = get_conf_from_evn()

    SparkSession.builder.config(
        "spark.hadoop.hive.exec.dynamic.partition", "true"
    ).config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")

    # [AICNS-61]
    if app_conf["SPARK_EXTRA_CONF_PATH"] != "":
        config_dict = parse_spark_extra_conf(app_conf)
        for conf in config_dict.items():
            SparkSession.builder.config(conf[0], conf[1])

    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    app_conf["sql_context"] = spark

    # Get feature metadata
    data_col_name = (
        "input_data"  # todo: metadata concern or strict validation column names
    )
    time_col_name = "event_time"

    # Load data
    ts = load_data(app_conf, time_col_name, data_col_name + "_scaled", "scaled_minmax_2")

    # windowing and filtering null
    window_df = transform_data_into_sliding_window(ts, time_col_name, data_col_name + "_scaled", app_conf["APP_TIME_STEP"], app_conf["APP_N_OUT"])
    transformed_df = filter_window_containing_null_out(window_df)

    # Save data
    save_sliding_window_to_dwh(transformed_df, time_col_name, app_conf)

    spark.stop()
