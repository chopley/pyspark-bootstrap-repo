"""
[scope]
    ETL Spark Job

"""
import sys
from argparse import ArgumentParser
from configparser import ConfigParser
from typing import Dict, Any
from datetime import datetime as dt

from pyspark.sql import DataFrame, Row, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType
from src.utils.spark_utils import get_spark_context, read_config_file, add_local_s3_conf
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

APP_NAME: str = 'etl_spark_job'


# create the general function
def _amount_spent(quantity: int, price: float) -> float:
    """
    Calculates the product between two variables
    :param quantity: (float/int)
    :param price: (float/int)
    :return:
            (float/int)
    """
    return quantity * price


def amount_spent_udf(data: DataFrame) -> DataFrame:
    # create the general UDF
    amount_spent_udf = F.udf(_amount_spent, DoubleType())
    # Note: DoubleType in Java/Scala is equal to Python float; thus you can alternatively specify FloatType()

    # Apply our UDF to the dataframe
    return data.withColumn('amount_spent', amount_spent_udf(F.col('quantity'), F.col('price')))

def get_table_from_db(table_name, db_val, user, password):
    df = spark.read.format("jdbc").\
    option("url", db_val).\
    option("dbtable", table_name).\
    option("user", user).\
    option("password", password).\
    load()
    return(df)

def get_table_from_parquet(sc,parquet_file):
    return(sc.read.parquet(parquet_file))

def write_parquet(dataframe_name,parquet_file):
    dataframe_name.write.mode("overwrite").parquet(parquet_file)

def get_data_from_parquets(sc):
    node = get_table_from_parquet(sc,"data/node.parquet")
    field_data_field_test_day_click = get_table_from_parquet(sc,"data/field_data_field_test_day_click.parquet")
    field_data_field_new_clicks = get_table_from_parquet(sc,"data/field_data_field_new_clicks.parquet")
    field_data_field_state = get_table_from_parquet(sc,"data/field_data_field_state.parquet")
    field_data_field_campaign = get_table_from_parquet(sc,"data/field_data_field_campaign.parquet")
    field_data_field_product_group = get_table_from_parquet(sc,"data/field_data_field_product_group.parquet")
    field_data_field_product_clicked = get_table_from_parquet(sc,"data/field_data_field_product_clicked.parquet")
    field_data_field_campaign_logo = get_table_from_parquet(sc,"data/field_data_field_campaign_logo.parquet")
    taxonomy_term_data  = get_table_from_parquet(sc,"data/taxonomy_term_data.parquet")
    field_data_field_keywords  = get_table_from_parquet(sc,"data/field_data_field_keywords.parquet")
    field_data_field_product_categories  = get_table_from_parquet(sc,"data/field_data_field_product_categories.parquet")
    field_data_field_max_cpc_new   = get_table_from_parquet(sc,"data/field_data_field_max_cpc_new.parquet")
    field_data_field_product_price_true   = get_table_from_parquet(sc,"data/field_data_field_product_price_true.parquet")
    field_data_field_product_url_true  = get_table_from_parquet(sc,"data/field_data_field_product_url_true.parquet")
    field_data_field_product_image_url_true   = get_table_from_parquet(sc,"data/field_data_field_product_image_url_true.parquet")
    print(node.count())
    return(node.count())


    

def get_data_from_database():
    db_val = "jdbc:mysql://dedi620.flk1.host-h.net:3306/relecpaxes_reldash?useLegacyDatetimeCode=false&serverTimezone=UTC"
    user = "relecpaxes_5"
    password = "08r11Y5OPm1G55oGA3mL"
    
    node = get_table_from_db("node", db_val, user, password)
    field_data_field_test_day_click = get_table_from_db("field_data_field_test_day_click", db_val, user, password)
    field_data_field_new_clicks = get_table_from_db("field_data_field_new_clicks", db_val, user, password)
    field_data_field_state = get_table_from_db("field_data_field_state", db_val, user, password)
    field_data_field_campaign = get_table_from_db("field_data_field_campaign", db_val, user, password)
    field_data_field_product_group = get_table_from_db("field_data_field_product_group", db_val, user, password)
    field_data_field_product_clicked = get_table_from_db("field_data_field_product_clicked", db_val, user, password)
    field_data_field_campaign_logo = get_table_from_db("field_data_field_campaign_logo", db_val, user, password)
    taxonomy_term_data  = get_table_from_db("taxonomy_term_data", db_val, user, password)
    field_data_field_keywords  = get_table_from_db("field_data_field_keywords", db_val, user, password)
    field_data_field_product_categories  = get_table_from_db("field_data_field_product_categories", db_val, user, password)
    field_data_field_max_cpc_new   = get_table_from_db("field_data_field_max_cpc_new", db_val, user, password)
    field_data_field_product_price_true   = get_table_from_db("field_data_field_product_price_true", db_val, user, password)
    field_data_field_product_url_true  = get_table_from_db("field_data_field_product_url_true", db_val, user, password)
    field_data_field_product_image_url_true   = get_table_from_db("field_data_field_product_image_url_true ", db_val, user, password)

    write_parquet(node,"data/node.parquet")
    write_parquet(field_data_field_test_day_click,"data/field_data_field_test_day_click.parquet")
    write_parquet(field_data_field_new_clicks,"data/field_data_field_new_clicks.parquet")
    write_parquet(field_data_field_state,"data/field_data_field_state.parquet")
    write_parquet(field_data_field_campaign,"data/field_data_field_campaign.parquet")
    write_parquet(field_data_field_product_group,"data/field_data_field_product_group.parquet")
    write_parquet(field_data_field_product_clicked,"data/field_data_field_product_clicked.parquet")
    write_parquet(field_data_field_campaign_logo,"data/field_data_field_campaign_logo.parquet")
    write_parquet(taxonomy_term_data,"data/taxonomy_term_data.parquet")
    write_parquet(field_data_field_keywords,"data/field_data_field_keywords.parquet")
    write_parquet(field_data_field_product_categories,"data/field_data_field_product_categories.parquet")
    write_parquet(field_data_field_max_cpc_new,"data/field_data_field_max_cpc_new.parquet")
    write_parquet(field_data_field_product_price_true,"data/field_data_field_product_price_true.parquet")
    write_parquet(field_data_field_product_url_true,"data/field_data_field_product_url_true.parquet")
    write_parquet(field_data_field_product_image_url_true,"data/field_data_field_product_image_url_true.parquet")

    print(field_data_field_max_cpc_new.count())
    field_data_field_max_cpc_new.show()
    
def main(conf: ConfigParser, spark: SparkSession) -> None:
    #get_data_from_database()
    get_data_from_parquets(spark)
    
if __name__ == "__main__":
    parser = ArgumentParser(prog=APP_NAME,
                            usage=APP_NAME,
                            description='Reads masterdata dump, dumps key tables to S3 '
                                        '& optionally generates synthetic data',
                            epilog='',
                            add_help=True)

    parser.add_argument('-c', '--config',
                        metavar='filepath',
                        help='filepath of config-file')

    argv = sys.argv[1:]
    options, args = parser.parse_known_args(argv)

    # read config-file
    config_filepath: str = options.config
    config: ConfigParser = read_config_file(config_filepath)

    job_start: dt = dt.now()
    print("Starting Spark job {} at {}".format(APP_NAME, job_start))
    print(config)
    spark_app_name: str = "{}_{}".format(APP_NAME, job_start)
    spark: SparkSession = get_spark_context(config=config, app_name=spark_app_name)
    # Optionally add S3 Path Spark
    spark: SparkSession = add_local_s3_conf(spark=spark, config=config)

    main(conf=config, spark=spark)

    job_finish: dt = dt.now()
    print("Finished Spark job {} at {}".format(APP_NAME, job_finish))
