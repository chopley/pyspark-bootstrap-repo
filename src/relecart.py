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


class relecart():
    def __init__(self,sc):
        self.sc = sc
        
    def get_table_from_db(table_name, db_val, user, password):
        df = spark.read.format("jdbc").\
        option("url", db_val).\
        option("dbtable", table_name).\
        option("user", user).\
        option("password", password).\
        load()
        return(df)

    def get_table_from_parquet(self,parquet_file):
        return(self.sc.read.parquet(parquet_file))

    def write_parquet(self,dataframe_name,parquet_file):
        dataframe_name.write.mode("overwrite").parquet(parquet_file)

    def get_data_from_parquets(self):
        self.node = self.get_table_from_parquet("data/node.parquet")
        self.field_data_field_test_day_click = self.get_table_from_parquet("data/field_data_field_test_day_click.parquet")
        self.field_data_field_new_clicks = self.get_table_from_parquet("data/field_data_field_new_clicks.parquet")
        self.field_data_field_state = self.get_table_from_parquet("data/field_data_field_state.parquet")
        self.field_data_field_campaign = self.get_table_from_parquet("data/field_data_field_campaign.parquet")
        self.field_data_field_product_group = self.get_table_from_parquet("data/field_data_field_product_group.parquet")
        self.field_data_field_product_clicked = self.get_table_from_parquet("data/field_data_field_product_clicked.parquet")
        self.field_data_field_campaign_logo = self.get_table_from_parquet("data/field_data_field_campaign_logo.parquet")
        self.taxonomy_term_data  = self.get_table_from_parquet("data/taxonomy_term_data.parquet")
        self.field_data_field_keywords  = self.get_table_from_parquet("data/field_data_field_keywords.parquet")
        self.field_data_field_product_categories  = self.get_table_from_parquet("data/field_data_field_product_categories.parquet")
        self.field_data_field_max_cpc_new   = self.get_table_from_parquet("data/field_data_field_max_cpc_new.parquet")
        self.field_data_field_product_price_true   = self.get_table_from_parquet("data/field_data_field_product_price_true.parquet")
        self.field_data_field_product_url_true  = self.get_table_from_parquet("data/field_data_field_product_url_true.parquet")
        self.field_data_field_product_image_url_true   = self.get_table_from_parquet("data/field_data_field_product_image_url_true.parquet")
        return(self.node.count())


    def register_tables(self):
        self.node.registerTempTable("node")
        self.field_data_field_test_day_click.registerTempTable("field_data_field_test_day_click")
        self.field_data_field_new_clicks.registerTempTable("field_data_field_new_clicks")
        self.field_data_field_state.registerTempTable("field_data_field_state")
        self.field_data_field_campaign.registerTempTable("field_data_field_campaign")
        self.field_data_field_product_group.registerTempTable("field_data_field_product_group")
        self.field_data_field_product_clicked.registerTempTable("field_data_field_product_clicked")
        self.field_data_field_max_cpc_new.registerTempTable("field_data_field_max_cpc_new")
        self.field_data_field_product_price_true.registerTempTable("field_data_field_product_price_true")
        self.field_data_field_product_url_true.registerTempTable("field_data_field_product_url_true")
        self.field_data_field_product_image_url_true.registerTempTable("field_data_field_product_image_url_true")
        self.campaign = self.node.select("*").where("type='campaign'")
        self.product_group = self.node.select("*").where("type='product_group'")
        self.product = self.node.select("*").where("type='product'")
        self.impression = self.sc.sql("SELECT * FROM node WHERE type = 'impression' AND nid IN (SELECT entity_id FROM field_data_field_test_day_click)")
        self.clicks = self.sc.sql("SELECT * FROM node WHERE node.type = 'impression' AND nid IN (SELECT entity_id FROM field_data_field_new_clicks)")
        self.product_state = self.sc.sql("SELECT * FROM field_data_field_state WHERE bundle = 'product'")
        self.impression.registerTempTable("impression")
        self.product_group.registerTempTable("product_group")
        self.product.registerTempTable("product")
        self.field_data_field_state.registerTempTable("field_data_field_state")
        self.clicks.registerTempTable("clicks")
        self.campaign.registerTempTable("campaign")

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

    def create_product_group(self):
        self.campaign_product_group = self.sc.sql("""
                                            SELECT campaign.nid AS campaign_nid,
                                              campaign.title AS campaign_title,
                                              field_data_field_campaign.entity_id AS product_group_nid,
                                              product_group.title AS product_group_title,
                                              field_data_field_product_group.entity_id AS product_nid,
                                              product.title AS product_title,
                                              product.status AS product_status,
                                              impression.nid AS impression_nid,
                                              impression.title AS impression_title,
                                              clicks.title AS click_title,
                                              clicks.nid AS click_nid,
                                              product_state.field_state_value
                                              FROM campaign
                                              LEFT JOIN
                                              field_data_field_campaign
                                              ON campaign.nid = field_data_field_campaign.field_campaign_nid
                                              LEFT JOIN
                                              field_data_field_product_group 
                                              ON 
                                              field_data_field_campaign.entity_id = field_data_field_product_group.field_product_group_nid
                                              LEFT JOIN
                                              field_data_field_product_clicked
                                              ON 
                                              field_data_field_product_group.entity_id = field_data_field_product_clicked.field_product_clicked_nid
                                              LEFT JOIN impression ON
                                              impression.nid = field_data_field_product_clicked.entity_id
                                              LEFT JOIN product_group ON
                                              product_group.nid = field_data_field_campaign.entity_id
                                              LEFT JOIN product ON
                                              product.nid = field_data_field_product_group.entity_id
                                              LEFT JOIN field_data_field_state AS product_state 
                                              ON
                                              product_state.entity_id = product.nid
                                              LEFT JOIN clicks ON
                                              clicks.nid = field_data_field_product_clicked.entity_id
                                            """
                                            )
        return(self.campaign_product_group.count())
    
def main(conf: ConfigParser, spark: SparkSession) -> None:
    #get_data_from_database()
    rlc = relecart(spark)
    rlc.get_data_from_parquets()
    rlc.register_tables()
    a = rlc.create_product_group()
    print(a)

    
    
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
