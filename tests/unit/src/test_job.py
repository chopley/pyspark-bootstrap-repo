from tests.test_utils.test_spark import spark_session
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.functions import col

from src.job import amount_spent_udf
from src.relecart import relecart

    
def test_get_data_from_parquets(spark_session: SparkSession) -> None:
    rlc = relecart(spark_session)
    result = rlc.get_data_from_parquets()
    assert result == 6319
    
def test_create_product_group(spark_session: SparkSession) -> None:
    rlc = relecart(spark_session)
    rlc.get_data_from_parquets()
    rlc.register_tables()
    result = rlc.create_product_group()
    assert result == 7251
