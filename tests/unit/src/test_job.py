from tests.test_utils.test_spark import spark_session
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.functions import col

from src.job import amount_spent_udf
from src.relecart import get_data_from_parquets, get_table_from_parquet


def test_amount_spent_udf(spark_session: SparkSession) -> None:
    input_df = spark_session.createDataFrame([
        Row(customer_name="Geoffrey", date="2016-04-22", category="Foo", product_name="Bar", quantity=1, price=2.00),
    ])
    result = amount_spent_udf(data=input_df)

    assert isinstance(result, DataFrame)
    assert result.count() == input_df.count()
    assert sorted(result.columns) == ['amount_spent', 'category', 'customer_name', 'date', 'price',
                                      'product_name', 'quantity']

    assert result.collect()[0].amount_spent == 2.00
    
def test_get_data_from_parquets(spark_session: SparkSession) -> None:
    result = get_data_from_parquets(spark_session)
    assert result == 6319
