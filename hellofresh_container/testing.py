from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, IntegerType, StringType, FloatType, BooleanType, StructField, StructType, Row

from images.hellofresh_container.parameter_file import test_recipes_raw_data, template_path, recipe_temp_file_name, \
    test_ingest_read_path, recipe_schema, tablename
from images.hellofresh_container.utility import spark_utility, common_utility
import findspark

findspark.init()


def parsing():
    spark = SparkSession.builder \
        .appName("HelloFreshSparkTest") \
        .getOrCreate()
    # reading ingested data in the parquet format
    ingested_data = spark_utility.reading_parquet_file(spark, test_ingest_read_path)
    # ingested_data.printSchema()

    # reading data from csv template file
    reading_template = spark_utility.reading_csv_file(spark, template_path + recipe_schema,
                                                      header=True)

    # parsing data according to the defined fields
    parse_df = common_utility.cast_columns(ingested_data, reading_template, tablename)
    # parse_df.printSchema()

    reading_template = reading_template.select(reading_template.fields, reading_template.dtype)
    pandas_df = reading_template.toPandas()
    df_dict = pandas_df.to_dict(orient='records')

    # Example DataFrame with different data types
    data = [
        (1, 'a', 1.1, True),
        (2, 'b', 2.2, False),
        (3, 'c', 3.3, True),
        (4, 'd', 4.4, True)
    ]

    # Define the schema for the DataFrame
    schema = StructType([
        StructField("col1", IntegerType(), True),
        StructField("col2", StringType(), True),
        StructField("col3", FloatType(), True),
        StructField("col4", BooleanType(), True)
    ])

    # Convert the data dictionary to a DataFrame
    df = spark.createDataFrame(list(zip(*data.values())), schema=schema)

    # Perform assertions on the column data types
    merged_dict = {
        'col1': int,
        'col2': str,
        'col3': float,
        'col4': bool
    }

    for col_name, expected_data_type in merged_dict.items():
        actual_data_type = df.schema[col_name].dataType
        print(actual_data_type)
        if isinstance(actual_data_type, expected_data_type):
            print("hii")


print(parsing())
