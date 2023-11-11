from pyspark.sql import SparkSession


def initialize_spark():
    """
    Initialize a SparkSession and return it.
    """
    spark = SparkSession.builder.appName("My first session").getOrCreate()
    return spark


def read_csv(spark, file_path):
    """
    Loads a large dataset and returns it.
    Returns:
        DataFrame: Loaded dataset as a DataFrame.
    """
    df = spark.read.option("header", "true").csv(file_path)
    return df


def sql_query(df, sql_query):
    """
    Performs sql queries on loaded large dataset.
    Returns:
        DataFrame: Result of query.
    """
    result_df = df.filter(sql_query)
    return result_df


def perform_data_transformation(df, column, target):
    """
    Perform a data transformation on the input DataFrame.
    Returns:
        DataFrame: Transformed DataFrame.
    """
    if (
        column not in df.columns
        or target not in df.select(column).distinct().rdd.flatMap(lambda x: x).collect()
    ):
        raise ValueError("Please enter a valid column and target value")
    transformed_df = df.filter(f"{column} = '{target}'")
    return transformed_df


