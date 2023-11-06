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


def save_summary_report(report_content, file_path):
    """
    Save a summary report to a PDF or markdown file.

    Args:
        report_content (str): Content of the summary report.
        file_path (str): Path to save the report file.
    """
    with open(file_path, "w") as file:
        file.write(report_content)