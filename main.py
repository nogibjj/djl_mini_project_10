"""
Main 
"""

from my_lib.lib import (
    initialize_spark,
    read_csv,
    sql_query,
    perform_data_transformation,
)


def main():
    # Initialize the spark connection
    spark = initialize_spark()

    #  Load the imecas dataset
    file_path = "contaminantes.cvs"

    imecas_df = read_csv(spark, file_path)

    # Print out the DF
    imecas_df.show()

    # Register DataFrame as a temporary SQL table/view
    imecas_df.createOrReplaceTempView("imecas")

    # Spark SQL query
    sql_query_string = "zona = 'NE'"
    result_df = sql_query(imecas_df, sql_query_string)

    # Print out the result DataFrame
    result_df.show()

    #  data transformation
    column_of_interest = "zona"
    target_value = "CE"
    transformed_df = perform_data_transformation(
        imecas_df, column_of_interest, target_value
    )
    
    # Display the result DataFrame
    transformed_df.show()


if __name__ == "__main__":
    main()
