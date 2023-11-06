"""
Main ci
"""

from my_lib.lib import (
    initialize_spark,
    read_csv,
    sql_query,
    perform_data_transformation,
    save_summary_report,
)


def main():
    # Initialize the spark connection
    spark = initialize_spark()

    #  Load the imecas dataset
    file_path = "https://raw.githubusercontent.com/jjsantos01/aire_cdmx/master/datos/contaminantes_2019-05-16.csv",

    imecas_df = read_csv(spark, file_path)

    # Print out the DF
    imecas_df.show()

    # Register DataFrame as a temporary SQL table/view
    imecas_df.createOrReplaceTempView("imecas")

    # Step 4: Perform a Spark SQL query
    sql_query_string = "zona = 'Sam Smith'"
    result_df = sql_query(songs_df, sql_query_string)

    # Print out the result DataFrame
    result_df.show()

    # Step 5: Perform data transformation
    column_of_interest = "artist"
    target_value = "Maroon 5"
    transformed_df = perform_data_transformation(
        songs_df, column_of_interest, target_value
    )

    # Combine the results into the report_content
    report_content = "SQL Query Result:\n"
    report_content += f"Number of rows: {result_df.count()}\n\n"
    report_content += "Transformed DataFrame:\n"
    report_content += f"Number of rows: {transformed_df.count()}\n"

    # Step 6: Save summary report
    report_file_path = "report.txt"
    save_summary_report(report_content, report_file_path)

    # Display the result DataFrame
    transformed_df.show()


if __name__ == "__main__":
    main()