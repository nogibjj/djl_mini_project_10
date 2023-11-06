"""
Main 
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

    # Build report
    report_content = "SQL Query Result (NE zone):\n"
    report_content += f"Rows: {result_df.count()}\n\n"
    report_content += "Transformed DataFrame (CE zone):\n"
    report_content += f"Rows: {transformed_df.count()}\n"

    # Step 6: Save summary report
    report_file_path = "report.txt"
    save_summary_report(report_content, report_file_path)

    # Display the result DataFrame
    transformed_df.show()


if __name__ == "__main__":
    main()