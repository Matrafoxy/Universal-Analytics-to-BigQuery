from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    MetricType,
    RunReportRequest,
    FilterExpression,
    Filter
)
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import os
import pandas as pd

KEY_FILE_LOCATION = '' # Path to your Google Cloud service account key file
VIEW_ID = ''  # Your Google Analytics View ID
BIGQUERY_PROJECT = ''  # Your Google Cloud Project ID
BIGQUERY_DATASET = ''  # BigQuery Dataset name where the data will be stored
BIGQUERY_TABLE = ''  # BigQuery Table name where the data will be stored

# Setting up the environment variable for Google Application Credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = KEY_FILE_LOCATION

ua_type_to_conversion = {
    'TYPE_INTEGER': int,
    'TYPE_CURRENCY': float,
    'TIME': float,
    'PERCENT': float,
    'TYPE_SECONDS': float,
    'TYPE_MILLISECONDS': float,
    'TYPE_FLOAT': float,
}

dim_value_map = {
    'dearRebelDiscount_14': 'siteWidePopup_14',
    '(not set)': 'dec23Giveaway_18'
}

def initialize_analyticsreporting():
    """Initializes the Google Analytics Reporting API client."""
    return BetaAnalyticsDataClient()

def get_report(analytics, limit=20000, offset=0):
    """Fetches the report data from Google Analytics."""
    # Here, specify the analytics report request details
    request = RunReportRequest(
        property=f"properties/{VIEW_ID}",
       	dimensions=[
            Dimension(name="date"),
            #Dimension(name="isoYearIsoWeek"),
            #Dimension(name="yearMonth"),
            #Dimension(name="year"),
            Dimension(name="sessionCampaignName"),
            Dimension(name="sessionSource"),
            Dimension(name="sessionMedium"),
            Dimension(name="customEvent:form_id"),
		],
		metrics=[
          Metric(name="eventCount"),
		 
		],
        dimension_filter=FilterExpression(
            filter=Filter(
                field_name="eventName",
                in_list_filter=Filter.InListFilter(
                    values=[
                        "generate_lead",
                        "form_submission",
                        #"form_submit",
                        "gift_finder_submit",
                        #"form_start",
                    ]
                ),
            )
        ),
        date_ranges=[DateRange(start_date="2023-05-01", end_date="2024-04-30")],
        #date_ranges=[DateRange(start_date="2024-05-01", end_date="2024-05-31")],
        #date_ranges=[DateRange(start_date="2024-01-01", end_date="2024-05-31")],
        limit=limit,
        offset=offset,

    ) 
    return analytics.run_report(request)

def response_to_dataframe(response):
    """Prints results of a runReport call."""
    list_rows = []
    for j, row in enumerate(response.rows):
        row_data = {}
        for i, dimension_value in enumerate(row.dimension_values):
            dimension_name = response.dimension_headers[i].name
            row_data[dimension_name] = dim_value_map.get(dimension_value.value, dimension_value.value)

        for i, metric_value in enumerate(row.metric_values):
            metric_name = response.metric_headers[i].name
            metric_type_name = MetricType(response.metric_headers[i].type_).name
            conversion_f = ua_type_to_conversion.get(metric_type_name, str)
            row_data[metric_name] = conversion_f(metric_value.value)
        list_rows.append(row_data)
    return pd.DataFrame(list_rows)

def upload_to_bigquery(df, project_id, dataset_id, table_id):
    """Uploads the DataFrame to Google BigQuery."""
    # The DataFrame's column names are formatted for BigQuery compatibility
    df.columns = [col.replace(':', '_') for col in df.columns]

    bigquery_client = bigquery.Client(project=project_id)
    dataset_ref = bigquery_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    schema = []

    # Generating schema based on DataFrame columns
    for col in df.columns:
        dtype = df[col].dtype
        if pd.api.types.is_integer_dtype(dtype):
            bq_type = 'INTEGER'
        elif pd.api.types.is_float_dtype(dtype):
            bq_type = 'FLOAT'
        elif pd.api.types.is_bool_dtype(dtype):
            bq_type = 'BOOLEAN'
        else:
            bq_type = 'STRING'
        schema.append(bigquery.SchemaField(col, bq_type))

    try:
        bigquery_client.get_table(table_ref)
    except NotFound:
        # Creating a new table if it does not exist
        table = bigquery.Table(table_ref, schema=schema)
        bigquery_client.create_table(table)
        print(f"Created table {table_id}")

    # Loading data into BigQuery and confirming completion
    full_table_id = f"{project_id}.{dataset_id}.{table_id}"
    load_job = bigquery_client.load_table_from_dataframe(df, table_ref)
    load_job.result()
    print(f"Data uploaded to {full_table_id}")

def main():
    """Main function to execute the script."""
    try:
        analytics = initialize_analyticsreporting()
        limit = 20000
        offset = 0
        while 1 == 1:
            response = get_report(analytics, limit, offset)
            df = response_to_dataframe(response)
            upload_to_bigquery(df, BIGQUERY_PROJECT, BIGQUERY_DATASET, BIGQUERY_TABLE)
            total = response.row_count
            if limit + offset > total:
                break
            offset += limit
    except Exception as e:
        # Handling exceptions and printing error messages
        print(f"Error occurred: {e}")

if __name__ == '__main__':
    main()  # Entry point of the script

