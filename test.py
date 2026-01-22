import importlib

import dlt


def get_customers():
    """Fake customers data."""
    yield {"id": 1, "name": "Acme Buyer"}
    yield {"id": 2, "name": "Globex Shopper"}


def get_companies():
    """Fake companies data."""
    yield {"id": 101, "name": "Acme Corp"}
    yield {"id": 102, "name": "Globex Corp"}


@dlt.resource
def customers():
    """Iterate over customers in CRM"""
    yield from get_customers()


@dlt.resource
def companies():
    """Iterate over companies in CRM"""
    yield from get_companies()


@dlt.source
def crm():
    """Entities registered in CRM"""
    return [customers(), companies()]


p = dlt.pipeline(
    pipeline_name="9zero_demo",
    destination="snowflake",
    dataset_name="dataset",
)


from dlt.sources.sql_database import sql_database                                                       # noqa: E402

sql_data_source = sql_database()
p.run(
    sql_data_source,  #  connection info defined in config + secrets.toml
    write_disposition="replace",
)
with p.sql_client() as client:
    # Creating a simple table joining companies and their customers.
    client.execute_sql(
        """
    CREATE TABLE company_customers AS
    SELECT
        c.company_name,
        c.industry,
        cu.customer_id,
        cu.lifetime_value
    FROM
        dwh.companies c
    LEFT JOIN
        dwh.customers cu
    ON
        c.company_id = cu.company_id;
    """
    )

import dlt                                                                                            # noqa: E402
from dlt.sources.filesystem import filesystem, read_parquet                                             # noqa: E402

filesystem_resource = filesystem(
  bucket_url="s3://my-bucket/files",
  file_glob="**/*.parquet",
  incremental=dlt.sources.incremental("modification_date")
)
filesystem_pipe = filesystem_resource | read_parquet()

# We load the data into the table_name table
p = dlt.pipeline(pipeline_name="my_pipeline", destination="duckdb")
p.run(filesystem_pipe.with_name("table_name"))

import pandas as pd                                                                                                                            # noqa: E402

df = pd.DataFrame({
    "order_id": [1, 2, 3],
    "customer_id": [1, 2, 3],
    "order_amount": [100.0, 200.0, 300.0],
})
p = dlt.pipeline("orders_pipeline", destination="snowflake")
p.run(df, table_name="orders")

