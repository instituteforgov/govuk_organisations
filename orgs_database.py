# %%
import pandas as pd
import os
import uuid
from sqlalchemy import NVARCHAR, Uuid
from sqlalchemy.dialects.mssql import DATE
from sqlalchemy.exc import NoSuchTableError
import ds_utils.database_operations as dbo

# %%
# Read in data

df = pd.read_json("organisations.json")

# %%
# Flatten "details" column
df_edited = pd.concat(
    [df.drop(columns=["details"]), df["details"].apply(pd.Series)], axis=1
    )

# %%
# Remove redundant columns
dropped_cols = [
    "updated_at", "parent_organisations", "child_organisations", "superseded_organisations", "superseding_organisations",
    "slug", "abbreviation", "logo_formatted_name", "organisation_brand_colour_class_name",
    "organisation_logo_type_class_name", "content_id"
]

df_edited = df_edited.drop(columns=dropped_cols)

# %%
# Add a UUID column
df_edited.insert(0, 'uuid', [uuid.uuid4() for _ in range(len(df_edited))])

# %%
# Creat start and end date columns
df_edited[['start_date', 'end_date']] = [None, None]

# %%
# Create connection to database
engine = dbo.connect_sql_db(
    driver="pyodbc",
    driver_version=os.environ["ODBC_DRIVER"],
    dialect="mssql",
    server=os.environ["ODBC_SERVER"],
    database=os.environ["ODBC_DATABASE"],
    authentication=os.environ["ODBC_AUTHENTICATION"],
    username=os.environ["AZURE_CLIENT_ID"],
    password=os.environ["AZURE_CLIENT_SECRET"],
)

# %%
# Read 'govuk_orgs' table and write to DataFrame, unless it doesn't exist, in which case write it to DB from JSON data

try:
    df_sql = pd.read_sql_table(
        table_name='govuk_orgs',
        con=engine,
        schema='testing',
        parse_dates=[
            'start_date', 'end_date'
        ]
    )
except NoSuchTableError:
    df_edited.to_sql(
        name='govuk_orgs',
        schema='testing',
        con=engine,
        if_exists='fail',
        index=False,
        dtype={
            'uuid': Uuid,
            'id': NVARCHAR(200),
            'title': NVARCHAR(100),
            'format': NVARCHAR(50),
            'web_url': NVARCHAR(200),
            'analytics_identifier': NVARCHAR(20),
            'closed_at': NVARCHAR(100),
            'govuk_status': NVARCHAR(100),
            'govuk_closed_status': NVARCHAR(100),
            'start_date': DATE,
            'end_date': DATE
        }
    )

# %%
# Concatenate SQL and JSON Dataframes to allow comparison

df_joint = pd.concat([df_edited, df_sql])

