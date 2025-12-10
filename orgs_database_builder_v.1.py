# %%
import pandas as pd
import os
import uuid
from sqlalchemy import NVARCHAR, Uuid
from sqlalchemy.dialects.mssql import DATE
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
# Create table in DB if it doesn't exist, otherwise skip
try:
    df_edited.to_sql(
        name='govuk_orgs',
        schema='testing',
        con=engine,
        if_exists='fail',
        index=False,
        dtype={
            'uuid': Uuid,
            'id': NVARCHAR(200),
            'title': NVARCHAR(200),
            'format': NVARCHAR(100),
            'web_url': NVARCHAR(200),
            'analytics_identifier': NVARCHAR(100),
            'closed_at': DATE,
            'govuk_status': NVARCHAR(100),
            'govuk_closed_status': NVARCHAR(100),
            'start_date': DATE,
            'end_date': DATE
        }
    )
except ValueError:
    pass
