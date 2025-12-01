# %%
import pandas as pd
import os
from sqlalchemy import NVARCHAR
from sqlalchemy.dialects.mssql import BIT, DATE
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
df_edited[['start_date', 'end_date']] = [None, None]

# %%
# Connect to database
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