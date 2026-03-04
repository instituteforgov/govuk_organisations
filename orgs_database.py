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

df = pd.read_json('organisations.json')

# %%
# Flatten "details" column
df_edited = pd.concat(
    [df.drop(columns=["details"]), df["details"].apply(pd.Series)], axis=1
    )

# %%
# Remove redundant columns
dropped_cols = [
    "id", "updated_at", "parent_organisations", "child_organisations", "superseded_organisations", "superseding_organisations",
    "slug", "abbreviation", "logo_formatted_name", "organisation_brand_colour_class_name",
    "organisation_logo_type_class_name", "closed_at", "govuk_closed_status", "content_id"
]

df_edited = df_edited.drop(columns=dropped_cols)

# %%
# Remove any duplicate rows

df_edited = df_edited.drop_duplicates(ignore_index=True)

# %%
# Add a UUID column
df_edited.insert(0, 'uuid', [uuid.uuid4() for _ in range(len(df_edited))])

# %%
# Create start and end date columns
df_edited[['start_date', 'end_date']] = [None, None]

# %%
# Edit URL column, rename and reorder columns

df_edited['web_url'] = df_edited['web_url'].str.replace(
    'https://www.gov.uk/government/organisations/', '', regex=False
    )

new_names = {
    'uuid': 'id',
    'analytics_identifier': 'govuk_identifier',
    'title': 'name',
    'web_url': 'url_name',
    'format': 'type'
}

column_order = [
    'id', 'govuk_identifier', 'name', 'url_name', 'type', 'govuk_status', 'start_date', 'end_date'
    ]

df_edited = df_edited.rename(columns=new_names).reindex(columns=column_order)

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
# Overwrite DB with more recent data

"""
df_edited.to_sql(
    name='govuk_orgs',
    con=engine,
    schema='testing',
    if_exists='replace',
    index=False,
    dtype={
        'id': Uuid,
        'govuk_identifier': NVARCHAR(20),
        'name': NVARCHAR(100),
        'url_name': NVARCHAR(200),
        'type': NVARCHAR(50),
        'govuk_status': NVARCHAR(100),
        'start_date': DATE,
        'end_date': DATE
    }
)
"""
# %%
# Read 'govuk_orgs' table and write to DataFrame, unless it doesn't exist, in which case write it to DB from JSON data
# NOTE: All code below refers to old column names so won't run - fix later when next steps decided

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
# Merge to find matches and replace new UUID with old where match exists

df_merged = df_edited.merge(
    df_sql[['govuk_identifier', 'id']],
    on='govuk_identifier',
    how='left',
    suffixes=('_new', '_old')
)

df_edited['id'] = df_merged['id_old'].fillna(df_merged['id_new'])
