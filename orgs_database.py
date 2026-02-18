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
# Read 'govuk_orgs' table and write to DataFrame, unless it doesn't exist, in which case write it to DB from JSON data
# NOTE: Now that the columns have been edited, the SQL DB will need updating - df_edited and
# df_sql currently have different columns and so comparison between them is not possible

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
# Vertically join SQL and JSON dataframes to allow comparison

df_joint = pd.concat([df_edited, df_sql], ignore_index=True)

# %%
# Drop duplicates from df_joint, i.e. drop rows which are identical in df_sql and df_edited
# (not inc. UUID which is different for both by default)
# NOTE: This won't work with these columns.
# NOTE 2: Maybe this code is unnecessary now

columns = ['id', 'title', 'format', 'web_url', 'analytics_identifier', 'closed_at',
           'govuk_status', 'govuk_closed_status', 'start_date', 'end_date']

df_changes = df_joint.drop_duplicates(subset=columns, keep=False)

# %%
# Find new and removed organisations by title

df_new = df_edited[
    ~df_edited['title'].isin(df_sql['title'])
]

df_removed = df_sql[
    ~df_sql['title'].isin(df_edited['title'])
]

df_removed

# %%
# Track what's changed from df_sql to df_edited

df_merged = df_sql.merge(
    df_edited,
    on="analytics_identifier",
    how="inner",
    suffixes=("_old", "_new")
)

change_columns = ['id', 'title', 'format', 'web_url', 'closed_at', 'govuk_status', 'govuk_closed_status']

mask = False
for col in change_columns:
    mask |= df_merged[f"{col}_old"].fillna("NA") != df_merged[f"{col}_new"].fillna("NA")

df_changed = df_merged[mask]  # This is similar to df_changes above but has info on old and new - joined horizontally not vertically

log = []
for col in change_columns:
    differences = df_changed[
        df_changed[f"{col}_old"] != df_changed[f"{col}_new"]
    ]

    for _, row in differences.iterrows():
        log.append({
            "analytics_identifier": row["analytics_identifier"],
            "field": col,
            "old_value": row[f"{col}_old"],
            "new_value": row[f"{col}_new"]
        })


df_record_changes = pd.DataFrame(log)

df_record_changes
