# %%
import pandas as pd
import os
import ds_utils.database_operations as dbo

# %%
# Read orgs file, drop cols and filter out childless/parentless orgs

df = pd.read_json('organisations.json')

df = pd.concat(
    [df.drop(columns=["details"]), df["details"].apply(pd.Series)], axis=1
    )

dropcols = ['id', 'title', 'format', 'updated_at', 'web_url',
            'superseded_organisations', 'superseding_organisations', 'slug',
            'abbreviation', 'logo_formatted_name', 'organisation_brand_colour_class_name',
            'organisation_logo_type_class_name', 'closed_at',
            'govuk_status', 'govuk_closed_status', 'content_id']

df = df.drop(columns=dropcols).rename(columns={'analytics_identifier': 'govuk_identifier'})

# %%
# Read in SQL data

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

df_sql = pd.read_sql_table(
    table_name='govuk_orgs',
    con=engine,
    schema='testing',
    columns=['id', 'govuk_identifier']
    )


# %%
# Merge on govuk_identifier

df_merged = df.merge(
    df_sql,
    on='govuk_identifier',
    how='inner'
)

order = ['id', 'govuk_identifier', 'parent_organisations', 'child_organisations']
df_merged = df_merged.reindex(columns=order)

# %%
# Filter for orgs with non-empty parent/child org columns

df_hasparents = df_merged[
    df['parent_organisations'].apply(lambda x: isinstance(x, list) and len(x) > 0)
    ]

df_haschildren = df_merged[
    df['child_organisations'].apply(lambda x: isinstance(x, list) and len(x) > 0)
]

# %%
"""
Next steps: how to flatten parent/child_organisations columns
Extract UUIDs of parent/child_orgs from df_sql
Add to a new df with columns = [id, parent_id, child_id]
Query: one row per org, or same org in multiple rows (e.g. more than one parent/child)?
"""
