# %%
import pandas as pd
import os
import ds_utils.database_operations as dbo
import utils

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
    )

# %%
# Merge on govuk_identifier

df_merged = df.merge(
    df_sql[['id', 'govuk_identifier']],
    on='govuk_identifier',
    how='inner'
)

order = ['id', 'govuk_identifier', 'parent_organisations', 'child_organisations']
df_merged = df_merged.reindex(columns=order)

# %%
# Filter for orgs with non-empty parent/child org columns

df_hasparents = df_merged[
    df['parent_organisations'].apply(lambda x: isinstance(x, list) and len(x) > 0)
    ].drop(columns=['child_organisations'])

df_haschildren = df_merged[
    df['child_organisations'].apply(lambda x: isinstance(x, list) and len(x) > 0)
].drop(columns=['parent_organisations'])

# %%

utils.flatten_list_of_dicts(
    df_hasparents,
    'parent_organisations',
    'id'
    )

utils.flatten_list_of_dicts(
    df_haschildren,
    'child_organisations',
    'id'
    )

# %%
# Strip the url paths from parent/child_organisations to leave url_name, then map url_name to relevant UUID from df_sql

utils.remove_prefixes(
    df=df_hasparents,
    col='parent_organisations',
    prefix='https://www.gov.uk/api/organisations/'
)

utils.remove_prefixes(
    df=df_haschildren,
    col='child_organisations',
    prefix='https://www.gov.uk/api/organisations/'
)

utils.match_and_replace(
    df1=df_sql,
    df2=df_hasparents,
    edit_col='parent_organisations',
    key_col='url_name',
    val_col='id'
)

utils.match_and_replace(
    df1=df_sql,
    df2=df_haschildren,
    edit_col='child_organisations',
    key_col='url_name',
    val_col='id'
)

# %%
# Merge formatted DataFrames to create table of organisation links

df_link = df_hasparents.merge(
    right=df_haschildren,
    how='outer',
    on=['id', 'govuk_identifier']
)
