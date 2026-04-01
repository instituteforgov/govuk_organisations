# %%
import pandas as pd
import os
import ds_utils.database_operations as dbo
import utils
import uuid
from sqlalchemy import Uuid

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
# Filter for orgs with non-empty child org columns and edit

df_sponsor = df_merged[
    df['child_organisations'].apply(lambda x: isinstance(x, list) and len(x) > 0)
].drop(columns=['parent_organisations'])

# Edit columns
utils.flatten_list_of_dicts(
    df_sponsor,
    'child_organisations',
    'id'
    )

utils.remove_prefixes(
    df=df_sponsor,
    col='child_organisations',
    prefix='https://www.gov.uk/api/organisations/'
)

utils.match_and_replace(
    df1=df_sql,
    df2=df_sponsor,
    edit_col='child_organisations',
    key_col='url_name',
    val_col='id'
)

df_sponsor = df_sponsor.rename(
    columns={'id': 'parent_org_id',
             'child_organisations': 'child_org_id'})

df_sponsor = df_sponsor.explode('child_org_id').reset_index(drop=True).drop(columns=['govuk_identifier'])

df_sponsor.insert(0, 'id', [uuid.uuid4() for x in range(len(df_sponsor))])

# %%
# Push to database

df_sponsor.to_sql(
    name='orgs_sponsorship',
    con=engine,
    schema='testing',
    if_exists='replace',
    index=False,
    dtype={
        'id': Uuid,
        'parent_org_id': Uuid,
        'child_or_id': Uuid
    }
)
