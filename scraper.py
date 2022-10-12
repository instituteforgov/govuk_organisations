# %%
# #!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
    Purpose
        Explore gov.uk organisations API
    Inputs
        - Web: gov.uk organisations API
    Outputs
        - None
    Parameters
        None
    Notes
        None
'''

import os
import requests

import pandas as pd
import urllib

# %%
# Prepare to call API
url_stub = 'https://www.gov.uk/api/organisations?page='
headers = {'accept': 'application/json'}


def call_api(url_stub, page_number, headers):
    url = url_stub + str(page_number)
    r = requests.get(url, headers=headers)

    if r.ok:
        record_list.extend(r.json()['results'])

    try:
        if r.json()['next_page_url']:
            page_number += 1
            call_api(url_stub, page_number, headers)
    except KeyError:
        pass

    return


# %%
# Call API
# This saves rows to a list of dictionaries then saves this list of dictionaries to a dataframe     # noqa: E501
# This has performance benefits over adding individual rows to a dataframe (ref: https://stackoverflow.com/questions/10715965/create-a-pandas-dataframe-by-appending-one-row-at-a-time)     # noqa: E501
page_number = 1
record_list = []

call_api(url_stub, page_number, headers)

df_govkorganisations = pd.DataFrame(record_list, )

# %%
# Preview data
df_govkorganisations

# %%
# SAVE SCRAPED DATA TO PICKLE
df_govkorganisations.to_pickle('data/govukorganisations_20220227.pkl')

# %%
# Remove trailing whitespace from organisation names
df_govkorganisations['title'] = df_govkorganisations['title'].str.strip()

# %%
# Reshape data
# This is necessary because the API returns data that is nested. Specifically:     # noqa: E501
# 1. Various things are extracted from the nested 'details' element and saved alongside other elements that aren't nested      # noqa: E501
# 2. The 'parent_organisations', 'child_organisations', 'superseded_organisations' and 'superseding_organisations' elements, which are lists, are saved to separate dataframes       # noqa: E501
# NB: In the language of gov.uk, 'superseded organisations' are predecesssors, 'superseding organisations' are successors (rather than organisations this organisation was superseding)       # noqa: E501
df_govkorganisations_flat = pd.concat(
    [
        df_govkorganisations.drop([
            'id', 'web_url', 'details'
        ], axis=1),
        df_govkorganisations['details'].apply(pd.Series)[[
            'slug', 'abbreviation', 'closed_at',
            'govuk_status', 'govuk_closed_status'
        ]]
    ],
    axis=1
)

df_govkorganisations_flat.rename(
    columns={
        'title': 'name',
        'format': 'organisation_type',
        'analytics_identifier': 'govuk_id',
        'slug': 'govuk_string',
    }, inplace=True
)

df_parentorganisations = pd.concat(
    [
        df_govkorganisations_flat.explode('parent_organisations')['govuk_id'],      # Exploded twice, so we get these identifiers on the rows we need them      # noqa: E501
        df_govkorganisations_flat.explode(
            'parent_organisations'
        )['parent_organisations'].apply(pd.Series)['web_url']
    ],
    axis=1
)

df_childorganisations = pd.concat(
    [
        df_govkorganisations_flat.explode('child_organisations')['govuk_id'],       # NB: As above      # noqa: E501
        df_govkorganisations_flat.explode(
            'child_organisations'
        )['child_organisations'].apply(pd.Series)['web_url']
    ],
    axis=1
)

df_supersededorganisations = pd.concat(
    [
        df_govkorganisations_flat.explode('superseded_organisations')['govuk_id'],       # NB: As above      # noqa: E501
        df_govkorganisations.explode(
            'superseded_organisations'
        )['superseded_organisations'].apply(pd.Series)['web_url']
    ],
    axis=1
)

df_supersedingorganisations = pd.concat(
    [
        df_govkorganisations_flat.explode('superseding_organisations')['govuk_id'],       # NB: As above      # noqa: E501
        df_govkorganisations_flat.explode(
            'superseding_organisations'
        )['superseding_organisations'].apply(pd.Series)['web_url']
    ],
    axis=1
)

df_govkorganisations_flat.drop([
    'parent_organisations', 'child_organisations',
    'superseded_organisations', 'superseding_organisations'
], axis=1, inplace=True)

# %%
# Drop rows without any substantive information
df_parentorganisations.drop(
    df_parentorganisations.index[
        df_parentorganisations['web_url'].isna()
    ],
    inplace=True
)
df_childorganisations.drop(
    df_childorganisations.index[
        df_childorganisations['web_url'].isna()
    ],
    inplace=True
)
df_supersededorganisations.drop(
    df_supersededorganisations.index[
        df_supersededorganisations['web_url'].isna()
    ],
    inplace=True
)
df_supersedingorganisations.drop(
    df_supersedingorganisations.index[
        df_supersedingorganisations['web_url'].isna()
    ],
    inplace=True
)

# %%
# Turn URL into slug
df_parentorganisations['govuk_string'] = df_parentorganisations[
    'web_url'
].str.replace(
    'https://www.gov.uk/government/organisations/', '', regex=False
)
df_childorganisations['govuk_string'] = df_childorganisations[
    'web_url'
].str.replace(
    'https://www.gov.uk/government/organisations/', '', regex=False
)
df_supersededorganisations['govuk_string'] = df_supersededorganisations[
    'web_url'
].str.replace(
    'https://www.gov.uk/government/organisations/', '', regex=False
)
df_supersedingorganisations['govuk_string'] = df_supersedingorganisations[
    'web_url'
].str.replace(
    'https://www.gov.uk/government/organisations/', '', regex=False
)

# %%
# Convert links dataframes into lookups of IDs
df_parentorganisationslookup = pd.merge(
    df_parentorganisations,
    df_govkorganisations_flat,
    how='inner',
    on='govuk_string',
    suffixes=('_left', '_right'),
    validate='many_to_one'
)[['govuk_id_left', 'govuk_id_right']]

df_childorganisationslookup = pd.merge(
    df_childorganisations,
    df_govkorganisations_flat,
    how='inner',
    on='govuk_string',
    suffixes=('_left', '_right'),
    validate='many_to_one'
)[['govuk_id_left', 'govuk_id_right']]

df_supersededorganisationslookup = pd.merge(
    df_supersededorganisations,
    df_govkorganisations_flat,
    how='inner',
    on='govuk_string',
    suffixes=('_left', '_right'),
    validate='many_to_one'
)[['govuk_id_left', 'govuk_id_right']]

df_supersedingorganisationslookup = pd.merge(
    df_supersedingorganisations,
    df_govkorganisations_flat,
    how='inner',
    on='govuk_string',
        suffixes=('_left', '_right'),
    validate='many_to_one'
)[['govuk_id_left', 'govuk_id_right']]

# %%
# Rename columns
df_parentorganisationslookup.rename(
    columns={
        'govuk_id_left': 'govuk_id_child',
        'govuk_id_right': 'govuk_id_parent',
    }, inplace=True
)

df_childorganisationslookup.rename(
    columns={
        'govuk_id_left': 'govuk_id_parent',     # NB: This differs from the above       # noqa: E501
        'govuk_id_right': 'govuk_id_child',
    }, inplace=True
)

df_supersededorganisationslookup.rename(
    columns={
        'govuk_id_left': 'govuk_id_successor',
        'govuk_id_right': 'govuk_id_predecessor',
    }, inplace=True
)

df_supersedingorganisationslookup.rename(
    columns={
        'govuk_id_left': 'govuk_id_predecessor',     # NB: This differs from the above       # noqa: E501
        'govuk_id_right': 'govuk_id_successor',
    }, inplace=True
)

# %%
# Concatenate data and drop duplicates
df_parentchild = pd.concat(        # This matches column names, so there's no need to reverse the order of columns first        # noqa: E501
    [df_parentorganisationslookup, df_childorganisationslookup]
)
df_parentchild.drop_duplicates(inplace=True)

df_predecessorsuccessor = pd.concat(        # This matches column names, so there's no need to reverse the order of columns first        # noqa: E501
    [df_supersedingorganisationslookup, df_supersededorganisationslookup]       # NB: Done this way round so the order of columns is govuk_id_predecessor, govuk_id_successor        # noqa: E501
)
df_predecessorsuccessor.drop_duplicates(inplace=True)

# %%
# Connect to d/b
driver = '{ODBC Driver 17 for SQL Server}'
server = os.environ['odbc_server']
database = os.environ['odbc_database']
authentication = 'ActiveDirectoryInteractive'       # Azure Active Directory - Universal with MFA support      # noqa: E501
username = os.environ['odbc_username']

conn = urllib.parse.quote_plus(
    'DRIVER=' + driver +
    ';SERVER=' + server +
    ';DATABASE=' + database +
    ';UID=' + username +
    ';AUTHENTICATION=' + authentication + ';'
)
engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(conn))

# %%
# Save results to SQL
pd.io.sql.execute(
    '''drop table if exists reference.''' +
    '''[civilservicepublicbodies.govukorganisations_20220227]''',
    con=engine
)
df_govkorganisations_flat.to_sql(
    'civilservicepublicbodies.govukorganisations_20220227',
    schema='reference',
    con=engine,
    dtype={
        'name': sqlalchemy.types.NVARCHAR(length=255),
        'organisation_type': sqlalchemy.types.NVARCHAR(length=64),
        'updated_at': sqlalchemy.dialects.mssql.DATETIMEOFFSET,
        'govuk_id': sqlalchemy.types.NVARCHAR(length=16),
        'govuk_string': sqlalchemy.types.NVARCHAR(length=255),
        'abbreviation': sqlalchemy.types.NVARCHAR(length=255),      # In some cases this is the full org name       # noqa: E501
        'closed_at': sqlalchemy.dialects.mssql.DATETIMEOFFSET,
        'govuk_status': sqlalchemy.types.NVARCHAR(length=16),
        'govuk_closed_status': sqlalchemy.types.NVARCHAR(length=16)
    },
    index=False
)

pd.io.sql.execute(
    '''drop table if exists reference.[''' +
    '''civilservicepublicbodies.''' +
    '''govukorganisations_parentchildlinks_20220227''' +
    ''']''',
    con=engine
)
df_parentchild.to_sql(
    '''civilservicepublicbodies.''' +
    '''govukorganisations_parentchildlinks_20220227''',
    schema='reference',
    con=engine,
    dtype={
        'govuk_id_child': sqlalchemy.types.NVARCHAR(length=16),
        'govuk_id_parent': sqlalchemy.types.NVARCHAR(length=16)
    },
    index=False
)

pd.io.sql.execute(
    '''drop table if exists reference.[''' +
    '''civilservicepublicbodies.''' +
    '''govukorganisations_predecessorsuccessorlinks_20220227''' +
    ''']''',
    con=engine
)
df_predecessorsuccessor.to_sql(
    '''civilservicepublicbodies.''' +
    '''govukorganisations_predecessorsuccessorlinks_20220227''',
    schema='reference',
    con=engine,
    dtype={
        'govuk_id_predecessor': sqlalchemy.types.NVARCHAR(length=16),
        'govuk_id_successor': sqlalchemy.types.NVARCHAR(length=16)
    },
    index=False
)

# %%
