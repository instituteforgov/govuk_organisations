# %%
"""
    Purpose
        Compare GOV.UK organisations API data
    Inputs
        - json: "<various>/organisations.json"
            - Monthly extracts of organisations data from GOV.UK organisations API
    Outputs
        - None
    Parameters
        None
    Notes
        None
"""

import pandas as pd
from pandas.io.formats import excel

from ds_utils import matching_operations as mo

# %%
# SET VARIABLES
data_urls = {
    "20221001": "https://raw.githubusercontent.com/philipnye/journocoders-github-actions/6859515e67103242bf614df6a2391c682424b528/organisations.json",      # noqa: E501
    "20221101": "https://raw.githubusercontent.com/philipnye/journocoders-github-actions/185d6c3029a5500fd5fdae3c500958b83d059ea6/organisations.json",      # noqa: E501
    "20221201": "https://raw.githubusercontent.com/philipnye/journocoders-github-actions/796352d5adca5f2ebced7cd3839744c61bf1d424/organisations.json",      # noqa: E501
    "20230101": "https://raw.githubusercontent.com/philipnye/journocoders-github-actions/223250add94eca80af678272a66a1a0e35cf6d5b/organisations.json",      # noqa: E501
    "20230201": "https://raw.githubusercontent.com/philipnye/journocoders-github-actions/e6cbd2f8f2e2cf2be45d73b3506a1bbf27581a39/organisations.json",      # noqa: E501
    "20230301": "https://raw.githubusercontent.com/philipnye/journocoders-github-actions/6b09887e67b757c3d972764687e3c0891b7d3512/organisations.json",      # noqa: E501
    "20230401": "https://raw.githubusercontent.com/philipnye/journocoders-github-actions/728e55d77a0e017ca65152ae5789ca16b91d70e2/organisations.json",      # noqa: E501
    "20230501": "https://raw.githubusercontent.com/philipnye/journocoders-github-actions/62eb11a748eb568e07e78efa043db1fd6bedc4bd/organisations.json",      # noqa: E501
    "20230601": "https://raw.githubusercontent.com/philipnye/journocoders-github-actions/9eda703a6147023c5862daac9b829b10dfc13b12/organisations.json",      # noqa: E501
    "20230701": "https://raw.githubusercontent.com/philipnye/journocoders-github-actions/d9c7ffa71c1e1bbcc0e520fdcf43b4ab9480a330/organisations.json",      # noqa: E501
    "20230801": "https://raw.githubusercontent.com/philipnye/journocoders-github-actions/57a5aa97fe7a6dfee70329cdb84b1ff3a69e60b0/organisations.json",      # noqa: E501
    "20230901": "https://raw.githubusercontent.com/philipnye/journocoders-github-actions/69454abe67678356943e47b7998cf1233ba4515b/organisations.json",      # noqa: E501
    "20231001": "https://raw.githubusercontent.com/philipnye/journocoders-github-actions/09df752cf1e52b142a8991f85789598434ac583b/organisations.json",      # noqa: E501
    "20231101": "https://raw.githubusercontent.com/philipnye/journocoders-github-actions/3ab1d0dca00fd92f6319a20f1aefa8317fb692ff/organisations.json",      # noqa: E501
    "20231201": "https://raw.githubusercontent.com/philipnye/journocoders-github-actions/6143347c513e8f1c6aaea13758508150f85ddabc/organisations.json",      # noqa: E501
    "20240101": "https://raw.githubusercontent.com/philipnye/journocoders-github-actions/3db2a2ed2ae7181f4157f4c0b72d5515301bd100/organisations.json",      # noqa: E501
    "20240201": "https://raw.githubusercontent.com/philipnye/journocoders-github-actions/e7c6bdae25f372f5cb932826c0cec63cdd0ea799/organisations.json",      # noqa: E501
    "20240301": "https://raw.githubusercontent.com/philipnye/journocoders-github-actions/4117fa2980c38b92c21b3d3e650ce91b0e03a167/organisations.json",      # noqa: E501
    "20240401": "https://raw.githubusercontent.com/philipnye/journocoders-github-actions/e8f639bc03476149e00844573889cea4e444b3c8/organisations.json",      # noqa: E501
    "20240501": "https://raw.githubusercontent.com/philipnye/journocoders-github-actions/5c1913785d58f8835c73df64b4a9b3c75d619eac/organisations.json",      # noqa: E501
    "20240601": "https://raw.githubusercontent.com/philipnye/journocoders-github-actions/49dde2d1fab2dbca2d8e181f43ef385989c6f910/organisations.json",      # noqa: E501
    "20240701": "https://raw.githubusercontent.com/philipnye/journocoders-github-actions/e580ae0675dae68268245649f937c3a42c206be8/organisations.json",      # noqa: E501
    "20240801": "https://raw.githubusercontent.com/philipnye/journocoders-github-actions/c838fd92849a1fa7f6a4d225e8615d8da1e11ebf/organisations.json",      # noqa: E501
    "20240901": "https://raw.githubusercontent.com/philipnye/journocoders-github-actions/596dd9e0467967269e34373503ce82b888a3f0a8/organisations.json",      # noqa: E501
    "20241001": "https://raw.githubusercontent.com/philipnye/journocoders-github-actions/d46c4fef0f109fe611ce3c053b594ac0aba16f80/organisations.json",      # noqa: E501
    "20241101": "https://raw.githubusercontent.com/philipnye/journocoders-github-actions/03eec3e31b1d2ee28b7be04e3845e1abe3555861/organisations.json",      # noqa: E501
}

# %%
# READ IN DATA
# Read in GOV.UK data
df = pd.DataFrame()
for date, url in data_urls.items():
    data = pd.read_json(url)
    data["date"] = date
    df = pd.concat([df, data])

# %%
# Read in CO data
df_co = pd.read_excel(
    "./temp/2024-11 - 04 - List of ALBs.xlsx"
)

# %%
# EDIT DATA
# Flatten 'details' column
df_edited = pd.concat([df.drop(["details"], axis=1), df["details"].apply(pd.Series)], axis=1)

# %%
# Strip all titles
df_edited["title"] = df_edited["title"].str.strip()

# %%
# Fix format values
# Organisations with analytics_identifier starting with 'D'
# NB: Ordnanace Survey - D38 - is a public corporation, therefore probably is correctly categorised
# as 'other'
df_edited.loc[
    df_edited["title"].isin([
        "Department for Business, Energy & Industrial Strategy",
        "Department for Digital, Culture, Media & Sport",
        "Department for International Trade",
        "Department for Levelling Up, Housing and Communities",
        "Office of the Secretary of State for Scotland",
        "Office of the Secretary of State for Wales",
    ]),
    "format"
] = "Ministerial department"

df_edited.loc[
    df_edited["title"].isin([
        "Office for National Statistics",
    ]),
    "format"
] = "Executive office"

# %%
# Add exclude flag
df_edited["exclude"] = False
df_edited.loc[
    df_edited["format"].isin([
        "Civil service",
        "Court",
        "Devolved administration",
        "Executive office",
        "Ministerial department",
        "Sub organisation",
        "Tribunal",
    ]),
    ["exclude", "exclude_reason"]
] = [True, "format"]

# %%
# Produce cross-tab of govuk_status, govuk_closed_status
pd.crosstab(df_edited["govuk_status"], df_edited["govuk_closed_status"], dropna=False)

# %%
# Clean up govuk_status values

# Set 'transitioning' to 'closed' or 'devolved' based on govuk_closed_status
df_edited.loc[
    (~df_edited["govuk_status"].isin(["closed", "live"])) &
    (df_edited["govuk_closed_status"].isin([
        "changed_name",
        "left_gov",
        "merged",
        "no_longer_exists",
        "replaced",
        "split",
    ])),
    "govuk_status"
] = "closed"
df_edited.loc[
    (~df_edited["govuk_status"].isin(["closed", "live"])) &
    (df_edited["govuk_closed_status"] == "devolved"),
    "govuk_status"
] = "devolved"

# Set remaining 'exempt' to 'live'
# NB: Done on the thesis that this is just a descriptor of whether something has a separate website
df_edited.loc[
    df_edited["govuk_status"] == "exempt",
    "govuk_status"
] = "live"

# Assert there are no 'exempt' govuk_status values
assert df_edited["govuk_status"].ne("exempt").all()

# %%
# Clean up govuk_closed_status values

# Set to blank where govuk_status is 'live'
df_edited.loc[
    df_edited["govuk_status"] == "live",
    "govuk_closed_status"
] = pd.NA

# %%
# Flag govuk_status values for exclusion
df_edited.loc[
    df_edited["govuk_status"].isin([
        "closed",
        "devolved",
        "joining",
    ]),
    ["exclude", "exclude_reason"]
] = [True, "govuk_status"]

# %%
# Produce cross-tab of govuk_status, govuk_closed_status
pd.crosstab(df_edited["govuk_status"], df_edited["govuk_closed_status"], dropna=False)

# %%
# Produce cross-tab of format, govuk_status
pd.crosstab(df_edited["format"], df_edited["govuk_status"], dropna=False)

# %%
# Reorder columns
df_edited = df_edited[[
    "date",
    "analytics_identifier",
    "title",
    "format",
    "govuk_status",
    "govuk_closed_status",
    "closed_at",
    "parent_organisations",
    "child_organisations",
    "superseded_organisations",
    "superseding_organisations",
    "updated_at",
    "exclude",
    "exclude_reason",
]]

# %%
# EXPLORE DATA
# Exclude organisations
df_edited = df_edited.loc[
    ~(
        (df_edited["exclude"]) &
        (df_edited["exclude_reason"] == "govuk_status")
    )
]

# %%
# Check 'format' values
df_edited["format"].value_counts()

# %%
# Identify first and last appearance of organisations
# NB: 'exclude' is included as a column here. Not doing so results in some side effects for
# organisations that change format, to/from a type that we exclude
df_firstlast = df_edited\
    .groupby(["title", "analytics_identifier", "exclude", "exclude_reason"], dropna=False)["date"]\
    .agg(["first", "last"])\
    .reset_index()

# %%
df_firstlast

# %% Identify excluded organisations
# NB: This uses the last appearance of an organisation, as that is expected to have more
# accurate information
df_excluded = df_firstlast[["title", "analytics_identifier", "first", "last"]].merge(
    df_edited.loc[
        df_edited["exclude"]
    ][[
        "analytics_identifier",
        "date",
        "format",
        "govuk_status",
        "govuk_closed_status",
        "superseded_organisations",
        "superseding_organisations"
    ]],
    how="inner",
    left_on=["analytics_identifier", "last"],
    right_on=["analytics_identifier", "date"],
    validate="1:1",
)

df_excluded.loc[
    df_excluded["first"] == df_edited["date"].min(),
    "first"
] = pd.NA
df_excluded.loc[
    df_excluded["last"] == df_edited["date"].max(),
    "last"
] = pd.NA

# Reorder and rename columns
df_excluded = df_excluded[[
    "title",
    "analytics_identifier",
    "format",
    "govuk_status",
    "govuk_closed_status",
    "first",
    "last",
    "date",
    "superseded_organisations",
    "superseding_organisations"
]].rename(columns={
    "first": "first_appearance",
    "last": "last_appearance",
    "date": "data_as_at"
})

# %%
# Identify organisations that might have started since the start of 2023
df_possibleneworgs = df_firstlast.loc[
    (df_firstlast["first"].str.contains("2023")) |
    (df_firstlast["first"].str.contains("2024"))
][["title", "analytics_identifier", "first"]].merge(
    df_edited.loc[
        ~df_edited["exclude"]
    ][[
        "analytics_identifier",
        "date",
        "format",
        "govuk_status",
        "govuk_closed_status",
        "superseded_organisations",
        "superseding_organisations"
    ]],
    how="inner",
    left_on=["analytics_identifier", "first"],
    right_on=["analytics_identifier", "date"],
    validate="1:1",
).drop(columns=["date"]).sort_values(by="first")

# %%
# Identify organisations that might have closed since the start of 2023
df_possibleclosedorgs = df_firstlast.loc[
    (
        (df_firstlast["last"].str.contains("2023")) |
        (df_firstlast["last"].str.contains("2024"))
    ) &
    (df_firstlast["last"] != df_edited["date"].max())
][["title", "analytics_identifier", "last"]].merge(
    df_edited.loc[
        ~df_edited["exclude"]
    ][[
        "analytics_identifier",
        "date",
        "format",
        "govuk_status",
        "govuk_closed_status",
        "superseded_organisations",
        "superseding_organisations"
    ]],
    how="inner",
    left_on=["analytics_identifier", "last"],
    right_on=["analytics_identifier", "date"],
    validate="1:1",
).drop(columns=["date"]).sort_values(by="last")

# %%
# Compare our list of organisations to CO list
df_live = df_firstlast[["title", "analytics_identifier", "last"]].merge(
    df_edited.loc[
        ~df_edited["exclude"]
    ][[
        "analytics_identifier",
        "date",
        "format",
        "govuk_status",
        "govuk_closed_status",
        "superseded_organisations",
        "superseding_organisations"
    ]],
    how="inner",
    left_on=["analytics_identifier", "last"],
    right_on=["analytics_identifier", "date"],
    validate="1:1",
).drop(columns=["date"]).sort_values(by="title")

# %%
df_merge = mo.fuzzy_merge(
    df_live,
    df_co,
    column_left="title",
    column_right="overall_organisation",
    drop_na=False,
    score_cutoff=90,
)

# %%
# Look for organisations in the CO list not matched to the GOV.UK list
df_co_nomatch = df_co.loc[
    ~df_co["overall_organisation"].isin(df_merge["overall_organisation"])
]

# %%
df_co_nomatch

# %%
# EXPORT DATA
# Export data
excel.ExcelFormatter.header_style = None

with pd.ExcelWriter("./temp/Comparison of GOV.UK, CO lists.xlsx") as writer:

    df_edited.to_excel(writer, sheet_name="Full GOV.UK list", index=False)
    df_excluded.to_excel(writer, sheet_name="Excluded orgs", index=False)
    df_possibleneworgs.to_excel(writer, sheet_name="Possible new orgs", index=False)
    df_possibleclosedorgs.to_excel(writer, sheet_name="Possible closed orgs", index=False)
    df_merge.to_excel(writer, sheet_name="Comparison of GOV.UK, CO lists", index=True)

# %%
