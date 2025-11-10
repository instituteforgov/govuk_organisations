#%%
# Set URL address and read in JSON
import pandas as pd
url = "https://raw.githubusercontent.com/instituteforgov/govuk_organisations/refs/heads/main/organisations.json"

df = pd.read_json(url)

# %%
#Flatten "details" column
df_edited = pd.concat(
    [df.drop(columns = ["details"]), df["details"].apply(pd.Series)], axis=1
    )

# %%
# Remove redundant columns
dropped_cols = [
    "updated_at", "parent_organisations", "child_organisations", "superseded_organisations", "superseding_organisations",
    "slug", "abbreviation", "logo_formatted_name", "organisation_brand_colour_class_name",
    "organisation_logo_type_class_name", "content_id"
] 

df_edited = df_edited.drop(columns = dropped_cols)

# %%
