# GOV.UK Organisations - Data and analytics

Scripts to extract data from the GOV.UK 'Organisations' API and write it to a database.

## Data

The plaintext information is available at the ['Departments, agencies and public bodies'](https://www.gov.uk/government/organisations) webpage. The scripts use data drawn from the [API](https://www.gov.uk/api/organisations).

## Extracting data

`extract_data.py` downloads data from the API as a JSON file once daily using GitHub [actions](https://github.com/features/actions).

## Database tables

`orgs_database.py` edits the JSON data and writes it to a SQL database table.

`orgs_parenthood.py` uses the data in the the organisations table to a table of parent organisations IDs and the IDs of their child organisations. (For example, the MoJ is the parent organisation of HM Prison and Probation Service.)

## TBC...
