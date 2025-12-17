# GOV.UK Organisations - Data and analytics #

Scripts to extract data from the GOV.UK 'Organisations' API and writing it to a database.

### Data ###

The plaintext information is available at the ['Departments, agencies and public bodies'](https://www.gov.uk/government/organisations) webpage. The scripts use data drawn from the [API](https://www.gov.uk/api/organisations).

### Extracting data ### 

`extract_data.py` downloads data from the API as a JSON file once daily using GitHub [actions]()

### Databases ###

`orgs_database.py` (when complete) will write the latest API data to a `SQL` table, compare it to the previous version, and highlight any changes - such as new organisations, mergers, abolitions. etc.

### TBC... ####