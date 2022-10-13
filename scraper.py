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

import json

import requests

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
page_number = 1
record_list = []

call_api(url_stub, page_number, headers)

# %%
# Save output
with open('organisations.json', 'w') as f:
    json.dump(record_list, f)

# %%
