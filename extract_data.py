# %%
"""
    Purpose
        Extract organisations data
    Inputs
        - Web: GOV.UK organisations API
            - Data underpinning https://www.gov.uk/government/organisations
    Outputs
        - JSON: organisations.json
            - Latest version of organisations data
    Parameters
        None
    Notes
        None
"""

import json
import time

import requests

# %%
# Prepare to call API
url_stub = "https://www.gov.uk/api/organisations?page="
headers = {"accept": "application/json"}


def call_api(url_stub, page_number, headers, max_retries=5):
    url = url_stub + str(page_number)

    for attempt in range(max_retries):
        try:
            r = requests.get(url, headers=headers, timeout=30)

            if not r.ok:
                if r.status_code == 503:
                    print(f"Warning: API temporarily unavailable (503) for page {page_number}. Attempt {attempt + 1}/{max_retries}")
                    if attempt < max_retries - 1:
                        time.sleep(5 * (attempt + 1))
                        continue
                    else:
                        print(f"Error: Failed after {max_retries} attempts. Stopping at page {page_number}")
                        print(f"Retrieved {len(record_list)} organisations before failure.")
                        return False
                else:
                    print(f"Error: API returned status code {r.status_code} for page {page_number}")
                    print(f"Response content: {r.text[:500]}")
                    return False

            try:
                data = r.json()
            except requests.JSONDecodeError as e:
                print(f"Error: Unable to parse JSON from page {page_number}")
                print(f"Status code: {r.status_code}")
                print(f"Response content: {r.text[:500]}")
                print(f"JSON decode error: {e}")
                return False

            # Extract results if present
            if "results" in data:
                record_list.extend(data["results"])
                print(f"Retrieved page {page_number}: {len(data['results'])} organisations (total: {len(record_list)})")
            else:
                print(f"Warning: No 'results' key found in response for page {page_number}")

            # Check for next page
            if data.get("next_page_url"):
                page_number += 1
                success = call_api(url_stub, page_number, headers, max_retries)
                return success
            else:
                print(f"Completed: Retrieved all {len(record_list)} organisations")
                return True

        except requests.exceptions.RequestException as e:
            print(f"Error: Request failed for page {page_number}. Attempt {attempt + 1}/{max_retries}")
            print(f"Error details: {e}")
            if attempt < max_retries - 1:
                time.sleep(5 * (attempt + 1))
            else:
                print(f"Failed after {max_retries} attempts. Stopping at page {page_number}")
                return False

    return False


# %%
# Call API
page_number = 1
record_list = []

success = call_api(url_stub, page_number, headers)

# %%
# Save output
if success and record_list:
    with open("organisations.json", "w") as f:
        json.dump(record_list, f)
    print(f"Saved {len(record_list)} organisations to organisations.json")
else:
    if not success:
        print("Error: Incomplete data retrieval. File not saved.")
    else:
        print("Warning: No organisations retrieved. File not saved.")

# %%
