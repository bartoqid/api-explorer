from dotenv import load_dotenv
import os
load_dotenv()
token = os.environ.get('DEVELOPER_TOKEN')
#“Life is a like an onion, you peel it off one layer at time, and sometimes you weep” ― Carl Sandberg
from pan_cortex_data_lake import Credentials, QueryService

url = "https://api.us.cdl.paloaltonetworks.com"

c = Credentials(developer_token=token)

qs = QueryService(url=url, force_trace=True, credentials=c)
#xxx is instance ID, go to API Explorer, click on the Authorization and you will find the instance ID there
SQL = "SELECT source_user FROM `xxx.firewall.globalprotect` where status.value = 'success' LIMIT 5"

query_params = {"query": SQL}

q = qs.create_query(query_params=query_params)

def json_extract(obj, key):
    """Recursively fetch values from nested JSON."""
    arr = []

    def extract(obj, arr, key):
        """Recursively search for values of key in JSON tree."""
        if isinstance(obj, dict):
            for k, v in obj.items():
                if isinstance(v, (dict, list)):
                    extract(v, arr, key)
                elif k == key:
                    arr.append(v)
        elif isinstance(obj, list):
            for item in obj:
                extract(item, arr, key)
        return arr

    values = extract(obj, arr, key)
    return values

for p in qs.iter_job_results(job_id=q.json()['jobId'], result_format="valuesDictionary"):
    a = p.json()
    user = json_extract(a, "source_user")
    print(user)