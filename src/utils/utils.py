import requests


def run_query(api: str, query: str) -> dict:
    """A simple function to use requests.post to make the API call."""
    request = requests.post(api, json={"query": query})
    if request.status_code == 200:
        return request.json()
    else:
        raise Exception(
            "Query failed to run by returning code of {}. {}".format(
                request.status_code, query
            )
        )
