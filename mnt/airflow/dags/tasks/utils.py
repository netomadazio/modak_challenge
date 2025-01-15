import requests
import json
import logging
from io import StringIO
import pandas as pd

def fetch_data_from_url(url):
    """
    Fetch data from the given URL.
    This function sends a GET request to the specified URL and returns the response
    if the request is successful. If the request fails, it logs an error message
    and raises the exception.
    Args:
        url (str): The URL to fetch data from.
    Returns:
        requests.Response: The response object resulting from the GET request.
    Raises:
        requests.RequestException: If there is an issue with the GET request.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response
    except requests.RequestException as e:
        logging.error(f"Failed to fetch data: {e}")
        raise

def fetch_json_from_url(url):
    """
    Fetches JSON data from a given URL and normalizes it into a pandas DataFrame.

    Args:
        url (str): The URL to fetch the JSON data from.

    Returns:
        pandas.DataFrame: A DataFrame containing the normalized JSON data.

    Raises:
        requests.exceptions.RequestException: If there is an issue with the HTTP request.
        json.JSONDecodeError: If the response is not valid JSON.
    """

    response = fetch_data_from_url(url)
    data = json.loads(response.text)
    return pd.json_normalize(data, sep='_', max_level=None)

def get_csv_df(url):
    """
    Fetches CSV data from a given URL and returns it as a pandas DataFrame.

    Args:
        url (str): The URL to fetch the CSV data from.

    Returns:
        pandas.DataFrame: The CSV data as a DataFrame.

    Raises:
        requests.exceptions.RequestException: If there is an issue with the network request.
        pandas.errors.ParserError: If there is an issue parsing the CSV data.
    """

    response = fetch_data_from_url(url)
    return pd.read_csv(StringIO(response.text))
