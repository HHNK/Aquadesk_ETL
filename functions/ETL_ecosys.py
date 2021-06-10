import json
import urllib
import pandas as pd
import numpy as np

'''
    File name: ETL_ecosys.py
    Author: Jeroen Baars (j.baars@hhnk.nl)
    Date created: 12/04/2021
    Date last modified: 02/06/2021
    Python Version: 3.8.6
'''


class ecosys_dataparser():
    def __init__(self,
                 api_url: str):
        """Initialize class and save a API url as variable

        Args:
            api_url (str, optional): Standard API url for querying. Defaults to None.
        """
        self.api_url = api_url

    def http_error_check(self,
                         e: urllib.error.HTTPError) -> bool:
        """Function to parse HTTP error from API

        Args:
            e (urllib.error.HTTPError, optional): HTTP error from API. Defaults to None.

        Returns:
            Boolean: return True for break in while loop
        """
        if e.code == 403:
            print('Invalid api key')
            return True
        else:
            print(f'Error: {e.reason}')
            return True

    def url_builder(self,
                    query_url: str,
                    query_filter: str = None,
                    skip_properties: str = None,
                    page_number: int = 1,
                    page_size: int = 10000) -> str:
        """Builds the query url for every page and with defined endpoint, filters and skip properties

        Args:
            query_url (str): API endpoint for query
            query_filter (str, optional): Filtering within API. Defaults to None.
            skip_properties (str, optional): Properties to skip in response. Defaults to None.
            page_number (int, optional): Starting page number. Defaults to 1.
            page_size (int, optional): Default max page size. Defaults to 10000.

        Returns:
            str: [description]
        """
        # Build base URL
        base = f'{self.api_url+query_url}?page={page_number}&pagesize={page_size}'
        # Add filter is present
        if query_filter != None:
            base = f'{base}&filter={query_filter}'
        # Add skip properties if present
        if skip_properties != None:
            base = f'{base}&skipproperties={skip_properties}'
        # Replace space for URL
        base = base.replace(" ", "%20")
        return base

    def check_ending(self,
                     response: list,
                     page_size: int) -> bool:
        """Check if ending of the response pages is reached (Response size smaller than max page size)

        Args:
            response (list, optional): Response list from query. Defaults to None.
            page_size (int, optional): Max page size. Defaults to None.

        Returns:
            bool: responses True if end reached
        """
        if len(response) < page_size:
            return True
        else:
            return False

    def return_query(self,
                     query_url: str,
                     query_filter: str = None,
                     skip_properties: str = None,
                     api_key: str = None,
                     page: int = 1,
                     page_size: int = 10000) -> list:
        """Returns query from API, for testing and discovery purposes.
        Returns json result and not as pd.DataFrame.

        Args:
            query_url (str): API endpoint for query
            query_filter (str, optional): Filtering within API. Defaults to None.
            skip_properties (str, optional): Properties to skip in response. Defaults to None.
            api_key (str, optional): API key for identification as company. Defaults to None.
            page_number (int, optional): Starting page number. Defaults to 1.
            page_size (int, optional): Default max page size. Defaults to 10000.
        Returns:
            list: return query result
        """
        # Create request URL
        request_url = self.url_builder(
            query_url, query_filter, skip_properties, page, page_size)
        # Try to request the URL and return result
        try:
            request = urllib.request.Request(
                request_url, headers={'x-api-key': api_key})
            response = urllib.request.urlopen(request)
            return json.load(response)['result']
        # Throw exception if HTTP error
        except urllib.error.HTTPError as e:
            self.http_error_check(e)

    def parse_data_dump(self,
                        api_key: str,
                        query_url: str,
                        query_filter: str = None,
                        skip_properties: str = None,
                        page: int = 1,
                        page_size: int = 10000,
                        parse_watertypes = False):
        """Parse through all pages and stream to path file location as csv

        Args:
            api_key (str, optional): API key for identification as company. Defaults to None.
            query_url (str): API endpoint for query
            query_filter (str, optional): Filtering within API. Defaults to None.
            skip_properties (str, optional): Properties to skip in response. Defaults to None.
            page (int, optional): Starting page number. Defaults to 1.
            page_size (int, optional): Default max page size. Defaults to 10000.
            parse_watertypes (list, optional): Can be used to extra parse watertypes column into split columns. Defaults to False.
        """

        json_request_list = []

        while True:
            # Create URL in for loop to parse through pages
            request_url = self.url_builder(
                query_url, query_filter, skip_properties, page, page_size)
            try:
                request = urllib.request.Request(
                    request_url, headers={'x-api-key': api_key})
                response = json.load(urllib.request.urlopen(request))['result']
                json_request_list.extend(response)

                # Check for ending and otherwise continue
                if self.check_ending(response, page_size):
                    return self.return_dataframe(json_request_list, parse_watertypes)

                page += 1

            except urllib.error.HTTPError as e:
                if self.http_error_check(e):
                    break

    def return_dataframe(self, json_object: list, parse_watertypes: bool) -> pd.DataFrame:
        """returns dataframe and parses watertypes column if it

        Args:
            data (list): JSON object from aquadesk API

        Returns:
            pd.DataFrame: returns dataframe of query
        """
        df = pd.json_normalize(json_object)
        if ("watertypes" in df.columns) & (parse_watertypes == True):
            watertypes_nan_dict = {'classificationsystem': np.nan, 'watertypecode': np.nan}
            return pd.concat([df.drop("watertypes", axis=1),
                              pd.json_normalize(df["watertypes"].apply(lambda x: x[0] if isinstance(x,list) else watertypes_nan_dict))], axis=1)
        else:
            return df
