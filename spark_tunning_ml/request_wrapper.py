import requests

from spark_tunning_ml.logger import logger


class RequestWrapper:
    """
    A simple request wrapper for making HTTP requests using the requests library.
    """

    def __init__(self, base_url):
        """
        Initialize the RequestWrapper with a base URL.

        Args:
            base_url (str): The base URL for the requests.
        """
        self.base_url = base_url

    def request(self, method, endpoint, params=None, data=None, json=None):
        """
        Make an HTTP request to the specified endpoint.

        Args:
            method (str): The HTTP method to use ('GET' or 'POST').
            endpoint (str): The endpoint to send the request to.
            params (dict, optional): Query parameters for the request.
            data (dict, optional): Form data for a POST request.
            json (dict, optional): JSON data for a POST request.

        Returns:
            dict: The JSON content of the response.
        """
        url = f"{self.base_url}{endpoint}"
        logger.info(f"Making {method} request to {url}.")

        if method.upper() == "GET":
            response = requests.get(url, params=params)
        elif method.upper() == "POST":
            response = requests.post(url, data=data, json=json)
        else:
            raise ValueError(
                "Unsupported HTTP method. Supported methods: 'GET', 'POST'."
            )

        response.raise_for_status()

        return response.json()
