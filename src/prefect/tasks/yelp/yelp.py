from prefect import Task
import requests
from prefect.utilities.tasks import defaults_from_attrs
from typing import Any


class BusinessSearch(Task):
    """
    Task for searching for bussiness on Yelp

    Args:
      - term (str): Optional. Search term, for example "food" or "resturants". The term may also be bussiness names, such as "Starbucks". If a term is not included, it will default to search across businesses from a small number of popular categories.
      - location (str): Required if either latitdude or longitude is not provided. This string indicates the gerographic area to be used when searching for businesses. Examples: "New York City", "NYC", "350 5th Ave, New York, NY 10118". Businesses returned in the response may not be strictly whithin the specified location. 
      - latitude (str): Required if location is not provided. Latitude of the location you want to search.
      - longitude (str): Required if location is not provided. Longitude of the location you want to search.
      - **kwargs (dict, optional): additional arguments to pass to the Task constructor
    """

    def __init__(self, term: str = None, location: str = None, latitude: str = None, longitude: str = None, **kwargs: Any):
        self.term = term
        self.location = location
        self.latitude = latitude
        self.longitude = longitude

        super().__init__(**kwargs)

    @defaults_from_attrs("term", "location", "latitude", "longitude")
    def run(self, term: str = None, location: str = None, latitude: str = None, longitude: str = None, yelp_api_key: str = "YELP_API_KEY") -> None:
        """
        Task run method.

        Args:
        - term (str): Optional. Search term, for example "food" or "resturants". The term may also be bussiness names, such as "Starbucks". If a term is not included, it will default to search across businesses from a small number of popular categories.
        - location (str): Required if either latitdude or longitude is not provided. This string indicates the gerographic area to be used when searching for businesses. Examples: "New York City", "NYC", "350 5th Ave, New York, NY 10118". Businesses returned in the response may not be strictly whithin the specified location. 
        - latitude (str): Required if location is not provided. Latitude of the location you want to search.
        - longitude (str): Required if location is not provided. Longitude of the location you want to search.
        - yelp_api_key (str): The name of the Prefect Secret where you've stored your API key

        Returns:
          - The response body if the status code is 200. None otherwise.
        """
        if yelp_api_key is None:
            raisee ValueError("An API key must be provided")

        if location is None and latitude is None and longitude is None:
            raise ValueError("Either a location or latitude/longitude needs to be provided")

        url = f"https://api.yelp.com/v3/businesses/search?term={term}&latitude={latidude}&longitude={longitude}&location={location}"

        response = requests.get(
            url, headers={"Content-Type": "application/type", "Authorization": "Bearer {}".format(yelp_api_key)})

        result = None
        if response.status_code == 200:
            result = response.json()
        else:
            raise ValueError(response.reason)

        return result


class BusinessReviews(Task):
    """
    Task for searching for reviews of a bussiness.

    Args:
      - business_id (str): The business id. 
      - **kwargs (dict, optional): additional arguments to pass to the Task constructor
    """

    def __init__(self, business_id: str = None, **kwargs: Any):
        self.business_id = business_id

        super().__init__(**kwargs)

    @defaults_from_attrs("business_id")
    def run(self, business_id: str = None, yelp_api_key: str = "YELP_API_KEY") -> None:
        """
        Task run method.

        Args:
        - business_id (str): The business id
        - yelp_api_key (str): The name of the Prefect Secret where you've stored your API key

        Returns:
          - The response body if the status code is 200. None otherwise
        """
        if yelp_api_key is None:
            raisee ValueError("An API key must be provided")

        if business_id is None:
            raise ValueError("A business id must be provided")

        url = f"https://api.yelp.com/v3/businesses/{id}/reviews"

        response = requests.get(
            url, headers={"Content-Type": "application/type", "Authorization": "Bearer {}".format(yelp_api_key)})

        result = None
        if response.status_code == 200:
            result = response.json()
        else:
            raise ValueError(response.reason)

        return result
