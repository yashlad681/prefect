"""
Tasks for interacting with Yelp

Note, to authenticate with the Yelp API use upstream PrefectSecret tasks to pass in
'"YELP_API_KEY"' (see https://www.yelp.com/developers/v3/manage_app)
"""

from prefect.tasks.yelp.yelp import BusinessSearch, BusinessReviews
