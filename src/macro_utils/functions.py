# This notebook stores all of the key functions for:
# Extracting the data
# Cleaning the Data
# Analysing the Data


# IMPORT PACKAGES
# Web - Scraping and API Requests
import requests
from httpx import AsyncClient, Response
from parsel import Selector
import parsel
import jmespath
import asyncio
from urllib.parse import urlencode

# Data Manipulation and Analysis
import pandas as pd
from pprint import pprint 
import json
from typing import List
from typing import TypedDict

# Database Connection
from sqlalchemy import create_engine

# url displays
from IPython.display import display, Markdown

# File and System Operations
import os
import sys

# DIRECTORY SETUP

### Find the directory of the current file
__file__ = "functions.py"

current_dir = os.path.dirname(os.path.abspath(__file__))

## Set it to be the current path of the system
sys.path.insert(0,os.path.join(current_dir, '..', '..'))

## Set Up The Paths of the Key Outside Directories/Files
credentials_file_path = os.path.join(current_dir, '..', '..', "credentials.json")
data_folder_path = os.path.join(current_dir, '..', '..', "data")

# REQUESTS SETUP
# 1. establish HTTP client with browser-like headers to avoid being blocked
client = AsyncClient(headers={
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)", # mimic browser use (baseline)
    "Accept": "application/json",  # Accept json apis
    "Referer": "https://www.rightmove.co.uk/",  # Helps mimic browser use
})



# THE FUNCTIONS

## EXTRACTION

### Function to find locations
async def find_locations(query: str) -> List[str]:
    """use rightmove's typeahead api to find location IDs. Returns list of location IDs in most likely order"""
    # Tokenize the query string into two-character segments separated by slashes, as required by the API
    tokenize_query = "".join(c + ("/" if i % 2 == 0 else "") for i, c in enumerate(query.upper(), start=1))
    # Construct the URL for the typeahead API using the tokenized query
    url = f"https://www.rightmove.co.uk/typeAhead/uknostreet/{tokenize_query.strip('/')}/"
    # Make an asynchronous GET request to the API
    response = await client.get(url)
    # Parse the JSON response from the API
    data = json.loads(response.text)
    # Extract and return the list of location identifiers from the response
    return [prediction["locationIdentifier"] for prediction in data["typeAheadLocations"]]


### Function to scrape results for a given location for multiple pages
async def scrape_search(location_id: str, total_results = 250) -> str:
    """
    Scrapes rental property listings from Rightmove for a given location identifier, handling pagination and returning all results.
    """
    RESULTS_PER_PAGE = 24

    def make_url(offset: int) -> str:
        url = "https://www.rightmove.co.uk/api/_search?"
        params = {
            "areaSizeUnit": "sqm", # the units for the size of each property
            "channel": "RENT",  # BUY or RENT - for my puyrposes, rent is the most relevant
            "currencyCode": "GBP", # chosen currency
            "includeSSTC": "false", # an empty search parameter
            "index": offset, # the number of the search result/property displayed at the start of the page 
            "isFetching": "false", 
            "locationIdentifier": location_id, # the location we wish to search for (London)
            "numberOfPropertiesPerPage": RESULTS_PER_PAGE,
            "radius": "0.0", # how far away we are allowed to be from the geographgical boundaries of the region
            "sortType": "6", # the sorting mechanism for search results
            "viewType": "LIST", # how results appear
        }
        return url + urlencode(params)

    # Build the URL for the first page of results
    url = make_url(0)
    # print(f"Requesting URL: {url}")
    # Send the request to the Rightmove API for the first page
    first_page = await client.get(url)
    # print(f"First page status: {first_page.status_code}")
    # Parse the JSON response from the first page
    first_page_data = first_page.json()
    results = first_page_data["properties"]

    # Prepare to fetch additional pages if there are more results
    other_pages = []
    # rightmove sets the API limit to 1000 properties, but here max_api_results is set to 20 for demonstration/testing
    max_api_results = 1000    
    # The 'index' parameter in the URL specifies the starting property for each page
    for offset in range(RESULTS_PER_PAGE, total_results, RESULTS_PER_PAGE):
        # Stop scraping more pages when the scraper reaches the API limit
        if offset >= max_api_results: 
            break
        print(f"Scheduling request for offset: {offset}")
        # Schedule the request for the next page
        other_pages.append(client.get(make_url(offset)))
    # Asynchronously (using async) gather and process all additional page responses
    for response in asyncio.as_completed(other_pages):
        response = await response
        # print(f"Received response for additional page: {response.status_code}")
        data = json.loads(response.text)
        results.extend(data['properties'])
    
    # display the number of results that we managed to parse across multiple pages
    total_results = len(results)
    print(f"Found {total_results} properties")
    return results




## CLEANING

### A function that filters out only the desired columns
def filter_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filters the input DataFrame to retain only the columns relevant for property analysis.

    Args:
        df (pd.DataFrame): The DataFrame to filter.

    Returns:
        pd.DataFrame: A DataFrame containing only the selected columns of interest.
    """
    # Define the list of columns to keep in the filtered DataFrame
    base_cols = [
        'id',
        'bedrooms',
        'bathrooms',
        'numberOfImages',
        'displayAddress',
        'location.latitude',
        'location.longitude',
        'propertySubType',
        'listingUpdate.listingUpdateReason',
        'listingUpdate.listingUpdateDate',
        'price.amount',
        'price.frequency',
        'premiumListing',
        'featuredProperty',
        'transactionType',
        'students',
        'displaySize',
        'propertyUrl',
        'firstVisibleDate',
        'addedOrReduced',
        'propertyTypeFullDescription'
    ]
    # Assign the columns of interest (can be extended or modified if needed)
    columns_of_interest = base_cols
    # Filter the DataFrame to include only the columns of interest
    filtered_df = df[columns_of_interest]
    # Create a price per bedroom column
    filtered_df = filtered_df.copy()
    filtered_df.loc[:, "price_per_bed"] = filtered_df["price.amount"] / filtered_df["bedrooms"]
    # remove rows with a duplicated id
    filtered_df = filtered_df.drop_duplicates(subset="id")
    # Return the filtered DataFrame
    return filtered_df


# Clean the column names
def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Renames selected columns of a DataFrame to make them more readable and SQL-friendly.
    Specifically, it replaces nested JSON column names (with dots) with simpler names.
    """
    # Create a mapping of the columns whose names we are changing
    rename_map = {
        "location.latitude": "latitude",
        "location.longitude": "longitude",
        "listingUpdate.listingUpdateReason": "listingUpdateReason",
        "listingUpdate.listingUpdateDate": "listingUpdateDate",
        "price.amount": "priceAmount",
        "price.frequency": "priceFrequency",
    }
    # then, actually rename the columns
    return df.rename(columns=rename_map)


# Define a function that cleans a dataframe for regression analysis
def clean_for_reg(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans the input DataFrame for regression analysis.

    Inputs:
        df (pd.DataFrame): The input DataFrame containing property data.
    Output:
        pd.DataFrame: The cleaned DataFrame suitable for regression.
    """
    # Adjust price_per_bed to monthly if priceFrequency is 'weekly', else keep as is
    df = df.copy()
    df["price_per_bed"] = df.apply(
        lambda row: row["price_per_bed"] * 52 / 12 if row["priceFrequency"] == "weekly" else row["price_per_bed"],
        axis=1
    )

    # Filter for rows where priceFrequency is 'monthly' or 'weekly' (after adjustment)
    reg_data = df[df["priceFrequency"].isin(["monthly", "weekly"])]

    # Eliminate all rows where travel_time is non-numeric or not between 60 and 5400 seconds
    reg_data = reg_data[pd.to_numeric(reg_data["travel_time"], errors="coerce").between(60, 5400)]

    # Eliminate all rows where bathrooms is non-numeric or not between 1 and 6
    reg_data = reg_data[pd.to_numeric(reg_data["bathrooms"], errors="coerce").between(1, 6)]

    # Filter for rows where price_per_bed is a valid number between 100 and 10,000
    reg_data = reg_data[pd.to_numeric(reg_data["price_per_bed"], errors="coerce").between(100, 10000)]
    
    # return the clean data
    return reg_data



# TRAVEL TIME DATA

## Define a function that generates a payload to pass into the API

def create_payload(df: pd.DataFrame, search_id: str="1", transportation_type: str = "public_transport") -> dict:
    """
    Creates a payload dictionary for the TravelTime API using property locations from a DataFrame.
    The payload includes an origin (Bank Station) and destination locations (properties), 
    and sets up the search parameters for a one-to-many public transport commute time query.
    """
    # Define origin (Bank Station - a key commuting hub)
    origin = {
        "id": "Origin",
        "coords": {"lat": 51.513, "lng": -0.088}
    }
    # Ensure the 'id' column is of string type for API compatibility
    df["id"] = df["id"].astype(str)

    # Select and rename latitude/longitude columns for API format
    locations = df[["id", "latitude", "longitude"]].rename(
        columns={"latitude": "lat", "longitude": "lng"}
    )

    # Convert DataFrame rows to a list of dicts for each destination
    destinations = locations.to_dict(orient="records")
    destination_locations = [
        {
            "id": d["id"],
            "coords": {"lat": d["lat"], "lng": d["lng"]}
        } for d in destinations
    ]

    # Build the final payload structure for the API request
    payload = {
        "arrival_searches": {
            "one_to_many": [
                {
                    "id": search_id,  # Unique search identifier
                    "departure_location_id": "Origin",  # Start from Bank Station
                    "arrival_location_ids": df["id"].tolist(),  # List of property IDs as destinations
                    "transportation": {"type": transportation_type},  # Mode of transport
                    "travel_time": 10800,  # Max travel time in seconds (3 hours)
                    "arrival_time_period": "weekday_morning",  # Commute time window
                    "properties": ["travel_time", "distance"]  # Data to return
                }
            ]
        },
        "locations": [origin] + destination_locations  # All locations (origin + destinations)
    }

    return payload


# Find underpriced flats relative to others with the same travel time
def find_underpriced(df, user_budget=1200):
    """Finds underpriced flats relative to others with the same travel time.
    Takes as input a dataframe with the information, and the user's budget, and outputs a sorted 
    dataframe with the most underpriced rental properties at the top, and
    a recommendation with a link to the most ideal such property.
    """
    # Make a copy and calculate savings
    df = df.copy()
    df['savings'] = df['predicted_price_per_bed'] - df['price_per_bed']

    # Filter by budget
    budget_data = df[df['price_per_bed'] <= user_budget]

    # Sort descending by savings
    sorted_data = budget_data.sort_values(by='savings', ascending=False)

    # Print the top property
    if not sorted_data.empty:
        top_flat = sorted_data.iloc[0]
        address = top_flat['displayAddress']
        price = top_flat['price_per_bed']
        pred_price = top_flat['predicted_price_per_bed']
        url = top_flat['propertyUrl']
        full_url = f"https://www.rightmove.co.uk{url}" if url.startswith('/') else url

        print(f"Your most underpriced flat is at '{address}' with a rent per bedroom of £{price:.2f}, "
              f"while similar properties fetch £{pred_price:.2f}.\n"
              f"View it here: {full_url}")
    else:
        print("No flats found within your budget.")

    return sorted_data

