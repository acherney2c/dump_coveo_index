# Dump Coveo Index

This script fetches data from the Coveo API and saves the results into a JSON file. To include dictionary fields in the output - use Admin search token, append viewAllContent=1 to the search/v2 API call and add 'debug': 'true' in the payload.

When the search token expires, the script will save last rowid into a file and resume from the last successfuly retrieved rowid in the next run.

## Requirements

- Python 3.x
- `requests`, `csv`, `json` and `logging` libraries

## Configurations

	1.	SEARCH_TOKEN: Replace the SEARCH_TOKEN variable with your Admin Search Token. If you don't need dictionary fields then this can be an API KEY.
	2.	Organization ID: Replace the ORG_ID variable with your Coveo organization ID.
	3.	Filter and Query: Customize the FILTER and QUERY variables to specify the data you want to fetch.
	4.	JSON File Name: Change the FILE_NAME variable if you want to save the results to a different file.
	5.	Fields to Include: Update the FIELDS_TO_INCLUDE and JSON_FIELDS_TO_INCLUDE variables if you want to include different fields in the API response and JSON file respectively.


