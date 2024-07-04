import requests
import csv
import logging

# Replace with your Coveo API key and endpoint
API_KEY = ''
ORG_ID = ''
ENDPOINT = f'https://platform.cloud.coveo.com/rest/search?organizationId={ORG_ID}'
# List of fields to include in the response
FIELDS_TO_INCLUDE = ['rowid', 'title', 'source', 'objecttype']
# List of columns to include in the CSV file
CSV_COLUMNS_TO_INCLUDE = ['title' , 'source', 'objecttype']
# Replace with your filter and query (ex: @source==MySource)
FILTER = '@source==MySource'
QUERY = ''
# Number of results to fetch per request
BATCH_SIZE = 500
# Name of the CSV file
FILE_NAME = 'dump.csv'

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_source(row_id=None):
    iteration = 1
    headers = {
        'Authorization': f'Bearer {API_KEY}',
        'Content-Type': 'application/json',
    }

    all_products = []
    total_count = None

    while True:
        logging.info(f"Iteration {iteration}")
        query = QUERY
        if row_id:
            query += f' @rowid>{row_id}'
        payload = {
            'q': query,
            'numberOfResults': BATCH_SIZE,
            'aq': FILTER,
            'fieldsToInclude': FIELDS_TO_INCLUDE,
            'sortCriteria': '@rowid ascending',
            'pipeline': ''
        }

        try:
            response = requests.post(ENDPOINT, json=payload, headers=headers)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed: {e}")
            return []

        data = response.json()
        results = data.get('results', [])
        all_products.extend(results)

        if total_count is None:
            total_count = data.get('totalCount', 0)

        if len(results) < BATCH_SIZE:
            break
        else:
            row_id = results[-1]['raw']['rowid']
            iteration += 1

    return all_products

def save_documents_to_csv(documents, csv_file):
    if not documents:
        logging.warning("No documents to save.")
        return

    try:
        with open(csv_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=CSV_COLUMNS_TO_INCLUDE, delimiter=';')
            writer.writeheader()

            for document in documents:
                row = {field: document['raw'].get(field, 'N/A') for field in CSV_COLUMNS_TO_INCLUDE}
                writer.writerow(row)

        logging.info(f"Documents have been saved to {csv_file}")
    except IOError as e:
        logging.error(f"Failed to write to CSV file: {e}")

def main():
    documents = fetch_source()
    save_documents_to_csv(documents, FILE_NAME)
    logging.info(f"Total number of records: {len(documents)}")

if __name__ == "__main__":
    main()