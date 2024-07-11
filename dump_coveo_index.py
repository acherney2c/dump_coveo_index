import requests
import json
import logging
import os

# Replace with your search token and endpoint
SEARCH_TOKEN = ''
ORG_ID = ''
# append &viewAllContent=1 if you need dictionary fields
ENDPOINT = f'https://{ORG_ID}.org.coveo.com/rest/search/v2?organizationId={ORG_ID}&viewAllContent=1'

# List of fields to include in the response, including required dictionary fields
FIELDS_TO_INCLUDE = ['rowid', 'title', 'source', 'objecttype', 'dict_field1', 'dict_field2', 'dict_field3']
# List of columns to include in the JSON file, dictionary fields will be output inside rawKeyValueEntries
JSON_FIELDS_TO_INCLUDE = ['title', 'source', 'objecttype']
# Replace with your filter and query (ex: @source==MySource)
FILTER = '@source==MySource'
QUERY = ''
# Number of results to fetch per request
BATCH_SIZE = 500
# Name of the JSON file
FILE_NAME = 'dump.json'
# Name of the file to store the last successful rowid
ROWID_FILE = 'last_rowid.txt'

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', handlers=[logging.StreamHandler()])
formatter = logging.Formatter('%(asctime)s - %(message)s')
for handler in logging.getLogger().handlers:
    handler.setFormatter(formatter)

def fetch_source(row_id=None):
    iteration = 1
    total_documents_saved = 0
    headers = {
        'Authorization': f'Bearer {SEARCH_TOKEN}',
        'Content-Type': 'application/json',
    }

    # order by rowid
    
    while True:        
        query = QUERY
        if row_id:
            query += f' @rowid>{row_id}'
        payload = {
            'q': query,
            'numberOfResults': BATCH_SIZE,
            'aq': FILTER,
            'fieldsToInclude': FIELDS_TO_INCLUDE,
            'sortCriteria': '@rowid ascending',
            'pipeline': '',                        
            'debug': 'true' # add 'debug': 'true' if you need dictionary fields
        }

        try:
            response = requests.post(ENDPOINT, json=payload, headers=headers)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            if response.status_code == 403:
                logging.error("Access forbidden. Saving last successful rowid and exiting.")
                save_last_rowid(row_id)
                return total_documents_saved
            else:
                logging.error(f"Request failed: {e}")
                return total_documents_saved
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed: {e}")
            return total_documents_saved

        data = response.json()
        results = data.get('results', [])
        
        if not results:
            break
        
        save_documents_to_json(results, FILE_NAME, append=True)
        total_documents_saved += len(results)

        if len(results) < BATCH_SIZE:
            logging.info(f"Iteration {iteration} - Documents saved to JSON: {total_documents_saved:,} - Last rowid: {row_id}")
            break
        else:
            row_id = results[-1]['raw']['rowid']
            save_last_rowid(row_id)
            logging.info(f"Iteration {iteration} - Documents saved to JSON: {total_documents_saved:,} - Last rowid: {row_id}")
            iteration += 1

    return total_documents_saved

def save_documents_to_json(documents, json_file, append=False):
    mode = 'a' if append and os.path.exists(json_file) else 'w'
    try:
        with open(json_file, mode, encoding='utf-8') as jsonfile:
            if mode == 'a':
                # Remove the closing bracket from the JSON array
                jsonfile.seek(jsonfile.tell() - 1)
                jsonfile.truncate()
                jsonfile.write(',\n')
            else:
                jsonfile.write('[\n')
            
            for i, document in enumerate(documents):
                doc = {field: document['raw'].get(field, 'N/A') for field in JSON_FIELDS_TO_INCLUDE}
                # Add fields from rawKeyValueEntries
                rawKeyValueEntries = document.get('rawKeyValueEntries', {})
                for key, value in rawKeyValueEntries.items():
                    doc[key] = value
                json.dump(doc, jsonfile, ensure_ascii=False, indent=4)
                if i < len(documents) - 1 or append:
                    jsonfile.write(',\n')
            
            if not append:
                jsonfile.write(']\n')
            else:
                jsonfile.write(']')

        logging.info("Batch of documents has been saved to JSON.")
    except IOError as e:
        logging.error(f"Failed to write to JSON file: {e}")

def save_last_rowid(rowid):
    try:
        with open(ROWID_FILE, 'w', encoding='utf-8') as f:
            f.write(str(rowid))
        logging.info(f"Last successful rowid {rowid} has been saved.")
    except IOError as e:
        logging.error(f"Failed to write last rowid to file: {e}")

def load_last_rowid():
    try:
        if os.path.exists(ROWID_FILE):
            with open(ROWID_FILE, 'r', encoding='utf-8') as f:
                return f.read().strip()
    except IOError as e:
        logging.error(f"Failed to read last rowid from file: {e}")
    return None

def main():
    last_rowid = load_last_rowid()
    total_documents_saved = fetch_source(last_rowid)
    logging.info(f"Total number of documents saved: {total_documents_saved:,}")

if __name__ == "__main__":
    main()
