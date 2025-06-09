import pandas as pd
import requests
import logging
import time
from bs4 import BeautifulSoup
from typing import Optional


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def import_file(csv_filename: str, random_sample: bool = False, sample_size: int = 100) -> pd.DataFrame:
    """
    Imports a CSV file into a pandas DataFrame.

    Args:
        csv_filename (str): The name of the CSV file to import.
        random_sample (bool): Whether to return a random sample of the data. Defaults to False.
        sample_size (int): The size of the random sample to return. Defaults to 100.

    Returns:
        pd.DataFrame: The DataFrame containing the data from the CSV file. Returns an empty DataFrame if the file is not found.
    """
    try:
        df = pd.read_csv(csv_filename)
        if not random_sample:
            logging.info(f"Successfully loaded data from {csv_filename}")
            return df
        else:
            df_sample = df.sample(n=sample_size, random_state=42).reset_index(drop=True)
            logging.info(f"Successfully loaded data from {csv_filename}. Random sample of {sample_size} selected.")
            return df_sample
    except FileNotFoundError:
        logging.warning(f"'{csv_filename}' not found.")
        return pd.DataFrame()


def print_to_file(df, output_csv_filename) -> None:
    """
    Prints a pandas DataFrame to a CSV file.

    Args:
        df (pd.DataFrame): The DataFrame to print.
        output_csv_filename (str): The name of the CSV file to print to.
    """
    try:
        df.to_csv(output_csv_filename, index=False)
        logging.info(f"Updated data saved to {output_csv_filename}")
    except Exception as e:
        logging.error(f"Error saving to {output_csv_filename}: {e}")


def extract_value_from_url(url_to_scrape: str, max_retries: int = 1, retry_delay: int = 5) -> Optional[str]:
    """
    Extracts the requested value from a given URL. Includes a retry mechanism for HTTP 403 errors.

    Parameters:
        url_to_scrape (str): The URL to scrape.
        max_retries (int): The maximum number of retries.
        retry_delay (int): The delay in seconds between retries.

    Returns:
        Optional[str]: The string which gets reversed. Returns None if no data is extracted.
    """

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.93 Safari/537.36',
        'Accept-Language': 'it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7'
    }

    extracted_text = None
    attempt = 0

    while attempt <= max_retries:  # Attempt until the max_retries is met
        logging.info(f"Processing URL: {url_to_scrape} (Attempt: {attempt + 1} of {max_retries + 1}")
        try:
            response = requests.get(url_to_scrape, headers=headers, timeout=15)
            response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
            soup = BeautifulSoup(response.text, 'html.parser')

            # Attempt to find the primary element
            result_counter = soup.find('span', id='sidebar-title') # find the first page element that matches the criteria
            if result_counter:
                extracted_text = result_counter.get_text(strip=True)
            else:
                result_counter_fallback = soup.find('span', attrs={'qaselector': 'sidebar-result-counter'})
                if result_counter_fallback:
                    extracted_text = result_counter_fallback.get_text(strip=True)

            if extracted_text:
                logging.info(f"Successfully extracted: {extracted_text}")
                return extracted_text
            else:
                logging.warning(f"Could not extract data for {url_to_scrape} (element not found).")
                return None

        except requests.exceptions.HTTPError as http_err:
            status_code = http_err.response.status_code if hasattr(http_err, 'response') and http_err.response is not None else 'N/A'
            logging.error(f"HTTP Error for {url_to_scrape}: {http_err} - Status: {status_code}")

            if status_code == 403 and attempt < max_retries:
                logging.info(f"Received 403 Forbidden. Waiting {retry_delay} seconds before retrying (Attempt {attempt + 1} failed)...")
                time.sleep(retry_delay)
                attempt += 1
                continue # Retry
            else:
                # If not a 403, or if it's a 403 but no more retries left
                return f"HTTP Error: {status_code} (Attempt {attempt + 1})"

        except requests.exceptions.Timeout:
            logging.error(f"Request Timeout for {url_to_scrape}.")
            if attempt < max_retries:
                logging.info(f"Waiting {retry_delay} seconds before retrying due to timeout...")
                time.sleep(retry_delay)
                attempt += 1
                continue # Retry
            else:
                return "Request Timeout"

        except requests.exceptions.RequestException as req_err:
            logging.error(f"Request Exception for {url_to_scrape}: {req_err}")
            return f"Request Exception: {str(req_err)}"

        except Exception as e:
            logging.exception(f"An unexpected error occurred for {url_to_scrape}: {e}")
            return f"Unexpected Error: {str(e)}"

    # If loop finishes without returning (e.g. all retries failed for 403)
    return f"Failed after {max_retries + 1} attempts (last error was likely 403 or Timeout)"



def process_urls(csv_filename: str, output_csv_filename: str, random_sample: bool = False, sample_size: int = 100) -> None:
    """
    Processes the supplied URLs from a CSV file, extracts the searched values, and saves the results to a new CSV file.

    Args:
        csv_filename (str): The name of the input CSV file.
        output_csv_filename (str): The name of the output CSV file.
        random_sample (bool): Whether to use a random sample of the data. Defaults to False.
        sample_size (int): The size of the random sample to use. Defaults to 100.
    """
    results = []
    df = import_file(csv_filename, random_sample, sample_size)
    if df.empty:
        logging.error("DataFrame is empty, exiting process_urls.")
        return

    df['results'] = None

    total_requests_to_make = df['url'].count()
    requests_made_count = 0

    for index, row in df.iterrows(): #  the method generates an iterator object of the df, to iterate each row. Each iteration produces an index object and a row object (a Series object).
        url = str(row['url']) if pd.notna(row['url']) else ""
        logging.info(f"Processing row {index + 1} of {len(df)}:")

        # Process the url
        if url.strip():
            result_value = extract_value_from_url(url, max_retries=2, retry_delay=5)
            df.at[index, 'results'] = result_value
            requests_made_count += 1
            if requests_made_count < total_requests_to_make:
                time.sleep(2)
        else:
            logging.info(f"Skipped empty or invalid url for row {index + 1}.")


    logging.info("\n--- Updated DataFrame ---")
    logging.info(df)

    print_to_file(df, output_csv_filename)


def main(payload: dict) -> None:
    """
    Main function to execute the data processing workflow.

    Args:
        payload (dict): A dictionary containing the configuration parameters.
    """
    try:
        process_urls(
            payload['csv_filename'],
            payload['output_csv_filename'],
            payload['random_sample'],
            payload['sample_size']
        )
    except KeyError as e:
        logging.error(f"Missing key in payload: {e}")
    except Exception as e:
        logging.exception(f"An unexpected error occurred in main: {e}")



if __name__ == '__main__':
    Payload = {
        'csv_filename': 'test.csv',
        'output_csv_filename': 'test_results.csv',
        'random_sample': False,
        'sample_size': 100
    }
    main(Payload)
