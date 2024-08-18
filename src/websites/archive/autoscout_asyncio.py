import asyncio
import aiohttp
from bs4 import BeautifulSoup
import re
import pandas as pd
import os
import json
import datetime
from google.cloud import bigquery
import numpy as np

# Function to fetch and parse a webpage asynchronously
async def get_soup_from_page(url, session, retries=3, timeout=10):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    for attempt in range(retries):
        try:
            async with session.get(url, headers=headers, timeout=timeout) as response:
                if response.status == 200:
                    text = await response.text()
                    soup = BeautifulSoup(text, 'html.parser')
                    return soup
                else:
                    print(f"Failed to load page {url}, status code: {response.status}")
                    return None
        except aiohttp.ClientError as e:
            print(f"Client error on {url}: {e}")
            return None
        except asyncio.TimeoutError:
            print(f"Timeout error on {url}, attempt {attempt + 1}/{retries}")
            if attempt + 1 == retries:
                print(f"Max retries reached for {url}")
                return None
            await asyncio.sleep(2)  # Wait before retrying
        except Exception as e:
            print(f"Unexpected error on {url}: {e}")
            return None

    return None

def get_car_articles(soup):
    articles = soup.find_all('article')
    return articles

def get_car_summary(article, base_url):
    power_text = article.find('span', {'data-testid': 'VehicleDetails-speedometer'}).text.strip()
    power_match = re.search(r"(\d+)\s*kW\s*\((\d+)\s*hp\)", power_text)

    result = {
        "ad_title": article.find('h2').text.strip(),
        "make_orig": article.get('data-make'),
        "model_orig": article.get('data-model'),
        "price": article.get('data-price'),
        "mileage": article.get('data-mileage'),
        "kw": power_match.group(1) if power_match else None,
        "hp": power_match.group(2) if power_match else None,
        "subpage_link": base_url + article.find('a', class_='ListItem_title__ndA4s').get('href')
    }
    return result

def get_section_data(soup):
    def get_car_details_subset(soup, section):
        subset_section = soup.find('section', {'data-cy': section})
        if subset_section:
            subset_data = {}
            for dt, dd in zip(subset_section.find_all('dt'), subset_section.find_all('dd')):
                key = dt.get_text(strip=True)
                value = dd.get_text(strip=True)
                subset_data[key] = value
            return subset_data

    section_data_combined = {}
    sections = ['basic-details-section', 'listing-history-section', 'technical-details-section', 'equipment-section', 'color-section', 'seller-notes-section']
    for section in sections:
        section_data = get_car_details_subset(soup, section)
        if section_data:
            if section == 'equipment-section':
                section_data_combined["options"] = section_data
            else:
                section_data_combined.update(section_data)

    return section_data_combined

async def get_car_details(subpage_link, session, base_url):
    soup = await get_soup_from_page(subpage_link, session)
    if soup is None:
        return None

    section_data_combined = get_section_data(soup)
    additional_json_data = get_additional_json_data(soup, subpage_link)
    combined_data = section_data_combined
    combined_data.update(additional_json_data)

    return combined_data

def convert_to_result_schema(available_data):
    with open("mappings/result_columns.json", "r") as f:
        result_schema = json.loads(f.read())

    with open("mappings/mapping_columns.json", "r") as f:
        mapping = json.loads(f.read())

    for key, value in available_data.items():
        if key in mapping:
            result_schema[mapping[key]] = value

    return result_schema

def write_data_to_csv(data, file_path):
    print(f"Writing data to file {file_path}")
    df = pd.DataFrame(data)
    if os.path.isfile(file_path):
        df.to_csv(file_path, mode='a', header=False, index=False)
    else:
        df.to_csv(file_path, mode='w', header=True, index=False)

def get_additional_json_data(soup, link):
    script_tags = soup.find_all('script', type='application/json')
    data = None
    for script in script_tags:
        if script.get('id') == '__NEXT_DATA__':
            data = json.loads(script.string)
        else:
            print("COULD NOT FOUND Next.js __NEXT_DATA__ script tag")

    listing_details = data.get('props', {}).get('pageProps', {}).get('listingDetails', {})
    vehicle_details = listing_details.get('vehicle', {})
    vd_error = vehicle_details.pop("rawData", None)
    if not vd_error:
        print(f"No raw data error: {link}")
    tracking_params = listing_details.get('trackingParams', {})
    location_details = listing_details.get('location', {})
    seller_details = listing_details.get('seller', {})
    filtered_seller_details = {key: seller_details[key] for key in ["id", "type", "companyName"] if key in seller_details}
    model_orig_details = {key: vehicle_details[key] for key in ["make", "makeId", "model", "modelOrModelLineId"] if key in vehicle_details}

    car_info = {
        "ad_img": data.get('props', {}).get('pageProps', {}).get('images', [None])[0],
        "make_orig": vehicle_details.get('make', None),
        'id': listing_details.get('id'),
        'description': listing_details.get('description'),
        'price_public': listing_details.get('prices', {}).get('public', {}).get('priceRaw'),
        'price_dealer': listing_details.get('prices', {}).get('dealer', {}).get('priceRaw'),
        'location': location_details,
        'seller': seller_details,
        'vehicle': vehicle_details,
        'model_orig_details': model_orig_details
    }

    return car_info

def add_additional_data(data_car_details):
    additional_data = {}
    additional_data["date_scraped"] = datetime.datetime.now().date().strftime('%Y-%m-%d')
    additional_data["source"] = "autoscout"
    additional_data["record_id"] = data_car_details["id"]
    return additional_data

def clean_and_prepare_data(data):
    def convert_dict_to_json(d):
        return json.dumps(d) if isinstance(d, dict) else d

    for item in data:
        for key, value in item.items():
            if isinstance(value, dict):
                item[key] = convert_dict_to_json(value)

    df = pd.DataFrame(data)

    df = df.astype(str)
    df = df.replace(np.nan, None)
    df = df.replace("None", None)
    df = df.replace("nan", None)
    return df.where(pd.notnull(df), None)

def upload_to_bigquery(df, project, table_id):
    client = bigquery.Client(project=project)
    job = client.load_table_from_dataframe(df, table_id)
    job.result()

    response = {
        "job_id": job.job_id,
        "status": job.state,
        "errors": job.errors,
        "output_rows": job.output_rows
    }
    print(response)

async def scrape_and_save_all_pages(url_filter, session, base_url):
    articles_parsed = []
    page_max = 20
    tasks = []

    for page in range(1, page_max):
        print(f"*** Processing page: {page}/{page_max}")

        try:
            url = url_filter + f"&page={page}"
            soup = await get_soup_from_page(url, session)
            if soup is None:
                break
            articles = get_car_articles(soup)
            len_articles = len(articles)
            print(f"Processing this number of articles: {len_articles}")
            if len_articles < 1:
                break

        except Exception as e:
            print("skipping this page. Maybe no soup or other error. Going to next page.")
            print(e)
            continue

        if articles:
            for article in articles:
                data_car_summary = get_car_summary(article, base_url)
                task = asyncio.create_task(get_car_details(data_car_summary["subpage_link"], session, base_url))
                tasks.append(task)

    details = await asyncio.gather(*tasks)

    for detail, article in zip(details, articles):
        try:
            if detail:
                data_car_summary = get_car_summary(article, base_url)
                additional_data = add_additional_data(detail)
                data_combined = {**data_car_summary, **detail, **additional_data}
                articles_parsed.append(data_combined)
        except Exception as e:
            print("failed processing this article: Going to next article")
            print(e)
            continue

    articles_parsed_converted = []
    for article in articles_parsed:
        articles_parsed_converted.append(convert_to_result_schema(article))

    return articles_parsed_converted


def clean_and_prepare_df(df):
    """Perform cleaning and preparation of the DataFrame."""
    # Example: Replace NaN with None (suitable for BigQuery)

    # Iterate through the list and convert necessary fields

    df = df.astype(str)
    df = df.replace(np.nan, None)
    df = df.replace("None", None)
    df = df.replace("nan", None)
    return df.where(pd.notnull(df), None)


async def main():
    body_types = [1, 2, 3, 4, 5, 6, 7]
    async with aiohttp.ClientSession() as session:
        for body_type in body_types:
            print("body_type: ", body_type)
            from_price = 20000
            to_price = 400000
            step = 500

            data = []
            for price in range(from_price, to_price, step):
                print("price range: ", price, price + step - 1)
                url = f"https://www.autoscout24.com/lst?atype=C&cy=D%2CA%2CB%2CE%2CF%2CI%2CL%2CNL&damaged_listing=exclude&desc=1&fregto=2005&powertype=kw&search_id=gd6zvktyks&sort=age&source=listpage_pagination&ustate=N%2CU&pricefrom={price}&priceto={price + step - 1}&body={body_type}"
                data += await scrape_and_save_all_pages(url, session, base_url)

            write_data_to_csv(data, "result/autoscout_data_2.csv")

        final_from_price = 400000
        url = f"https://www.autoscout24.com/lst?atype=C&cy=D%2CA%2CB%2CE%2CF%2CI%2CL%2CNL&damaged_listing=exclude&desc=1&fregto=2005&powertype=kw&search_id=gd6zvktyks&sort=age&source=listpage_pagination&ustate=N%2CU&pricefrom={final_from_price}"
        await scrape_and_save_all_pages(url, session, base_url)

        df = pd.read_csv("result/autoscout_data_2.csv")
        df = clean_and_prepare_df(df)
        upload_to_bigquery(df, bigquery_project, bigquery_table)


# Main script
if __name__ == "__main__":
    import time
    start_time = time.time()  # Start the time

    test_mode = True
    if test_mode:
        bigquery_project = "python-rocket-1"
        bigquery_table = "assetclassics.autoscout_scrapper_sample_v5"
    else:
        bigquery_project = "ac-vehicle-data"
        bigquery_table = "autoscout24.autoscout_scrapper_sample_v1"

    base_url = "https://www.autoscout24.com"
    asyncio.run(main())


    end_time = time.time()  # End the timer
    execution_time = end_time - start_time  # Calculate the execution time
    print(f"Execution time: {execution_time:.2f} seconds")  # Print the execution time