import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import os
import json
import datetime
from google.cloud import bigquery
import numpy as np


# Function to fetch and parse a webpage
def get_soup_from_page(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        return soup

    else:
        raise Exception(f"Failed to load page {url}")



def get_car_articles(soup):
    articles = soup.find_all('article')
    return articles


def get_car_summary(article):
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
        #https://www.autoscout24.com/offers/ferrari-f430-ceramic-brakes-carbon-seats-roll-cage-gasoline-red-8b969b68-ee77-4f38-9c1b-5b17d9468d05
        #<section class="DetailsSection_container__68Nlc DetailsSection_breakElement__BD_zV" data-cy="basic-details-section" id="basic-details-section"><div class="DetailsSection_detailsSection__FJZXR"><div class="DetailsSection_titleSection__nbi_I"><div class="DetailsSectionTitle_container__Sr9hK"><svg color="currentColor" viewBox="0 0 24 24"><use xlink:href="/assets/as24-search-funnel/icons/icons-sprite-8ad85fa2.svg#search_car"></use></svg><h2 class="DetailsSectionTitle_text__KAuxN">Basic Data</h2></div></div><div class="DetailsSection_childrenSection__aElbi"><dl class="DataGrid_defaultDlStyle__xlLi_"><dt class="DataGrid_defaultDtStyle__soJ6R">Body type</dt><dd class="DataGrid_defaultDdStyle__3IYpG DataGrid_fontBold__RqU01">Coupe</dd><dt class="DataGrid_defaultDtStyle__soJ6R">Type</dt><dd class="DataGrid_defaultDdStyle__3IYpG DataGrid_fontBold__RqU01">Used</dd><dt class="DataGrid_defaultDtStyle__soJ6R">Drivetrain</dt><dd class="DataGrid_defaultDdStyle__3IYpG DataGrid_fontBold__RqU01">Rear</dd><dt class="DataGrid_defaultDtStyle__soJ6R">Seats</dt><dd class="DataGrid_defaultDdStyle__3IYpG DataGrid_fontBold__RqU01">2</dd><dt class="DataGrid_defaultDtStyle__soJ6R">Doors</dt><dd class="DataGrid_defaultDdStyle__3IYpG DataGrid_fontBold__RqU01">2</dd><dt class="DataGrid_defaultDtStyle__soJ6R">Country version</dt><dd class="DataGrid_defaultDdStyle__3IYpG DataGrid_fontBold__RqU01">Belgium</dd><dt class="DataGrid_defaultDtStyle__soJ6R">Offer number</dt><dd class="DataGrid_defaultDdStyle__3IYpG DataGrid_fontBold__RqU01">OR89</dd><dt class="DataGrid_defaultDtStyle__soJ6R">Warranty</dt><dd class="DataGrid_defaultDdStyle__3IYpG DataGrid_fontBold__RqU01">12 months</dd></dl></div></div></section>

        # Parse the HTML content using BeautifulSoup

        # Find the section with Basic Data
        subset_section = soup.find('section', {'data-cy': section})

        # Extract key-value pairs from the dl, dt, and dd tags
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


def get_car_details(subpage_link):
    soup = get_soup_from_page(subpage_link)

    # Get Section Data
    section_data_combined = get_section_data(soup)

    # Get Additional Json data
    additional_json_data = get_additional_json_data(soup, subpage_link)

    # Combine data
    combined_data = section_data_combined
    combined_data.update(additional_json_data)

    return combined_data


def convert_to_result_schema(available_data):
    import json
    with open("mappings/result_columns.json", "r") as f:
        result_schema = json.loads(f.read())

    with open("mappings/mapping_columns.json", "r") as f:
        mapping = json.loads(f.read())

    # Map the available columns to the result columns
    for key, value in available_data.items():
        if key in mapping:
            result_schema[mapping[key]] = value

    return result_schema


def write_data_to_csv(data, file_path):
    print(f"Writing data to file {file_path}")
    df = pd.DataFrame(data)
    # Check if the file exists
    if os.path.isfile(file_path):
        # Append to the existing file without writing the header
        df.to_csv(file_path, mode='a', header=False, index=False)
    else:
        # Write to the file (including the header)
        df.to_csv(file_path, mode='w', header=True, index=False)


def write_data_to_json(data, base_path):
    for i, element in enumerate(data):
        file_path = base_path + f"_{i}" + ".json"
        print(f"Writing data to file {file_path}")
        with open(file_path, 'w') as file:
            json.dump(element, file, indent=4)


def get_additional_json_data(soup, link):
    # Check for the presence of dynamic content scripts
    script_tags = soup.find_all('script', type='application/json')

    data = None
    for script in script_tags:
        if script.get('id') == '__NEXT_DATA__':
            data = json.loads(script.string)
        else:
            print("COULD NOT FOUND Next.js __NEXT_DATA__ script tag")


    """
    dict_keys(['abTest517Props', 'pageTitle', 'pageid', 'cultureInfo', 'eTldPlusOne', 'pageQuery', 'numberOfResults', 'numberOfPages', 'listings', 'numberOfOcsResults', 'trackingId', 'isMobile', 'taxonomy', 'translations', 'adTargetingString', 'loggedInCustomerTypeId', 'recommendations', 'userSession', 'seoText', 'latestUserSession', 'togglingParams', 'topspotTrackingData', 'interlinking', 'optimizelyResults', 'isoCulture', 'pagePath', 'collectWebVitals', 'isTradeInCampaign', 'resumeFinancing', 'listHeaderTitle', 'deliverableTailTotalItems'])
    
    """

    #listing_details = additional_data["props"]["pageProps"]["listingDetails"]

    # Extract the car listing details
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

    # Extract the relevant keys
    car_info = {
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


"""
def add_options(data):
    #print("DATA FOR OPTIONS", data.keys())
    options = {
        "options": {}
    }
    for key in ["Comfort & Convenience", "Entertainment & Media", "Safety & Security", "Extras"]:
        if key in data:
            options["options"][key] = data[key]

    return options
"""


def clean_and_prepare_data(data):
    """Perform cleaning and preparation of the DataFrame."""
    # Example: Replace NaN with None (suitable for BigQuery)
    def convert_dict_to_json(d):
        return json.dumps(d) if isinstance(d, dict) else d

    # Iterate through the list and convert necessary fields
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
    """Upload a pandas DataFrame to a Google BigQuery table and return the job response."""
    client = bigquery.Client(project=project)
    job = client.load_table_from_dataframe(df, table_id)
    job.result()  # Wait for the job to complete

    # Optionally, return the job details
    response =  {
        "job_id": job.job_id,
        "status": job.state,
        "errors": job.errors,
        "output_rows": job.output_rows
    }
    print(response)


def scrape_and_save_all_pages(url_filter):
    # Replace this with the actual URL

    # url = "https://www.autoscout24.com/lst?atype=C&cy=D%2CA%2CB%2CE%2CF%2CI%2CL%2CNL&damaged_listing=exclude&desc=1&fregto=2005&powertype=kw&pricefrom=20000&search_id=gd6zvktyks&sort=age&source=detailsearch&ustate=N%2CU"

    articles_parsed = []

    page_max = 20
    for page in range(1, page_max):
        print("")
        print(f"*** Processing page: {page}/{page_max}")

        #url_filter = "https://www.autoscout24.com/lst?atype=C&cy=D%2CA%2CB%2CE%2CF%2CI%2CL%2CNL&damaged_listing=exclude&desc=1&fregto=2005&powertype=kw&pricefrom=20000&search_id=gd6zvktyks&sort=age&source=listpage_pagination&ustate=N%2CU"
        url = url_filter + f"&page={page}"
        soup = get_soup_from_page(url)
        articles = get_car_articles(soup)
        len_articles = len(articles)
        print(f"Processing this number of articles: {len_articles}")
        if len_articles < 1:
            break
        for i, article in enumerate(articles):
            # print("")
            # print(f"*** Processing article: {i + 1}/{len_articles}")
            data_car_summary = get_car_summary(article)
            data_car_details = get_car_details(data_car_summary["subpage_link"])
            additional_data = add_additional_data(data_car_details)

            #data_car_options = add_options(data_car_details)


            data_combined = {**data_car_summary, **data_car_details, **additional_data}
            articles_parsed.append(data_combined)

            """
            if test_mode and i >= 2:
                break
            """
        """
        if test_mode:
            break
        """


    articles_parsed_converted = []
    for article in articles_parsed:
        articles_parsed_converted.append(convert_to_result_schema(article))

    #write_data_to_csv(articles_parsed, "autoscout_parsed_2.csv")


    #print("RESULTING DATA" )
    #print(articles_parsed_converted[0])
    #print(articles_parsed_converted[0].keys())

    write_data_to_csv(articles_parsed_converted, "result/autoscout_data_2.csv")
    #write_data_to_json(articles_parsed_converted, "result/autoscout_data")

    # df = clean_and_prepare_data(articles_parsed_converted)
    # upload_to_bigquery(df, bigquery_project, bigquery_table)


def clean_and_prepare_df(df):
    """Perform cleaning and preparation of the DataFrame."""
    # Example: Replace NaN with None (suitable for BigQuery)

    # Iterate through the list and convert necessary fields

    df = df.astype(str)
    df = df.replace(np.nan, None)
    df = df.replace("None", None)
    df = df.replace("nan", None)
    return df.where(pd.notnull(df), None)


def main():

    body_types = [1, 2, 3, 4, 5, 6, 7]
    for body_type in body_types:
        print("body_type: ", body_type)
        from_price = 20000
        to_price = 400000
        step = 1000
        for price in range(from_price, to_price, step):
            print("price range: ", price, price+step-1)
            url = f"https://www.autoscout24.com/lst?atype=C&cy=D%2CA%2CB%2CE%2CF%2CI%2CL%2CNL&damaged_listing=exclude&desc=1&fregto=2005&powertype=kw&search_id=gd6zvktyks&sort=age&source=listpage_pagination&ustate=N%2CU&pricefrom={price}&priceto={price+step-1}&body={body_type}"
            scrape_and_save_all_pages(url)

    final_from_price = 400000
    url = f"https://www.autoscout24.com/lst?atype=C&cy=D%2CA%2CB%2CE%2CF%2CI%2CL%2CNL&damaged_listing=exclude&desc=1&fregto=2005&powertype=kw&search_id=gd6zvktyks&sort=age&source=listpage_pagination&ustate=N%2CU&pricefrom={final_from_price}"
    scrape_and_save_all_pages(url)

    df = pd.read_csv("result/autoscout_data_2.csv")
    # Print the full DataFrame
    df = clean_and_prepare_df(df)
    upload_to_bigquery(df, bigquery_project, bigquery_table)


# Main script
if __name__ == "__main__":

    test_mode = True
    if test_mode:
        bigquery_project = "python-rocket-1"
        bigquery_table = "assetclassics.autoscout_scrapper_sample_v1"

    else:
        bigquery_project = "ac-vehicle-data"
        bigquery_table = "autoscout24.autoscout_scrapper_sample_v1"

    base_url = "https://www.autoscout24.com"
    main()





