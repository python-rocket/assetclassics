import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import os
import json

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



def get_car_articles():
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


def get_car_details(subpage_link):
    soup = get_soup_from_page(subpage_link)

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
            section_data_combined.update(section_data)


    additional_json_data = get_additional_json_data(soup)
    combined_data = section_data_combined
    combined_data.update(additional_json_data)

    return combined_data


def convert_to_result_schema(available_data):
    import json
    with open("result_columns.json", "r") as f:
        result_schema = json.loads(f.read())

    with open("mapping_columns.json", "r") as f:
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


def get_additional_json_data(soup):
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
    vehicle_details.pop("rawData")
    tracking_params = listing_details.get('trackingParams', {})
    location_details = listing_details.get('location', {})
    seller_details = listing_details.get('seller', {})
    filtered_seller_details = {key: seller_details[key] for key in ["id", "type", "companyName"] if key in seller_details}


    # Extract the relevant keys
    car_info = {
        'id': listing_details.get('id'),
        'description': listing_details.get('description'),
        'price_public': listing_details.get('prices', {}).get('public', {}).get('priceRaw'),
        'price_dealer': listing_details.get('prices', {}).get('dealer', {}).get('priceRaw'),
        'location': location_details,
        'seller': seller_details,
        'vehicle': vehicle_details,
    }

    return car_info


# Main script
if __name__ == "__main__":
    # Replace this with the actual URL
    base_url = "https://www.autoscout24.com"
    page_start = 1
    page_max = 2
    # url = "https://www.autoscout24.com/lst?atype=C&cy=D%2CA%2CB%2CE%2CF%2CI%2CL%2CNL&damaged_listing=exclude&desc=1&fregto=2005&powertype=kw&pricefrom=20000&search_id=gd6zvktyks&sort=age&source=detailsearch&ustate=N%2CU"


    sample_count_article = 1
    sample_limit_article = 2

    articles_parsed = []

    
    for page in range(page_start, page_max):
        print("")
        print(f"*** Processing page: {page}/{page_max}")

        url = f"https://www.autoscout24.com/lst?atype=C&cy=D%2CA%2CB%2CE%2CF%2CI%2CL%2CNL&damaged_listing=exclude&desc=1&fregto=2005&powertype=kw&pricefrom=20000&search_id=gd6zvktyks&sort=age&source=listpage_pagination&ustate=N%2CU&page={page}"
        soup = get_soup_from_page(url)
        articles = get_car_articles()
        len_articles = len(articles)
        print(f"Processing this number of articles: {len_articles}")
        
        for i, article in enumerate(articles):
            print("")
            print(f"*** Processing article: {i + 1}/{len_articles}")
            data_car_summary = get_car_summary(article)
            data_car_details = get_car_details(data_car_summary["subpage_link"])
    
            data_combined = {**data_car_summary, **data_car_details}
            articles_parsed.append(data_combined)

            sample_count_article += 1
            if sample_count_article >= sample_limit_article:
                break


    articles_parsed_converted = []
    for article in articles_parsed:
        articles_parsed_converted.append(convert_to_result_schema(article))

    #write_data_to_csv(articles_parsed, "autoscout_parsed_2.csv")


    print("RESULTING DATA BEFORE TRANSFOMRRRNATION" )
    print(articles_parsed[0].keys())
    print("RESULTING DATA AFTER TRANSFOMRRRNATION" )
    print(articles_parsed_converted[0].keys())
    print(articles_parsed_converted[0])

    write_data_to_csv(articles_parsed_converted, "autoscout_results_2.csv")



