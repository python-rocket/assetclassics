import requests
from bs4 import BeautifulSoup
import re
import pandas as pd

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
        section_data_combined.update(section_data)


    return section_data_combined





# Main script
if __name__ == "__main__":
    # Replace this with the actual URL
    base_url = "https://www.autoscout24.com"
    page_start = 1
    page_max = 20
    # url = "https://www.autoscout24.com/lst?atype=C&cy=D%2CA%2CB%2CE%2CF%2CI%2CL%2CNL&damaged_listing=exclude&desc=1&fregto=2005&powertype=kw&pricefrom=20000&search_id=gd6zvktyks&sort=age&source=detailsearch&ustate=N%2CU"


    sample_count_page = 1
    sample_limit_page = 2
    
    sample_count_article = 1
    sample_limit_article = 3

    articles_parsed = []
    
    print("### Runnin in this mode")
    print("Number of Pages: " + str(sample_limit_page))
    print("Number of Articles: " + str(sample_limit_article))
    print("")
    
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

        sample_count_page += 1
        if sample_count_page >= sample_limit_page:
            break

    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    df = pd.DataFrame(articles_parsed)
    data = df.to_dict("records")[0]
    for key, value in data.items():
        print("{}: {}".format(key, value))
        print("")









"""

    result = {
        "date_scraped": None,
        "source": None,
        "source_id": None,
        "ad_url": None,
        "ad_title": article.find('h2').text.strip(),
        "record_id": None,
        "make_orig": article.get('data-make'),
        "model_orig": article.get('data-model'),
        "model_orig_details": None,
        "model_orig_variant": None,
        "price": article.get('data-price'),
        "currency": None,
        "mileage": article.get('data-mileage'),
        "mileage_unit": None,
        "production_year": None,
        "chassis_no": None,
        "engine_no": None,
        "body_no": None,
        "condition": None,
        "color_exterior": None,
        "color_interior": None,
        "notes": None,
        "country": None,
        "region": None,

        # kw details
        "bodystyle": None,
        "engine": None,
        "transmission": None,
        "drive_type": None,
        "gears": None,
        "driver_side": None,
        "auction_house": None,
        "auction_event": None,
        "fuel": None,
        "kw": power_match.group(1) if power_match else None,
        "hp": power_match.group(2) if power_match else None,
        "cylinders_volume": None,
        "cylinders_number": None,
        "seats": None,
        "doors": None,
        "weight": None,
        "options": None,

        "subpage_link": base_url + article.find('a', class_='ListItem_title__ndA4s').get('href')
    }
    return result
    

article_data = {
    'id': article.get('id'),
    'first_registration': article.get('data-first-registration'),
    'fuel_type_code': article.get('data-fuel-type'),
    'seller_type': article.get('data-seller-type'),
    'vehicle_type': article.get('data-vehicle-type'),
    'transmission': article.find('span', {'data-testid': 'VehicleDetails-transmission'}).text.strip(),
    'fuel_type': article.find('span', {'data-testid': 'VehicleDetails-gas_pump'}).text.strip(),
}
"""

