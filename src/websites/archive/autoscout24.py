import requests
from bs4 import BeautifulSoup
import re
import pandas as pd

# Function to fetch and parse a webpage
def fetch_page(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.text
    else:
        raise Exception(f"Failed to load page {url}")

# Function to parse car details from a single car listing element
def parse_car_details(listing):
    car_details = {}

    # Extract the car name
    car_name_element = listing.find('a', href=re.compile('/offers/.*'))
    if car_name_element:
        h2_element = car_name_element.find('h2')
        if h2_element:
            car_details['name'] = h2_element.get_text(strip=True)

    # Extract the car price
    car_price_element = listing.find('p', {'data-testid': 'regular-price'})
    if car_price_element:
        car_details['price'] = car_price_element.get_text(strip=True)

    # Extract additional car details
    details_elements = listing.find_all('span', {'data-testid': re.compile('VehicleDetails-.*')})
    for detail in details_elements:
        key = detail['data-testid'].replace('VehicleDetails-', '')
        value = detail.get_text(strip=True)
        car_details[key] = value

    return car_details

# Main script
if __name__ == "__main__":
    # Replace this with the actual URL
    url = "https://www.autoscout24.com/lst?atype=C&cy=D%2CA%2CB%2CE%2CF%2CI%2CL%2CNL&damaged_listing=exclude&desc=1&fregto=2005&powertype=kw&pricefrom=20000&search_id=gd6zvktyks&sort=age&source=detailsearch&ustate=N%2CU"
    html_content = fetch_page(url)
    soup = BeautifulSoup(html_content, 'html.parser')

    # Find all car listings
    listings = soup.find_all('div', class_='ListItem_wrapper__TxHWu')

    # Parse details for each listing
    all_car_details = []
    for listing in listings:
        try:
            car_details = parse_car_details(listing)
        except:
            continue
        if car_details:  # Only add if details were found
            all_car_details.append(car_details)

    # Convert to DataFrame
    df = pd.DataFrame(all_car_details)
    print("RESULT")
    print(df)

    # Save to CSV
    df.to_csv('car_listings.csv', index=False)

    """
    # Print the parsed car details for each car
    for car in all_car_details:
        print(car)
    """
