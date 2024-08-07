import requests
from bs4 import BeautifulSoup
import json
# Fetch the HTML content
url = "https://www.autoscout24.com/offers/mercedes-benz-230-sl-pagode-3-besitz-dt-fahrzeug-h-zul-gasoline-red-d435cd80-8739-4679-89af-350bfe0200db?sort=age&desc=1&lastSeenGuidPresent=true&cldtidx=1&position=1&search_id=gd6zvktyks&source_otp=nfm&ap_tier=t50&source=listpage_search-results&order_bucket=0&topspot-algorithm=nfm-default&topspot-dealer-id=27198807"
response = requests.get(url)

# Parse the HTML content
soup = BeautifulSoup(response.content, 'html.parser')


def get_additional_json_data():
    # Check for the presence of dynamic content scripts
    script_tags = soup.find_all('script', type='application/json')

    data = None
    for script in script_tags:
        if script.get('id') == '__NEXT_DATA__':
            print("Found Next.js __NEXT_DATA__ script tag.")
            # You can further parse the JSON content if needed
            data = json.loads(script.string)


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





# Print the extracted information
for key, value in car_info.items():
    print(f"{key}: {value}")
    print("")


with open("testing_data_result_2.json", "w") as f:
    f.write(json.dumps(car_info))


"""
- id
- description >> save as ad_title
- prices.public.priceRaw
- prices.dealer.priceRaw
- location (all information)
- seller (id, type, companyName)
- vehicle (nearly all information - only rawData seems to be duplicates)
"""
