
import json
from bs4 import BeautifulSoup


def remove_html_tags(text):
    if text:
        soup = BeautifulSoup(text, "html.parser")
        return soup.get_text(separator=" ")
    else:
        return None

def get_additional_json_data(soup, link, logger):
    script_tags = soup.find_all('script', type='application/json')
    data = None
    for script in script_tags:
        if script.get('id') == '__NEXT_DATA__':
            data = json.loads(script.string)
        else:
            logger.warning("COULD NOT FOUND Next.js __NEXT_DATA__ script tag")

    if data is None:
        logger.warning("No json found")
        return None

    listing_details = data.get('props', {}).get('pageProps', {}).get('listingDetails', {})
    vehicle_details = listing_details.get('vehicle', {})
    vd_error = vehicle_details.pop("rawData", None)
    if not vd_error:
        logger.warning(f"No raw data error: {link}")
    tracking_params = listing_details.get('trackingParams', {})
    location_details = listing_details.get('location', {})
    seller_details = listing_details.get('seller', {})
    filtered_seller_details = {key: seller_details[key] for key in ["id", "type", "companyName"] if key in seller_details}
    model_orig_details = {key: vehicle_details[key] for key in ["make", "makeId", "model", "modelOrModelLineId"] if key in vehicle_details}

    description =  remove_html_tags(listing_details.get('description'))
    """
    if description is not None:
        description = description.replace('<br>', '').replace("<br />", "").replace('<strong>', '').replace('</strong>', '').replace('<ul>', '').replace('</ul>', '')
    else:
        description = ""
    """

    ad_img = None
    images = data.get('props', {}).get('pageProps', {}).get("listingDetails", {}).get('images', [])
    if images:
        ad_img = images[0]


    car_info = {
        "record_id": listing_details.get('id', None),
        "ad_img": ad_img,
        "make_orig": vehicle_details.get('make', None),
        "model_orig": vehicle_details.get('model') or vehicle_details.get('modelVersionInput', None),
        "model_orig_details": vehicle_details.get('modelVersionInput', {}),
        "price": listing_details.get('prices', {}).get('public', {}).get('priceRaw', None),

        # Adding the new fields
        "mileage": vehicle_details.get('mileageInKmRaw', None),
        "production_year": vehicle_details.get('firstRegistrationDateRaw', None),
        "color_exterior": vehicle_details.get('bodyColor', None),
        "color_interior": vehicle_details.get('upholsteryColor', None),
        "country": None, #"country": location_details.get('countryCode', None),
        "region": "",  # Will be defined later based on country/country code
        "location_country_code": location_details.get('countryCode', None),
        "location_zip": location_details.get('zip', None),
        "location_city": location_details.get('city', None),
        "bodystyle": vehicle_details.get('bodyType', None),
        "transmission": vehicle_details.get('transmissionType', None),
        "drive_type": vehicle_details.get('driveTrain', None),
        "gears": vehicle_details.get('gears', None),
        "kw": vehicle_details.get('rawPowerInKw', None),
        "hp": vehicle_details.get('rawPowerInHp', None),
        "cylinders_volume": vehicle_details.get('rawDisplacementInCCM', None),
        "cylinders_number": vehicle_details.get('cylinders', None),
        "seats": vehicle_details.get('numberOfSeats', None),
        "doors": vehicle_details.get('numberOfDoors', None),
        "bodyColorOriginal": vehicle_details.get('bodyColorOriginal', None),
        "upholstery": vehicle_details.get('upholstery', None),
        "weight": vehicle_details.get('weight', None),
        "options": listing_details.get('vehicle', {}).get('equipment', {}),  # As JSON, to be stored as JSON in BQ
        "description": description,
        "seller_id": seller_details.get('id', None),
        "seller_isDealer": seller_details.get('isDealer', None),
        "seller_link": seller_details.get('links', {}).get('infoPage', None),
        "seller_phone": seller_details.get('phones', [])[0].get('callTo', None) if seller_details.get('phones') else None,
        "source_makeID": vehicle_details.get('makeId', None),
        "source_modelID": vehicle_details.get('modelOrModelLineId', None),
        "vehicle_type": vehicle_details.get('type', None),
        "vehicle_hsnTsn":  vehicle_details.get('hsnTsn', None),
        "vehicle_originalMarket":  vehicle_details.get('originalMarket', None),
        "vehicle_hadAccident":  str(vehicle_details.get('hadAccident', None)),
        "vehicle_hasFullServiceHistory":  str(vehicle_details.get('hasFullServiceHistory', None)),
        "vehicle_noOfPreviousOwners":  vehicle_details.get('noOfPreviousOwners', None),
        "seller_type": seller_details.get('type', None),
        "seller_companyName": seller_details.get('companyName', None)

    }

    return car_info