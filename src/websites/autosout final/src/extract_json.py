
import json
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
        "model_orig": vehicle_details.get('model') or vehicle_details.get('modelVersionInput', None),
        "model_orig_details": vehicle_details.get('model', {}) or vehicle_details.get('modelVersionInput', {}),
        "price": listing_details.get('prices', {}).get('public', {}).get('priceRaw', None),

        # Adding the new fields
        "mileage_unit": "KM",  # As it is assumed to be in KM
        "production_year": vehicle_details.get('firstRegistrationDateRaw', None),
        "chassis_no": "",  # Not relevant/existing
        "engine_no": "",  # Not relevant/existing
        "body_no": "",  # Not relevant/existing
        "condition": "",  # Not relevant/existing
        "color_exterior": vehicle_details.get('bodyColor', None),
        "color_interior": vehicle_details.get('upholsteryColor', None),
        "country": location_details.get('countryCode', None),
        "region": "",  # Will be defined later based on country/country code
        "location_country_code": location_details.get('countryCode', None),
        "location_zip": location_details.get('zip', None),
        "location_city": location_details.get('city', None),
        "bodystyle": vehicle_details.get('bodyType', None),
        "engine": "",  # Not relevant/existing
        "transmission": vehicle_details.get('transmissionType', None),
        "drive_type": vehicle_details.get('driveTrain', None),
        "gears": vehicle_details.get('gears', None),
        "driver_side": "",  # Not relevant/existing
        "auction_house": "",  # Not relevant/existing
        "auction_event": "",  # Not relevant/existing
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
        "description": listing_details.get('description', "").replace('<br>', '').replace('<strong>', '').replace('</strong>', '').replace('<ul>', '').replace('</ul>', ''),  # Remove HTML tags
        "seller_id": seller_details.get('id', None),
        "seller_isDealer": seller_details.get('isDealer', None),
        "seller_link": seller_details.get('links', {}).get('infoPage', None),
        "seller_phone": seller_details.get('phones', [])[0].get('callTo', None) if seller_details.get('phones') else None,
        "source_makeID": vehicle_details.get('makeId', None),
        "source_modelID": vehicle_details.get('modelOrModelLineId', None),
        "vehicle_type": vehicle_details.get('type', None),
        "vehicle_hsnTsn":  vehicle_details.get('hsnTsn', None),
        "vehicle_originalMarket":  vehicle_details.get('originalMarket', None),
        "vehicle_hadAccident":  vehicle_details.get('hadAccident', None),
        "vehicle_hasFullServiceHistory":  vehicle_details.get('hasFullServiceHistory', None),
        "vehicle_noOfPreviousOwners":  vehicle_details.get('noOfPreviousOwners', None)

    }

    return car_info