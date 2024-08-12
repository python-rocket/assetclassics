import re
import json

def get_section_data(soup):
    def get_subset_data(soup, section):
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
        section_data = get_subset_data(soup, section)
        if section_data:
            if section == 'equipment-section':
                section_data_combined["options"] = section_data
            else:
                section_data_combined.update(section_data)


    with open("mappings/mapping_columns.json", "r") as f:
        mapping = json.loads(f.read())


    # Convert keys
    section_dict_converted = {}
    for key, value in section_data_combined.items():
        if key in mapping:
            section_dict_converted[mapping[key]] = value

    # Only keep keys which are in the result schema
    section_dict_final = {}
    with open("mappings/result_columns.json", "r") as f:
        result_schema = json.loads(f.read())

    for key, value in section_dict_converted.items():
        if key in result_schema:
            section_dict_final[key] = value


    return section_dict_final



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


