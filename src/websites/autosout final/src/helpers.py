import asyncio
import aiohttp
from bs4 import BeautifulSoup
import pandas as pd
import os
import datetime



def get_subpage_link(article, base_url):
    subpage_link = base_url + article.find('a', class_='ListItem_title__ndA4s').get('href')
    return subpage_link


def get_last_page_number(soup):

    # Find all the buttons in the pagination
    pagination_buttons = soup.select('li.pagination-item button')

    # Extract the numbers from the buttons
    page_numbers = [int(button.text) for button in pagination_buttons if button.text.isdigit()]

    # Find the maximum page number
    if page_numbers:
        last_page = max(page_numbers)
        return last_page
        print(f"The last page number is: {last_page}")
    else:
        print("No page numbers found.")
        return 1


def get_car_articles(soup):
    articles = soup.find_all('article')
    return articles



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


def add_additional_data():
    additional_data = {}
    additional_data["date_scraped"] = datetime.datetime.now().date().strftime('%Y-%m-%d')
    additional_data["source"] = "autoscout"
    return additional_data


def write_data_to_csv(data, file_path):
    print(f"Writing data to file {file_path}")
    df = pd.DataFrame(data)
    if os.path.isfile(file_path):
        df.to_csv(file_path, mode='a', header=False, index=False)
    else:
        df.to_csv(file_path, mode='w', header=True, index=False)

