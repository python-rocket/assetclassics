import asyncio
import aiohttp
from bs4 import BeautifulSoup
import pandas as pd
import os
import datetime
import time
import traceback


class HelperFunctions:
    def __init__(self, logger):
       self.logger = logger
    
    def get_subpage_link(self, article, base_url):
        subpage_link = base_url + article.find('a', class_='ListItem_title__ndA4s').get('href')
        return subpage_link
    
    
    def get_last_page_number(self, soup):
    
        # Find all the buttons in the pagination
        pagination_buttons = soup.select('li.pagination-item button')
    
        # Extract the numbers from the buttons
        page_numbers = [int(button.text) for button in pagination_buttons if button.text.isdigit()]
    
        # Find the maximum page number
        if page_numbers:
            last_page = max(page_numbers)
            return last_page
            #print(f"The last page number is: {last_page}")
        else:
            #print("No page numbers found.")
            return 1
    
    
    def get_car_articles(self, soup):
        articles = soup.find_all('article')
        return articles
    
    
    
    async def get_soup_from_page(self, url, session, retries=3, timeout=10):
        failed_articles = []
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Referer": "https://www.autoscout24.com",
        }
    
        for attempt in range(retries):
            try:
                #print("USING THIS URL", url)
                async with session.get(url, headers=headers, timeout=timeout) as response:
                    if response.status == 200:
                        text = await response.text()
                        soup = BeautifulSoup(text, 'html.parser')
                        return soup, len(failed_articles)
                    else:
                        self.logger.error(f"Failed to load page {url}, status code: {response.status}")
                        self.logger.error(f"Error: {response.text}")
                        await asyncio.sleep(2)  # Wait before retrying
                        return None, len(failed_articles)
            except aiohttp.ClientError as e:
                self.logger.error(f"Client error on {url}: {e}")
                if attempt + 1 == retries:
                    self.logger.error(f"Max retries reached for {url}")
                    failed_articles.append(1)
                    return None, len(failed_articles)
                await asyncio.sleep(2)  # Wait before retrying
    
            except asyncio.TimeoutError:
                self.logger.error(f"Timeout error on {url}, attempt {attempt + 1}/{retries}")
                if attempt + 1 == retries:
                    self.logger.error(f"Max retries reached for {url}")
                    failed_articles.append(1)
                    return None, len(failed_articles)
                await asyncio.sleep(2)  # Wait before retrying
            except Exception as e:
                self.logger.error(f"Unexpected error on {url}: {e}")
                if attempt + 1 == retries:
                    self.logger.error(f"Max retries reached for {url}")
                    failed_articles.append(1)
                    return None, len(failed_articles)
                await asyncio.sleep(2)
    
        return None, len(failed_articles)
    
    
    def add_additional_data(self):
        additional_data = {}
        additional_data["date_scraped"] = datetime.datetime.now().date().strftime('%Y-%m-%d')
        additional_data["source"] = "autoscout"
        additional_data["mileage_unit"] = "KM"
        additional_data["currency"] = "Euro"

    
        return additional_data


    def write_data_to_csv(self, data, file_path):
        self.logger.info(f"Writing data to file {file_path}")
        df = pd.DataFrame(data)

        if os.path.isfile(file_path):
            df.to_csv(file_path, mode='a', header=False, index=False)
        else:
            df.to_csv(file_path, mode='w', header=True, index=False)

    def delete_csv_if_exists(self, file_path):
        if os.path.isfile(file_path):
            self.logger.info(f"File {file_path} already exists. Deleting it.")
            os.remove(file_path)



    def get_execution_time(self, start_time):
        end_time = time.time()  # End the timer
        execution_time = end_time - start_time  # Calculate the execution time
        # Convert to minutes and hours
        execution_time_minutes = execution_time / 60
        execution_time_hours = execution_time / 3600
    
        # Print the execution times
        #print(f"Execution time: {execution_time:.2f} seconds")
        self.logger.info(f"Execution time: {execution_time_minutes:.2f} minutes")
        #print(f"Execution time: {execution_time_hours:.2f} hours")



