import asyncio
import aiohttp
import pandas as pd
import json

from src.helpers import get_soup_from_page
from src.helpers import get_car_articles
from src.helpers import get_subpage_link
from src.helpers import add_additional_data
from src.helpers import write_data_to_csv
from src.helpers import get_last_page_number


from src.extract_html import get_car_summary
from src.extract_html import get_section_data
from src.extract_json import get_additional_json_data

from src.bigquery import clean_and_prepare_df
from src.bigquery import upload_to_bigquery


class AutoScout():
    def __init__(self):
        self.article_counter = 0
        self.page_counter = 0

    async def get_car_details(self, subpage_link, session):
        soup = await get_soup_from_page(subpage_link, session)
        if soup is None:
            return None

        additional_json_data = get_additional_json_data(soup, subpage_link)
        section_data_combined = get_section_data(soup)

        return additional_json_data, section_data_combined


    async def combine_data(self, additional_json_data, section_data_combined, data_car_summary, additional_data):
        with open("mappings/result_columns.json", "r") as f:
            result_schema = json.loads(f.read())

        car_details = {}

        # Hierarchy of data sources
        for key, item in result_schema.items():
            if additional_json_data.get(key):
                car_details[key] = additional_json_data.get(key)
            elif data_car_summary.get(key):
                car_details[key] = data_car_summary.get(key)
            elif section_data_combined.get(key):
                car_details[key] = section_data_combined.get(key)


        # Add additional data
        car_details.update(additional_data)

        return car_details



    async def loop_through_all_pages(self, url_filter, session, base_url):
        articles_parsed = []
        page_max = 20
        tasks = []
        all_articles = []  # List to store articles from all pages


        # Reading first page to get number of pages
        try:
            page = 1
            url = url_filter + f"&page={page}"
            soup = await get_soup_from_page(url, session)
            page_max = get_last_page_number(soup)
            self.page_counter += 1

        except Exception as e:
            print("fail")
            print(e)

        # Looping through all pages
        for page in range(1, page_max):
            print(f"*** Processing page: {page}/{page_max}")

            try:
                url = url_filter + f"&page={page}"
                soup = await get_soup_from_page(url, session)
                if soup is None:
                    break

                # Getting all articles for this page
                articles = get_car_articles(soup)
                len_articles = len(articles)
                print(f"Processing this number of articles: {len_articles}")
                if len_articles < 1:
                    break
                all_articles.extend(articles)  # Collect all articles from all pages

            except Exception as e:
                print("skipping this page. Maybe no soup or other error. Going to next page.")
                print(e)
                continue

            # Creating Async Tasks: For each article getting car details
            if articles:
                for article in articles:
                    self.article_counter += 1
                    subpage_link = get_subpage_link(article, base_url)
                    task = asyncio.create_task(self.get_car_details(subpage_link, session, base_url))
                    tasks.append(task)

        # Eecuting asnync tasks
        additional_json_data, section_data_combined = await asyncio.gather(*tasks)


        # Combine json data, section data and summary data
        combined_data = zip(additional_json_data, section_data_combined, all_articles)
        for data_html, data_json, article in combined_data:
            try:
                data_car_summary = get_car_summary(article, base_url)
                additional_data = add_additional_data()
                data_combined = self.combine_data(additional_json_data, section_data_combined, data_car_summary, additional_data)
                articles_parsed.append(data_combined)

            except Exception as e:
                print("failed processing this article: Going to next article")
                print(e)
                continue

        return articles_parsed



    async def loop_through_all_filter_combinations(self):

        ## Loop through all body types
        body_types = [1, 2, 3, 4, 5, 6, 7]
        async with aiohttp.ClientSession() as session:
            for body_type in body_types:
                print("body_type: ", body_type)

                from_price = 20000
                to_price = 80000
                step = 500
                data = []
                ## Loop through all price ranges
                for price in range(from_price, to_price, step):
                    print("price range: ", price, price + step - 1)
                    url = f"https://www.autoscout24.com/lst?atype=C&cy=D%2CA%2CB%2CE%2CF%2CI%2CL%2CNL&damaged_listing=exclude&desc=1&fregto=2005&powertype=kw&search_id=gd6zvktyks&sort=age&source=listpage_pagination&ustate=N%2CU&pricefrom={price}&priceto={price + step - 1}&body={body_type}"

                    # Get all cars ant their data
                    data += await self.loop_through_all_pages(url, session, base_url)

                from_price = 80000
                to_price = 400000
                step = 5000
                for price in range(from_price, to_price, step):
                    print("price range: ", price, price + step - 1)
                    url = f"https://www.autoscout24.com/lst?atype=C&cy=D%2CA%2CB%2CE%2CF%2CI%2CL%2CNL&damaged_listing=exclude&desc=1&fregto=2005&powertype=kw&search_id=gd6zvktyks&sort=age&source=listpage_pagination&ustate=N%2CU&pricefrom={price}&priceto={price + step - 1}&body={body_type}"

                    data += await self.loop_through_all_pages(url, session, base_url)

                # Write data to csv
                write_data_to_csv(data, "result/autoscout_data_2.csv")

            # Final run after loop ended
            final_from_price = 400000
            url = f"https://www.autoscout24.com/lst?atype=C&cy=D%2CA%2CB%2CE%2CF%2CI%2CL%2CNL&damaged_listing=exclude&desc=1&fregto=2005&powertype=kw&search_id=gd6zvktyks&sort=age&source=listpage_pagination&ustate=N%2CU&pricefrom={final_from_price}"
            await self.loop_through_all_pages(url, session, base_url)

            # Read CSV and save result in Big Query
            df = pd.read_csv("result/autoscout_data_2.csv")
            df = clean_and_prepare_df(df)
            upload_to_bigquery(df, bigquery_project, bigquery_table)


    async def run(self):
        await self.loop_through_all_filter_combinations()
        print("Number of processed articles", self.article_counter)


# Main script
if __name__ == "__main__":
    autoscout = AutoScout()
    import time
    start_time = time.time()  # Start the time

    test_mode = True
    if test_mode:
        bigquery_project = "python-rocket-1"
        bigquery_table = "assetclassics.autoscout_scrapper_sample_v4"
    else:
        bigquery_project = "ac-vehicle-data"
        bigquery_table = "autoscout24.autoscout_scrapper_sample_v1"

    base_url = "https://www.autoscout24.com"
    asyncio.run(autoscout.run())


    end_time = time.time()  # End the timer
    execution_time = end_time - start_time  # Calculate the execution time
    print(f"Execution time: {execution_time:.2f} seconds")  # Print the execution time
