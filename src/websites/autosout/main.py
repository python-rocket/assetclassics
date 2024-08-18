import asyncio
import aiohttp
import pandas as pd
import json
import argparse

from src.helpers import HelperFunctions

from src.extract_html import get_car_summary
from src.extract_html import get_section_data
from src.extract_json import get_additional_json_data

from src.bigquery import clean_and_prepare_df
from src.bigquery import upload_to_bigquery
from src.bigquery import get_existing_record_ids

import traceback

import logging
import ast




"""
# Log some messages
logger.debug("This is a debug message")
logger.info("This is an info message")
logger.warning("This is a warning message")
logger.error("This is an error message")
logger.critical("This is a critical message")
"""


class AutoScout():
    def __init__(self):
        self.article_counter = 0
        self.failed_article_counter = 0
        self.page_counter = 0

    async def get_car_details(self, subpage_link, session, article):
        soup, num_failed_articles = await helpers_functions.get_soup_from_page(subpage_link, session)
        self.failed_article_counter += num_failed_articles
        if soup is None:
            return None

        car_summary = get_car_summary(article, base_url)
        additional_json_data = get_additional_json_data(soup, subpage_link, logger)
        section_data_combined = get_section_data(soup)
        additional_data = helpers_functions.add_additional_data()
        car_details = ({
            "car_summary": car_summary,
            "additional_json_data": additional_json_data,
            "section_data_combined": section_data_combined,
            "additional_data": additional_data
        }
        )

        return car_details


    async def combine_data(self, additional_json_data, section_data_combined, data_car_summary, additional_data):
        with open("src/result_columns.json", "r") as f:
            result_schema = json.loads(f.read())

        car_details = result_schema

        #print("CAR SUMMARY COMBINED", data_car_summary)

        # Hierarchy of data sources
        for key, item in result_schema.items():
            additional_json_value = additional_json_data.get(key, None)
            data_car_summary_value = data_car_summary.get(key, None)
            section_data_combined_value = section_data_combined.get(key, None)
            if additional_json_value and additional_json_value not in ["None", "", "null"]:
                #print("1 WE USE JSON")
                car_details[key] = additional_json_data.get(key)
            elif data_car_summary_value and data_car_summary_value not in ["None", "", "null"]:
                #print("2 WE USE data_car_summary")
                car_details[key] = data_car_summary.get(key)
            elif section_data_combined_value and section_data_combined_value not in ["None", "", "null"]:
                #print("3 WE USE section_data_combined")
                car_details[key] = section_data_combined.get(key)
            """
            else:
                if key == "ad_url":
                    logger.warning(f"Could not find a value for this key {key}")
            """


        #print("CAR SUMMARY AFTER COMBINED", car_details)


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
            soup, num_failed_articles = await helpers_functions.get_soup_from_page(url, session)
            self.failed_article_counter += num_failed_articles
            page_max = helpers_functions.get_last_page_number(soup)
            self.page_counter += 1

        except Exception as e:
            pass
            #print("fail")
            #print(e)
            #traceback.print_exc()  # This will print the full traceback


    # Looping through all pages
        for page in range(1, page_max + 1):
            #print("")
            #print(f"*** Processing page: {page}/{page_max}")

            try:
                url = url_filter + f"&page={page}"
                soup, num_failed_articles = await helpers_functions.get_soup_from_page(url, session)
                self.failed_article_counter += num_failed_articles
                if soup is None:
                    break

                # Getting all articles for this page
                articles = helpers_functions.get_car_articles(soup)
                len_articles = len(articles)
                #print(f"Processing this number of articles: {len_articles}")
                if len_articles < 1:
                    break
                all_articles.extend(articles)  # Collect all articles from all pages

            except Exception as e:
                logger.error("skipping this page. Maybe no soup or other error. Going to next page.")
                logger.error(e)
                logger.error(traceback.print_exc())
                continue

            # Creating Async Tasks: For each article getting car details
            if articles:
                for article in articles:
                    self.article_counter += 1
                    subpage_link = helpers_functions.get_subpage_link(article, base_url)
                    task = asyncio.create_task(self.get_car_details(subpage_link, session, article))
                    tasks.append(task)
                # Log the number of concurrent tasks
                #logger.info(f"Number of concurrent tasks: {len(tasks)}")

            else:
                logger.error("NO ARTICLES FOUND")


            if test_mode:
                #print("Test mode. only using first page")
                break

        # Executing asnyc task to get all car details
        #logger.info(f"Total running tasks BEFORE: {len(asyncio.all_tasks())}")
        car_details = await asyncio.gather(*tasks)
        #logger.info(f"Total running tasks AFTER: {len(asyncio.all_tasks())}")

        for car in car_details:
            try:
                data_combined = await self.combine_data(car["additional_json_data"], car["section_data_combined"], car["car_summary"], car["additional_data"])
                articles_parsed.append(data_combined)

            except Exception as e:
                logger.error("failed processing this article: Going to next article")
                logger.error(e)
                logger.error(traceback.print_exc())
                continue


        return articles_parsed



    async def reaggregate_all_data(self):
        logger.info("Start: Reaggregating all datae")


        ## Loop through all body types
        body_types = [1, 2, 3, 4, 5, 6, 7]
        async with aiohttp.ClientSession() as session:
            for body_type in body_types:
                logger.info("")
                logger.info("********** Loop for body_type: {}".format(body_type))


                from_price = 20000
                to_price = 20049
                step = 50
                data = []
                price_limit_1 = 100000
                price_limit_last = 400000
                next_is_last_round = False
                ## Loop through all price ranges
                #for price in range(from_price, to_price, step):
                while True:

                    if next_is_last_round:
                        pass
                        #print("Using last call now")





                    #print("")
                    if from_price % 10000 == 0: # or round_down_to_nearest_hundred_thousand(from_price) % 100000 == 0
                        logger.info("-------------------------")
                        logger.info("Status update")
                        helpers_functions.get_execution_time(start_time)
                        logger.info("Number of processed articles {}".format(self.article_counter))
                        logger.info("Number of failed articles {}".format(self.failed_article_counter))
                        logger.info("-------------------------")
                        logger.info("")
                        logger.info("****** price range: {} - {}".format(from_price,to_price ))


                    url = f"https://www.autoscout24.com/lst?atype=C&cy=D%2CA%2CB%2CE%2CF%2CI%2CL%2CNL&damaged_listing=exclude&desc=1&fregto=2005&powertype=kw&search_id=gd6zvktyks&sort=age&source=listpage_pagination&ustate=N%2CU&pricefrom={from_price}&priceto={to_price}&body={body_type}"

                    # Get all cars ant their data
                    data += await self.loop_through_all_pages(url, session, base_url)


                    # Increasing price steps

                    if next_is_last_round:
                        break


                    if from_price >= price_limit_last:
                        step = 100000000
                        next_is_last_round = True
                        from_price = to_price + 1

                    elif to_price == price_limit_1 - 1:
                        step = 1000
                        #print(f"Increasing Steps to: {step}")
                        from_price = price_limit_1

                    else:
                        from_price = from_price + step

                    # Get all which have higher price


                    to_price = from_price + step - 1







                # Write data to csv
                helpers_functions.write_data_to_csv(data, csv_path)
                if test_mode:
                    logger.info("test mode: stopping after one body type")
                    break


            # Final run after loop ended
            """
            final_from_price = 400000
            url = f"https://www.autoscout24.com/lst?atype=C&cy=D%2CA%2CB%2CE%2CF%2CI%2CL%2CNL&damaged_listing=exclude&desc=1&fregto=2005&powertype=kw&search_id=gd6zvktyks&sort=age&source=listpage_pagination&ustate=N%2CU&pricefrom={final_from_price}"
            await self.loop_through_all_pages(url, session, base_url)
            """

            # Read CSV and save result in Big Query
            df = pd.read_csv(csv_path)
            df = clean_and_prepare_df(df)
            upload_to_bigquery(df, bigquery_project, bigquery_table)


    async def get_newest_data(self):
        logger.info("Start: Getting newest data")
        from_price = 20000
        url = f"https://www.autoscout24.com/lst?atype=C&cy=D%2CA%2CB%2CE%2CF%2CI%2CL%2CNL&damaged_listing=exclude&desc=1&fregto=2005&powertype=kw&pricefrom={from_price}&search_id=gd6zvktyks&sort=age&source=listpage_pagination&ustate=N%2CU"

        # Get all cars ant their data
        async with aiohttp.ClientSession() as session:
            data = await self.loop_through_all_pages(url, session, base_url)

        helpers_functions.write_data_to_csv(data, csv_path)

        existing_record_ids = get_existing_record_ids(bigquery_project, bigquery_dataset_id, bigquery_table_id)
        df = pd.read_csv(csv_path)
        num_rows_before = df.shape[0]
        df = df[~df['record_id'].isin(existing_record_ids)]
        num_rows_after = df.shape[0]
        logger.info(f"Removed this number of duplicate record ids: {num_rows_after - num_rows_before}")
        df = clean_and_prepare_df(df)
        upload_to_bigquery(df, bigquery_project, bigquery_table)


    async def run(self):
        helpers_functions.delete_csv_if_exists(csv_path)
        if is_aggregation:
            await self.reaggregate_all_data()
            logger.info("FINAL: Number of processed articles {}".format(self.article_counter))
        else:
            await self.get_newest_data()



# Main script
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Description of your program")
    parser.add_argument("-a", "--is_aggregation", help="If true will reaggregate all data. Otherwise only newest 20 pages.", type=ast.literal_eval, required=True)
    parser.add_argument("-t", "--test_mode", help="If true test mode will be on. Only sample data will be processed", type=ast.literal_eval, required=True)
    parser.add_argument("-p", "--csv_path", help="Path of csv to store results", type=str, required=True)
    parser.add_argument("-bp", "--big_query_project", help="project where to write big query table", type=str, required=True)
    parser.add_argument("-bt", "--big_query_table", help="big query table (dataset.table)", type=str, required=True)
    parser.add_argument("-l", "--logger_path", help="path of logger file", type=str, required=True)

    args = parser.parse_args()




    autoscout = AutoScout()
    import time
    start_time = time.time()  # Start the time
    is_aggregation = args.is_aggregation
    test_mode = args.test_mode
    csv_path =args.csv_path #"result/autoscout_data_7.csv"
    logger_path = args.logger_path
    bigquery_project = args.big_query_project # "python-rocket-1"
    bigquery_table = args.big_query_table # "assetclassics.autoscout_scrapper_sample_11"
    bigquery_dataset_id = args.big_query_table.split(".")[0]
    bigquery_table_id = args.big_query_table.split(".")[1]

    logging.basicConfig(
        level=logging.DEBUG,  # Set the log level
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Set the log format
        handlers=[
            logging.FileHandler(logger_path),  # Log to a file
            logging.StreamHandler()  # Also log to the console
        ]
    )
    logger = logging.getLogger(__name__)
    helpers_functions = HelperFunctions(logger)

    #bigquery_project = "ac-vehicle-data"
    #bigquery_table = "autoscout24.autoscout_scrapper_sample_v1"

    base_url = "https://www.autoscout24.com"
    asyncio.run(autoscout.run())

    helpers_functions.get_execution_time(start_time)

