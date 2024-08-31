## Fetches car data from autoscout and saves it in big query


```bash
python3 -m venv venv
source venv/bin/activate
pip3 install -r requirements.txt

# Start autoscout scrapper
cd websites/autoscout
# Run daily (change csv path and big query destination)
python3 main.py -a False -t False -p result/autoscout_data_8.csv -bp python-rocket-1 -bt assetclassics.autoscout_scrapper_sample__daily_1 -l "app_d.log"
# Reaggregate all data (change csv path and big query destination)
python3 main.py -a True -t False -p result/autoscout_data_8.csv -bp python-rocket-1 -bt assetclassics.autoscout_scrapper_sample__daily_1 -l "app_d.log"
# New version 
python3 main.py -a True -t True -p result/autoscout_data_1.csv -bp python-rocket-1 -bt assetclassics.all_cars_data_1 -400 "result/models_more_400.csv" -fc "result/failed_cars.csv" -l  "test.log"

```