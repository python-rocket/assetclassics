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
python3 main.py -t False -p result/autoscout_data_1.csv -bp ac-vehicle-data -bt autoscout24.distinct_autoscout_records -400 result/models_more_400.csv -fc result/failed_cars.csv -l test.log
```

## Create artifact registry, deploy cloud run and set up scheduler
```bash
gcloud artifacts repositories create autoscout-scrapper \
  --repository-format=docker \
  --location=europe-west1

gcloud auth configure-docker europe-west1-docker.pkg.dev

docker build -t autoscout --platform .

docker build -t autoscout --platform linux/amd64 . (mac m processor users)

docker tag autoscout europe-west1-docker.pkg.dev/python-rocket-1/autoscout-scrapper/autoscout:latest

docker push europe-west1-docker.pkg.dev/python-rocket-1/autoscout-scrapper/autoscout:latest

gcloud run deploy autoscout --image europe-west1-docker.pkg.dev/python-rocket-1/autoscout-scrapper/autoscout:latest --platform managed --region europe-west1 --allow-unauthenticated

gcloud scheduler jobs create http autoscout-job --schedule="00 00 * * *" --time-zone="Europe/Warsaw" --http-method=GET --uri="https://autoscout-340162917499.europe-west1.run.app/start" --location="europe-west1"
```