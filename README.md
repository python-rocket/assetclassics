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
# Auth in with gcloud
gcloud auth login
gcloud config set project [PROJECT_ID]
gcloud auth list

# Set variables
project_id=python-rocket-1
# Service id number
gcloud projects describe $project_id --format="value(projectNumber)" 
service_id=340162917499 

gcloud artifacts repositories create autoscout-scrapper --repository-format=docker --location=europe-west1

gcloud auth configure-docker europe-west1-docker.pkg.dev

docker build -t autoscout --platform .

docker build -t autoscout --platform linux/amd64 . #(mac m processor users)

docker tag autoscout europe-west1-docker.pkg.dev/$project_id/autoscout-scrapper/autoscout:latest

docker push europe-west1-docker.pkg.dev/$project_id/autoscout-scrapper/autoscout:latest

gcloud run jobs create autoscout --image europe-west1-docker.pkg.dev/$project_id/autoscout-scrapper/autoscout:latest --region europe-west1 --max-retries 1 --task-timeout=86400

gcloud scheduler jobs create http autoscout-job_test --schedule "00 00 * * *" --http-method POST --uri https://europe-west1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/$project_id/jobs/autoscout:run --location="europe-west1" --time-zone "Europe/Warsaw" --oauth-service-account-email "${service_id}-compute@developer.gserviceaccount.com" --oauth-token-scope "https://www.googleapis.com/auth/cloud-platform"

```