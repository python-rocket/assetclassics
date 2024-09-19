FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV GOOGLE_APPLICATION_CREDENTIALS="/app/key_file.json"

WORKDIR /app/src/websites/autosout

#EXPOSE 8080
#
#CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8080", "--reload"]
CMD ["python3", "main.py", "-t", "False", "-p", "result/autoscout_data_1.csv", "-bp", "ac-vehicle-data", "-bt", "autoscout24.autoscout_daily_scraper_v2", "-400", "result/models_more_400.csv", "-fc", "result/failed_cars.csv", "-l", "test.log"]