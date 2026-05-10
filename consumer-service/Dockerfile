FROM python:3.11-slim

WORKDIR /service

RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc g++ curl librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

COPY consumer-service/requirements.txt /service/requirements.txt
RUN pip install --upgrade pip \
    && pip install -r /service/requirements.txt

COPY consumer-service/app /service/app
COPY schemas /service/schemas

EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
