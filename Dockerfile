# Pulled June 16, 2021
FROM python:3.8@sha256:c7706b8d1b1e540b9dd42ac537498d7f3138e4b8b89fb890b2ee4d2c0bccc8ea
RUN apt-get update && apt-get install -y brotli
RUN pip install --upgrade pip
WORKDIR /srv
COPY requirements.txt requirements-docker.txt ./
RUN pip install -r requirements-docker.txt
COPY setup.py ./
COPY open_bus_siri_etl ./open_bus_siri_etl
RUN pip install -e .
ENV PYTHONUNBUFFERED=1
ENV SQLALCHEMY_URL=postgresql://postgres:123456@db
ENTRYPOINT ["open-bus-siri-etl"]
