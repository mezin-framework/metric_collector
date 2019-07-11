
FROM python:3

COPY . /var/app/collector

WORKDIR /var/app/collector

RUN pip install -r requirements.txt
