FROM python:3.11-slim

WORKDIR /opt/etl

ENV PYTHONUNBUFFERED=1

COPY ./requirements.txt requirements.txt

RUN mkdir -p /opt/state/ \
    && pip install --upgrade pip \
    && pip install -r requirements.txt

COPY . .
ENTRYPOINT ["python", "-m", "etl.main"]