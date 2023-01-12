FROM python:3.11-slim

WORKDIR /opt/etl

ENV PYTHONUNBUFFERED=1

COPY ./requirements.txt requirements.txt

RUN mkdir -p /opt/state/ \
    && pip install --upgrade pip \
    && pip install -r requirements.txt

COPY . .
HEALTHCHECK --interval=30s --timeout=5s \
  CMD curl -f http://localhost:5432/ || exit 1

ENTRYPOINT ["python", "-m", "etl.main"]
