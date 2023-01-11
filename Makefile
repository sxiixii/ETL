PROJECT_NAME = ETL

all:
	@echo "make es_index		- Создать схему "

es_index:
	docker compose exec elasticsearch chmod +x docker-entrypoint.sh && ./docker-entrypoint.sh
