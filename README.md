## Проект `ETL`

### Установка:
1. Перейти в каталог с проектом `/etl` и переименовать файл `.env.template` в `.env` и заполнить переменными окружения согласно шаблону
2. Перейти в корневую директорию и начать билд и запуск контейнеров
    `docker compose up`
    У вас должен быть установлен [докер](https://docs.docker.com/engine/install/)
3. После завершения процесса запуска контейнеров, сервис готов к работе:
   - Создается БД, заполненная данными;
   - Создается индекс `movies` в Elasticsearch;
   - Осуществляется первичная загрузка данных в Elasticsearch;
   - Запускается отслеживание изменений в таблицах `film_work`, `person`, `genre`.
4. Файл с тестами `ETLTests.json` для Postman находится в корне проекта.

P.S. Для старта скрипта локально вне контейнера во время разработки важно запускать его из корня проекта командой
`python -m etl.main`
