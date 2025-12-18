# de_project

## Требования
- Docker Desktop (Windows/macOS) или Docker Engine (Linux)
- docker compose

## Быстрый старт (рекомендуется): восстановление из дампа
1) Клонируй репо:
   git clone <REPO_URL>
   cd <repo>

2) Создай env:
   cp .env.example .env

3) Подними сервисы (чисто):
   docker compose down -v || true
   docker compose up -d

4) Восстанови БД из дампа (файл de_db.dump скачать отдельно по ссылке в репо/релизах):
   docker cp de_db.dump de_postgres:/tmp/de_db.dump
   docker compose exec postgres pg_restore -U de_user -d de_db --clean --if-exists /tmp/de_db.dump

5) pgAdmin:
   http://localhost:5050

## Медленный старт: загрузка parquet + нормализация
1) cp .env.example .env
2) docker compose down -v || true
3) docker compose up -d
4) положи *.parquet в папку ./data
5) загрузить raw:
   docker compose run --rm loader
6) нормализация:
   docker compose exec postgres psql -U de_user -d de_db -f /sql/normalize.sql
