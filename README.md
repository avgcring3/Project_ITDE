# de_project

## Требования
- Docker Desktop + WSL2 (Windows) **или** Docker Engine (Linux/macOS)
- `docker compose`

---

## Быстрый старт (рекомендуется): восстановление из дампа

### 1) Клонируй репо и подними контейнеры
```bash
git clone https://github.com/avgcring3/Project_ITDE.git
cd Project_ITDE

cp .env.example .env

docker compose down -v || true
docker compose up -d
```
### 2) Скачай дамп и восстанови БД (Linux / WSL / macOS)
```bash
curl -L -o de_db.dump.gz https://github.com/avgcring3/Project_ITDE/releases/download/v1.0.0/de_db.dump.gz
gunzip -c de_db.dump.gz | docker exec -i de_postgres pg_restore -U de_user -d de_db --clean --if-exists
```
### 3) Открой pgAdmin
http://localhost:5050

Логин и пароль берутся из файла .env:

PGADMIN_DEFAULT_EMAIL

PGADMIN_DEFAULT_PASSWORD

# Подключение к Postgres
### Подключение с хоста (DBeaver / psql / IDE)
```text
host: localhost
port: 5432
db:   de_db
user: de_user
pass: de_password
```
### Подключение из pgAdmin (контейнер → контейнер)
В pgAdmin: Register → Server
```text
Host name/address: de_postgres
Port:              5432
Maintenance DB:    de_db
Username:          de_user
Password:          de_password
```
## Проверка, что данные восстановились (опционально)
```bash
docker exec -it de_postgres psql -U de_user -d de_db \
  -c "select count(*) as orders_cnt from dwh.orders;"
```  
