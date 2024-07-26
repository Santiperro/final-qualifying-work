# Приложение для поиска шаблонов в данных портала GitHub
## Елисеев Е.В.

Проект включает в себя приложение Django с веб-интерфейсом, которое позволяет отбирать и загружать данные GitHub за любой период, а также применять поиск ассоциативных правил для нахождения сложных зависимостей. Поиск шаблонов в данных GitHub может помочь исследовать, например, какие факторы влияют на популярность репозитория, активность команды и т.д.

## Установка

1. Установите [Python 3.11.7](https://www.python.org/downloads/release/python-3117/) 
2. Клонируйте репозиторий на ваш локальный компьютер
3. Перейдите в директорию проекта и создайте виртуальную среду при помощи команды `py -m venv .venv`
4. Активируйте виртуальную среду командой `.venv/scripts/activate`, обновите pip `py -m pip install --upgrade pip`
5. Установите необходимые зависимости командой `pip install -r requirements.txt`
6. Скачайте и установите [PostgreSQL(16.2)](https://www.enterprisedb.com/downloads/postgres-postgresql-downloads)
7. Войдите в консоль psql и выполните команды `CREATE USER github_admin WITH LOGIN PASSWORD 'root';` и `CREATE DATABASE github_data WITH OWNER github_admin;`
8. Выполните команды `py manage.py makemigrations` и `py manage.py migrate`
9. Запустите приложение, выполнив файл `py manage.py runserver`<br>
(Дополнительно) Для возможности запроса дополнительных данных из GitHub API вставьте свой GitHub токен в /github_patterns/github_patterns/.env GITHUB_KEY='{ваш токен}' 
