Елисеев Е.В. Исходные коды к выпускной работе бакалавра по теме "Разработка системы поиска шаблонов в данных портала Github"

Настройка базы данных PostgreSQL(16.2):
CREATE USER github_admin WITH LOGIN PASSWORD 'root';

CREATE DATABASE github_data WITH OWNER github_admin;

