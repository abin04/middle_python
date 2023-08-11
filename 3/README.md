Итоговый проект: Python для ETL (Попов Сергей)


Создано вирт окружение (python -m venv airflow) и активировано (source airflow/bin/activate)

Установлен python 3.10.11, Postgesql

Необходимые модули в файле requirements.txt (python -m pip install -r .\requirements.txt)

Конфигурация и настройка

Создана база project, пользователю airflow_user даны права на базу. (встречалась рекомендация называть базу и пользователя по логину в системе???)
Создана папка ~/airflow, ~/airflow/dags, настроен airflow.cfg

В папке dags:

result_project  основной модуль;
get_company     модуль из архива считывает файлы справочника ЕГРЮЛ и записывает в базу в таблицу telecom_companies предприятия с кодом ОКВЭД 61;
get_vacansy     модуль скачивает вакансии middle python (до 15 страниц по 20 вакансий) и записывает их в базу в таблицу vacancies;
get_skills      модуль связывает таблицы telecom_companies и vacancies по нормализованному полю имени компании, выбирает key_skills, подсчитывает top и записывает в файл total.txt;
config.py database.ini  модули подключения к базе данных. Кроме того в airflow/admin/connections настроено соединение "postgres_default" для postgres_conn_id;

В папке dags/sql:

telecom_schema.sql, vacancy_schema.sql  запросы создания таблиц telecom_companies и vacancies в базе;
update_records_telecom.sql, update_records_vacancy.sql запросы приведения имени компании к нормализованному виду. (отбросить ООО и перевести все буквы в верхний регистр)

Tasks:

start,stop  фиксирует в лог-файле старт/стоп DAGа;
download_file_egrul скачивает на диск zip-архив справочника ЕГРЮЛ с проверкой даты локального и файла на свйте и возможностью докачки;
parse_egrul запускает функцию read_json из модуля get_company;
create_telecom_table выполняет запрос создания таблицы из telecom_schema.sql;
update_records_telecom выполняет запрос приведения имени компании к нормализованному виду из update_records_telecom.sql;
download_vacancies запускает функцию find_vacancies из модуля get_vacansy;
create_vacancy_table выполняет запрос создания таблицы из vacancy_schema.sql;
update_records_vacancy выполняет запрос приведения имени компании к нормализованному виду из update_records_vacancy.sql;
top_skills запускает функцию calc из модуля get_skills.

Примечания:

В качестве регионов выбрана вся Россия.
После первой удачной записи в таблицу telecom_companies (~20000 записей) можно раскоментировать в модуле get_company строку сравнения даты скаченного архива ЕГРЮЛ с файлом org.txt в котором сохраняется результат работы функции read_json. Это необходимо для обхода продолжительной операции выборки если архив на сайте не изменялся со времени последнего запуска.
На реальных данных по названиям совпала лишь одна компания "РБК". Для большей наглядности пришлось изменить названия компаний в нескольких вакансиях запустив replace_name.py и повторить расчет get_skills.py

Задание (справочно)

Разработайте поток работ для Apache AirFlow, который должен включать следующие этапы:
1. Загрузка архива Единого государственного реестра юридических лиц в формате JSON по ссылке https://ofdata.ru/open-data/download/egrul.json.zip.
2. Выбор из реестра компаний, занимающихся деятельностью в сфере телекоммуникаций (Группировка ОКВЭД 61).
3. Поиск на сайте hh.ru вакансий middle python developer от компаний, занимающихся деятельностью в сфере телекоммуникаций. Допускается использовать парсинг Web-страниц или REST API.
4. Составление списка ТОП-10 наиболее востребованных ключевых навыков middle python developer в компаниях, занимающихся деятельностью в сфере телекоммуникаций.
