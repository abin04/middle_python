import requests
from bs4 import BeautifulSoup
import sqlite3

url = 'https://tula.hh.ru/search/vacancy?text=Middle+python&salary=&ored_clusters=true'
url_next = 'https://tula.hh.ru/search/vacancy?text=Middle+python&salary=&ored_clusters=true&page=1&hhtmFrom=vacancy_search_list'
user_agent = {'User-agent': 'Mozilla/5.0'}
data_prep = []

def append_id(url, session, user_agent):
    """
    функция принимает адрес страницы поиска вакансий по запросу 'middle python' (~20 вакансий)
    возвращает список id вакансий (~20 id)
    """
    vacancy_id = []
    with session.get(url=url, headers=user_agent) as result:
        soup = BeautifulSoup(result.content, 'lxml')
        links = soup.find_all('a',{"class":"serp-item__title"})
        for link in links:
            if 'Middle+python' in link.attrs.get('href'):
                vacancy_id.append(link.attrs.get('href')[27:35])
    return vacancy_id

def find_key_skills(session, vacancy_id, data_prep, user_agent):
    """
    функция принимает адрес вакансии и если в ключевых навыках есть питон добавляет в список данных
    для записи в базу
    """
    url = f'https://tula.hh.ru/vacancy/{vacancy_id}'
    with session.get(url=url, headers=user_agent) as response:
        soup = BeautifulSoup(response.content,'lxml')
        if soup.find_all('span', {"data-qa":"bloko-tag__text"}, string='Python'):
            # print(vacancy_id)
            company_name =soup.find('span', {"data-qa":"bloko-header-2"}).text
            position = soup.find('h1', {"data-qa":"vacancy-title"}).text
            job_description = soup.find('div', {"data-qa":"vacancy-description"}).text
            skills = soup.find_all('span', {"data-qa":"bloko-tag__text"})
            key_skills = ''
            for skill in skills:
                key_skills+= f'{skill.text}, '
            data_prep.append((company_name, position, job_description, key_skills))
    return data_prep

with requests.Session() as session:
    vacancies = append_id(url, session, user_agent)
    for vacancy in vacancies:
        find_key_skills(session, vacancy, data_prep, user_agent)
    while len(data_prep)<100:
        vacancies = append_id(url_next, session, user_agent)
        for vacancy in vacancies:
            find_key_skills(session, vacancy, data_prep, user_agent)
    
try:
    connection = sqlite3.connect('hw1.db')
    cursor = connection.cursor()
    new_table = """ CREATE TABLE IF NOT EXISTS vacancies
    (
        id INTEGER PRiMARY KEY AUTOINCREMENT,
        company_name TEXT,
        position TEXT,
        job_description TEXT,
        key_skills TEXT
    )
    """
    cursor.execute(new_table)
    insert_several_rows_parameters = """
        INSERT INTO vacancies (company_name, position, job_description, key_skills)
        VALUES (?, ?, ?, ?)
        """
    cursor.executemany(insert_several_rows_parameters, data_prep)
    connection.commit()
    cursor.close()
except sqlite3.DatabaseError as err:
    print("Ошибка:", err)
else:
    print("Запрос успешно выполнен")
