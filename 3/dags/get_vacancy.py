from bs4 import BeautifulSoup
import requests
import time
import logging
import psycopg2
from config import config

conn = None
logger = logging.getLogger(__name__)
url = "https://api.hh.ru/vacancies"
url_params = {
    "text": "middle python",
    "search_field": "name",
    "per_page": "20",
}

def get_vacancy(url, id, session):
    """
    функция принимает id вакансии и сессию с сайтом и возвращает строку описания вакансии
    """
    url_vac = f'{url}/{id}'
    with session.get(url=url_vac) as response:
        vacancy_json = response.json()
        logger.info(f"Загружена вакансия {url_vac}")
        return vacancy_json

def find_vacancies():
    """
    функция возвращает скачивает описание вакансий и записывает их в базу 
    """

    for i in range(15):
        data_prep_vacancy = []
        page = {"page":str(i)}
        params = {**url_params, **page}
        result_vac = requests.get(url, params=params)
        vacancies = result_vac.json().get('items')
        for vacancy in vacancies:
            with requests.session() as session:
                result=get_vacancy(url, vacancy['id'], session)
                if result['key_skills']:
                    skills_name =''
                    for skills in result['key_skills']:
                        skills_name += skills['name'] +", "
                    data_prep_vacancy.append((vacancy['id'], result['name'], result['employer']['name'],
                                        BeautifulSoup(result['description'],'lxml').get_text(),
                                        skills_name))
        try:
            params = config()
            conn = psycopg2.connect(**params)
            cur = conn.cursor()
            insert_several_rows_parameters = 'INSERT INTO vacancies (id, position, company_name, job_description, key_skills) VALUES (%s, %s, %s, %s, %s)'
            cur.executemany(insert_several_rows_parameters, data_prep_vacancy)
            conn.commit()
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            logger.info(error)
        finally:
            if conn is not None:
                conn.close()
    session.close
