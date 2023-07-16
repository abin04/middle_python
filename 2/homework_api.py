import asyncio
import requests
from aiohttp import ClientSession, TCPConnector
import logging
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column
from sqlalchemy import String, Integer

async def get_vacancy(id, session):
    """
    функция принимает id вакансии и сессию с сайтом и возвращает строку описания вакансии
    """
    url = f'/vacancies/{id}'
    logging.debug(f"Начата загрузка вакансии {url}")
    async with session.get(url=url) as response:
        vacancy_json = await response.json()
        logging.debug(f"Завершена загрузка вакансии {url}")
        return vacancy_json
async def main(ids):
    """
    функция принимает список id вакансий и возвращает список кортежей с параметрами вакансий 
    """
    connector = TCPConnector(limit=5)
    async with ClientSession('https://api.hh.ru/', connector=connector, trust_env=True) as session:
        tasks = []
        for id in ids:
            tasks.append(asyncio.create_task(get_vacancy(id, session)))
        results = await asyncio.gather(*tasks)
    for result in results:
        if result['key_skills']:
            skills_name =''
            for skills in result['key_skills']:
                skills_name += skills['name'] +", "
            data_prep.append((result['name'], result['employer']['name'],
                            BeautifulSoup(result['description'],'lxml').get_text(),
                            skills_name))
    session.close
    return data_prep
logging.basicConfig(filename='my_app.log', 
                    encoding='utf-8', 
                    level=logging.DEBUG,
                    format="%(asctime)s %(name)s %(levelname)s %(message)s")
url = "https://api.hh.ru/vacancies"
url_params = {
    "text": "middle python",
    "search_field": "name",
    "per_page": "20",
    # "page": "1",
}
data_prep = []
for i in range(10):
    if len(data_prep)<100:
        vacancies_ids = []
        page = {"page":str(i)}
        params = {**url_params, **page}
        result = requests.get(url, params=params)
        vacancies = result.json().get('items')
        logging.debug(f"Получена {i} страница поиска вакансий")
        for vacancy in vacancies:
            vacancies_ids.append(vacancy['id'])
        asyncio.run(main(vacancies_ids ))
        logging.debug(f"Получено {len(data_prep)} вакансий")
# print(data_prep)
Base = declarative_base()
class Vacancy_prep(Base):
    __tablename__ = 'vacancies'
    id = Column('id', Integer, primary_key=True, autoincrement=True)
    company_name = Column('company_name', String)
    position = Column('position', String)
    job_description = Column('job_description', String)
    key_skills = Column('key_skills', String)
    def __init__(self, company_name, position, job_description, key_skills):
        self.company_name = company_name
        self.position = position
        self.job_description = job_description
        self.key_skills = key_skills
engine = create_engine("sqlite:///hw2.db", echo=True)
Base.metadata.create_all(bind=engine)
Sesion = sessionmaker(bind=engine)
session = Sesion()
for record in data_prep:
    new_record = Vacancy_prep(company_name=record['company_name'],
                              position=record['position'],
                              job_description=record['job_descripton'],
                              key_skills=record['key_skills'])
    session.add(new_record)
session.commit()
session.close()