import asyncio
import requests
from aiohttp import ClientSession, TCPConnector
import logging
from bs4 import BeautifulSoup

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
    async with ClientSession('https://api.hh.ru/', connector=connector) as session:
        tasks = []
        for id in ids:
            tasks.append(asyncio.create_task(get_vacancy(id, session)))
        results = await asyncio.gather(*tasks)
    for result in results:
        skills_name =''
        for skills in result['key_skills']:
            skills_name += skills['name'] +", "
        data_prep.append((result['name'], result['employer']['name'],
                          BeautifulSoup(result['description'],'lxml').get_text(),
                          skills_name))
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
vacancies_ids = []
for i in range(5):
    # while len(data_prep)<100:
    page = {"page":str(i)}
    params = {**url_params, **page}
    result = requests.get(url, params=params)
    vacancies = result.json().get('items')
    for vacancy in vacancies:
        vacancies_ids.append(vacancy['id'])
asyncio.run(main(vacancies_ids ))
print(data_prep)