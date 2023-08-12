import logging
import psycopg2
from collections import Counter
from config import config
conn = None
logger = logging.getLogger(__name__)
def calc():
   try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute('SELECT vacancies.key_skills FROM telecom_companies INNER JOIN vacancies ON telecom_companies.name_corp = vacancies.company_name;')
        rows=cur.fetchall()
        cur.close()
        lst=''
        for row in rows:
            for words in row:
                lst+=f'{words} '
        list_text = lst.lower().replace(',  ', '').split(',')
        counts = Counter(list_text)
        top=sorted(counts.items(), key=lambda x: x[1], reverse=True)
        top10=top[:10]
        with open('total.txt', "w") as file:
            file.write(f'{top10}')     
    except (Exception, psycopg2.DatabaseError) as error:
        logger.info(error)
    finally:
        if conn is not None:
            conn.close()


    
    
    
