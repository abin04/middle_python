import psycopg2
from config import config
conn = None
try:
    params = config()
    conn = psycopg2.connect(**params)
    cur = conn.cursor()
    cur.execute('select name_corp from telecom_companies limit 20;')
    names=cur.fetchall()
    cur.execute('select id from vacancies limit 20;')
    ids=cur.fetchall()
    for i in range(20):
        name=names[i]
        id=ids[i]
        insert_several_rows_parameters = f"UPDATE vacancies SET company_name = '{name[0]}' WHERE id = {id[0]};"
        cur.execute(insert_several_rows_parameters)
    conn.commit()
    cur.close()     
except (Exception, psycopg2.DatabaseError) as error:
    print(error)
finally:
    if conn is not None:
        conn.close() 

