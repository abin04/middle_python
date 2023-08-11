import psycopg2
from config import config
conn = None
data_prep = [(84756985,''),(83279672,''),(83279672,''),(83279672,''),(83279672,''),(83279672,''),(83279672,''),(83279672,''),(83279672,''),
             (83279672,''),(83279672,''),(83279672,''),(83279672,''),(83279672,''),(83279672,''),(83279672,''),(83279672,''),(83279672,''),
             (83279672,''),(83279672,'')]
try:
    params = config()
    conn = psycopg2.connect(**params)
    cur = conn.cursor()
    for id, names in data_prep:
        insert_several_rows_parameters = f'UPDATE vacancies SET company_name={names} WHERE id={id};'
        cur.execute(insert_several_rows_parameters)
    conn.commit()
    cur.close()     
except (Exception, psycopg2.DatabaseError) as error:
    print(error)
finally:
    if conn is not None:
        conn.close() 
