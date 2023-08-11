import logging
from pathlib import Path
import psycopg2
import json
import zipfile
from config import config
import os

logger = logging.getLogger(__name__)
path_zip=Path.home().joinpath('egrul.json.zip')
path_org=Path.home().joinpath('airflow','org.txt')
data_prep_corp=[]
conn = None

def read_json():

    with open(path_org, "a") as file:
        #if os.path.getmtime(path_zip)>os.path.getmtime(path_org):
            try:
                with zipfile.ZipFile(path_zip) as archive:
                    count_write = 0
                    for filename in archive.namelist(): # получить список файлов в архиве
                        with archive.open(filename) as files: # пройти по файлам архива
                            logger.info(f"Начата обработка файла {files.name}")
                            json_files=json.load(files)
                            for row in json_files: # пройти по записям файла
                                if row['data'].get('СвОКВЭД'): # если в записи присутствует ключ
                                    if row['data']['СвОКВЭД'].get('СвОКВЭДОсн'): # если в записи присутствует ключ
                                        okved=row['data']['СвОКВЭД']['СвОКВЭДОсн'] # считать код
                                        if okved['КодОКВЭД'][:3]=='61.': # отобрать записи по коду 61
                                        # добавить запись в список
                                            data_prep_corp.append((row['ogrn'],row['full_name'],row['inn'],okved['КодОКВЭД'],okved['НаимОКВЭД']))
                                            count_write+=1 # количество записей в бд    
                    files.close
                for  line in data_prep_corp:
                    file.write(','.join(line)) 
            except zipfile.BadZipFile as error:
                logger.info(f'Ошибка архива {error}')
            finally:
                archive.close()
                logger.info(f'подготовлено {count_write} записей компаний')
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        insert_several_rows_parameters = 'INSERT INTO telecom_companies (ogrn, name_corp, inn, okved, okved_name) VALUES (%s, %s, %s, %s, %s)'
        cur.executemany(insert_several_rows_parameters, data_prep_corp)
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        logger.info(error)
    finally:
        if conn is not None:
            conn.close()
