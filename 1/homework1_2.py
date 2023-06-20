import zipfile
import json
import sqlite3
from pathlib import Path

def load_with_copy(lists):
    connection = sqlite3.connect('hw1.db')
    cursor = connection.cursor()
    insert_several_rows_parameters = """
        INSERT INTO telecom_companies (ogrn, name, inn, okved, okved_name)
        VALUES (?, ?, ?, ?, ?)
    """
    cursor.executemany(insert_several_rows_parameters, lists)
    connection.commit()
    cursor.close()

connection = sqlite3.connect('hw1.db') # создать базу
cursor = connection.cursor()
new_table = """ CREATE TABLE IF NOT EXISTS telecom_companies(
    ogrn INTEGER PRIMARY KEY,
    name TEXT,
    inn INTEGER,
    okved TEXT,
    okved_name TEXT)"""
cursor.execute(new_table)
connection.commit()
cursor.close()

path=Path.home().joinpath('Загрузки','egrul.json.zip') # ввести путь загрузки файла
count_read=0 # количество считанных строк
count_write=0 # количество записей в бд
try:
    with zipfile.ZipFile(path) as archive:
        for filename in archive.namelist(): # получить список файлов в архиве
            with archive.open(filename) as files: # пройти по файлам архива
                json_files=json.load(files)
                data_prep=[] # очистить список
                for row in json_files: # пройти по записям файла
                    count_read+=1 # количество считанных строк
                    if row['data'].get('СвОКВЭД'): # если в записи присутствует ключ
                        if row['data']['СвОКВЭД'].get('СвОКВЭДОсн'): # если в записи присутствует ключ
                            okved=row['data']['СвОКВЭД']['СвОКВЭДОсн'] # считать код
                            if okved['КодОКВЭД'][:3]=='61.': # отобрать записи по коду 61
                                # добавить запись в список
                                data_prep.append((row['ogrn'],row['full_name'],row['inn'],okved['КодОКВЭД'],okved['НаимОКВЭД']))
                                count_write+=1 # количество записей в бд
                load_with_copy(data_prep) # добавить блок записей в базу
                print(f'обработан файл {files.name}')
            files.close
except zipfile.BadZipFile as error:
    print(f'Ошибка архива {error}')
finally:
    archive.close()
    print(f'считано {count_read} записей')
    print(f'записано {count_write} записей')
