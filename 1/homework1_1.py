import pandas as pd
import sqlite3
import os.path
file='okved_2.json'
if os.path.exists(file):
    company=pd.read_json(file)
    connection=sqlite3.connect('hw1.db')
    company.to_sql('okved',connection,if_exists='append',index=False)
else:
    print(f'Загрузите файл {file} в текущий каталог')
