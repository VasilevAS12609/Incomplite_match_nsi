import datetime

import pandas as pd
import sqlalchemy.types as sqal
import numpy as np
import sqlite3
import re
from tqdm import tqdm
from itertools import product
db = sqlite3.connect('DataBase1.db')
db2 = sqlite3.connect('DataBase2.db')
sql = db.cursor()
sql2 = db2.cursor()

# справочник плохих символов
bad_sym = {'b': 'в', 'c': 'с', 'h': 'н', 'p': 'р', 'y': 'у', 'a': 'а',
           'e': 'е', 'k': 'к', 'm': 'м', 'o': 'о', 't': 'т', 'x': 'х',
           'n': 'н', 'g': 'д', 'r': 'р', 's': 'с', 'd': 'д'}


def multi_rep(target_str, bad_sym_rep):
    # получаем заменяемое: подставляемое из словаря в цикле
    for i, j in bad_sym_rep.items():
        # меняем все target_str на подставляемое
        target_str = target_str.replace(i, j)
    return target_str


def ozm_process(text: str):
    try:
        text = multi_rep(text.lower(), bad_sym)
        text = str(set(re.findall(r'[a-zа-я]+|\d+', text)))
        return text
    except Exception:
        pass


def import_db(import_q):
    sku_list = []
    yes_list = ["да", "y"]
    if import_q.lower() in yes_list:
        print("Подождите, идет импортирование...")
        data_xlsx = pd.read_excel('Nsi.xlsx', index_col=0, dtype={'Статус_ОЗМ': str})
        data_xlsx.to_sql('Nsi', db, if_exists='replace')
        sku_base = sql.execute("SELECT * FROM Nsi")
        for sku in sku_base:
            sku_process_1 = ozm_process(sku[1])
            sku_process_2 = ozm_process(sku[2])
            sku_process_3 = ozm_process(sku[3])
            # Добавляем в лист обработанную базу SKU
            sku_list.append((sku[0],
                             sku[1],
                             sku[2],
                             sku[3],
                             sku[4],
                             sku[5],
                             sku[6],
                             sku[7],
                             sku_process_1,
                             sku_process_2,
                             sku_process_3,))
        df = pd.DataFrame(sku_list,
                          columns=['ОЗМ',
                                   'Наименование_краткое',
                                   'Наименование_полное',
                                   'Кат_номер',
                                   'ЕИ',
                                   'Статус',
                                   'ID_Аналог',
                                   'Дата_создания',
                                   'short_processed',
                                   'full_processed',
                                   'catalog_processed'])
        df['Дата_создания'] = df['Дата_создания'].astype("datetime64")
        df.to_sql('Nsi', db, if_exists='replace', index=False)
    pass


def read_input():
    date_1 = '1988-12-31 00:00:00'
    date_2 = '1989-01-12 00:00:00'
    nsi_base = sql.execute(
        "SELECT * "
        "FROM Nsi "
        "WHERE DATETIME(Дата_создания) BETWEEN ? AND ?",
        (date_1, date_2)
    )
    df = pd.DataFrame(nsi_base)
    df.to_sql('Input_OZM', db2, if_exists='replace')
    print(f'Выборка ОЗМ по датам = {len(df)} строк')


import_db('y')
read_input()
