import datetime

import pandas as pd
import sqlalchemy as sqal
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
        data_xlsx = pd.read_excel('Nsi.xlsx', index_col=0, dtype={'Дата_создания': datetime.date, 'Статус_ОЗМ': str})
        print(data_xlsx)
        data_xlsx.to_sql('Nsi', db, if_exists='replace', index=True, dtype={'Дата_создания': sqal.DATE})
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
        df.to_sql('Nsi', db, if_exists='replace', index=False)
    pass


def read_input():
    input_list = []
    nsi_base = sql.execute("SELECT * FROM Nsi")
    nsi_base_analog = sql.execute("SELECT * FROM Nsi WHERE ID_Аналог IS NOT NULL")
    for input in nsi_base:
        ozm_proc = ozm_process(input[1])
        id_analog = ""
        if input[0] != None:
            for i in nsi_base:
                if input[0] == i[0]:
                    id_analog += str(i[6])
                    break
        input_list.append((
            input[0],
            input[1],
            id_analog,
            ozm_proc
                           ))
    sql2.execute("DROP TABLE Input_OZM")
    sql2.execute("CREATE TABLE Input_OZM (Keys INTEGER, Что_ищем TEXT, ID_Аналога REAL, Обработанный_текст TEXT)")
    sql2.executemany("INSERT INTO Input_OZM VALUES (?, ?, ?, ?)", input_list)
    db2.commit()


def doubles_search():
    print("Идет поиск совпадений, ожидайте сообщение об окончании...")
    result_list = []
    sql2.execute("SELECT * FROM Input_OZM")
    input_rows_count = []
    for rows in sql2:
        input_rows_count.append(rows)
    nsi_base = sql.execute("SELECT * FROM Nsi")
    ozm_input = sql2.execute("SELECT * FROM Input_OZM")
    with tqdm(total=len(sku_rows_count) * len(input_rows_count),
              unit_scale=(1 / len(sku_rows_count)),
              unit='row',
              colour='green',
              leave=False,
              ncols=90,
              bar_format="{l_bar} {bar} | строки: {n:.0f}/{total:.0f} | время: [{elapsed} < {remaining} = {eta:%H:%M:%S}] | {rate_fmt}{postfix}") as pbar:
        for insert_ozm, s_ozm in product(ozm_input, nsi_base):
            if insert_ozm[0] != s_ozm[0]:
                try:
                    insert_ozm_process = set(insert_ozm[3].replace(",", "").replace("'", "").strip("{}").split())  # ОЗМ входная обработанная
                # Обрабатываем краткое наименование ОЗМ
                    s_ozm_process_1 = s_ozm[7].replace(",", "").replace("'", "").strip("{}").split() # ОЗМ из базы обработанная
                    s_ozm_inter_1 = insert_ozm_process.intersection(s_ozm_process_1)  # выводим совпадения слов
                    s_ozm_percent_1 = len(s_ozm_inter_1) / len(insert_ozm_process)  # вычисляем процент совпадения
                except AttributeError:
                    s_ozm_percent_1 = 0
                except ZeroDivisionError:
                    s_ozm_percent_1 = 0
                # Обрабатываем полное наименование ОЗМ
                try:
                    s_ozm_process_2 = s_ozm[8].replace(",", "").replace("'", "").strip("{}").split()  # ОЗМ из базы обработанная
                    s_ozm_inter_2 = insert_ozm_process.intersection(s_ozm_process_2)  # выводим совпадения слов
                    s_ozm_percent_2 = len(s_ozm_inter_2) / len(insert_ozm_process)  # вычисляем процент совпадения
                except AttributeError:
                    s_ozm_percent_2 = 0
                except ZeroDivisionError:
                    s_ozm_percent_2 = 0
                # Обрабатываем каталожный номер ОЗМ
                try:
                    s_ozm_process_3 = s_ozm[9].replace(",", "").replace("'", "").strip("{}").split()  # ОЗМ из базы обработанная
                    s_ozm_inter_3 = insert_ozm_process.intersection(s_ozm_process_3)  # выводим совпадения слов
                    s_ozm_percent_3 = len(s_ozm_inter_3) / len(insert_ozm_process)  # вычисляем процент совпадения
                except AttributeError:
                    s_ozm_percent_3 = 0
                except ZeroDivisionError:
                    s_ozm_percent_3 = 0
                max_percent = max(s_ozm_percent_1, s_ozm_percent_2, s_ozm_percent_3)
                if max_percent >= show_percent:
                    result_list.append(('{:.0%}'.format(max_percent),
                                        insert_ozm[0],
                                        insert_ozm[1],
                                        insert_ozm[2],
                                        s_ozm[0],
                                        s_ozm[1],
                                        s_ozm[2],
                                        s_ozm[3],
                                        s_ozm[4],
                                        s_ozm[5],
                                        s_ozm[6],
                                        ))
                pbar.update(1)
        return result_list


#  Стартовый запрос
print("Импортировать Excel в базу?")
print("Введите: Да/Нет (y/n)")
try:
    import_db(input())
    sku_rows_count = []
    sql.execute("SELECT ОЗМ FROM Nsi")
    for rows in sql:
        sku_rows_count.append(rows)
    print(f'Строк в базе данных: {len(sku_rows_count)}')
except Exception as error:
    print('')
    print(error)
    print('')


while True:
    print("Введите % соответствия для вывода результата от 0 до 100:")
    show_percent = int(input()) / 100
    read_input()
    try:
        df = pd.DataFrame(doubles_search(),
                          columns=['%_соответствия',
                                   'ОЗМ_искомый',
                                   'Искомый_материал',
                                   'ID_Аналога_исходный',
                                   'ОЗМ',
                                   'Наименование_краткое',
                                   'Наименование_полное',
                                   'Кат_номер',
                                   'ЕИ',
                                   'Статус',
                                   'ID_Аналог'])

        with pd.ExcelWriter('Data_Output.xlsx') as data_output:
            df.to_excel(data_output, index_label='№ п/п')
        print(f'Готово, строк подобрано: {len(df)}, откройте файл - Data_Output.xlsx')
        print("")
    except PermissionError:
        print('')
        print('ОШИБКА - Закройте файл Data_Output.xlsx')
        print('')
    except IndexError:
        print('')
        print('ОШИБКА - количество строк на вывод более 1млн., проверьте Input_ozm.xlsx')
        print('')
    except Exception as error:
        print('')
        print(error)
        print('')
