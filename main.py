import json
import sqlalchemy as sq
from sqlalchemy.orm import sessionmaker
import pandas as pd

from models import create_tables, Publisher, Book, Shop, Sale, Stock


class Filling:

    def __init__(self):
        self.publisher = []
        self.book = []
        self.shop = []
        self.stock = []
        self.sale = []

    def json_separator(self, json_file_name):
        """Разделяет JSON-файл на части по условию.

        Каждый элемент JSON-файла добавляется в отдельный список
        для дальнейшей загрузки в конкретную таблицу.
        :param json_file_name: относительный путь к JSON-файлу
        """
        with open(json_file_name, 'r') as fd:
            data = json.load(fd)
            for element in data:
                if element['model'] == 'publisher':
                    self.publisher.append(element)
                elif element['model'] == 'book':
                    self.book.append(element)
                elif element['model'] == 'stock':
                    self.stock.append(element)
                elif element['model'] == 'shop':
                    self.shop.append(element)
                else:
                    self.sale.append(element)

    def add_values_to_tabs(self):
        """Добавляет данные в таблицы с помощью библиотеки pandas.

        Данные из заранее сформированных при помощи json_separator
        списков добавляются в конкретные таблицы SQL.
        """
        structured_list = ([self.publisher, 'publisher', ['id_publisher', 'fields.publisher_name']],
                           [self.book, 'book', ['id_book', 'fields.title', 'fields.id_publisher']],
                           [self.shop, 'shop', ['id_shop', 'fields.shop_name']],
                           [self.stock, 'stock',
                            ['id_stock', 'fields.id_shop', 'fields.id_book', 'fields.count_stock']],
                           [self.sale, 'sale', ['id_sale', 'fields.price', 'fields.date_sale', 'fields.id_stock',
                                                'fields.count_sale']])
        for list_of_data, name_of_table, stucture_of_table in structured_list:
            table = pd.DataFrame(pd.json_normalize(list_of_data), columns=stucture_of_table)
            table.columns = table.columns.str.replace('fields.', '', regex=True)
            table.to_sql(name_of_table, con=engine, if_exists='append', index=False)


if __name__ == '__main__':

    # подключение базы данных
    DSN = f'postgresql://{input("Укажите лониг postgres: ")}:{input("Укажите пароль postgres: ")}' \
          f'@localhost:5432/{input("Укажите название базы данных: ")}'
    engine = sq.create_engine(DSN)

    # создание таблиц
    create_tables(engine)

    # наполнение таблиц
    filling_tabs = Filling()
    filling_tabs.json_separator('tests_data.json')
    filling_tabs.add_values_to_tabs()

    Session = sessionmaker(bind=engine)
    session = Session()

    # запросы
    found_publisher = input('Ввидетие id издателя: ')
    for c in session.query(Publisher).filter(Publisher.id_publisher == found_publisher).all():
        print(c)
    for c in session.query(Shop).join(Stock.shop).join(Stock.book).join(Book.publisher).filter(Publisher.id_publisher ==
                                                                                               found_publisher).all():
        print(c)

    session.close()
