import json
import sqlalchemy as sq
from sqlalchemy.orm import sessionmaker
import pandas as pd

from models import create_tables, Publisher, Book, Shop, Sale, Stock


# подключение базы данных
DSN = f'postgresql://{input("Укажите лониг postgres: ")}:{input("Укажите пароль postgres: ")}@localhost:5432/{input("Укажите название базы данных: ")}'
engine = sq.create_engine(DSN)

# создание таблиц
create_tables(engine)

# наполнение БД из JSON
publisher = []
book = []
shop = []
stock = []
sale = []

with open('tests_data.json', 'r') as fd:
    data = json.load(fd)
    for element in data:
        if element['model'] == 'publisher':
            publisher.append(element)
        elif element['model'] == 'book':
            book.append(element)
        elif element['model'] == 'stock':
            stock.append(element)
        elif element['model'] == 'shop':
            shop.append(element)
        else:
            sale.append(element)

# ввод данных в таблицу publisher
publisher_add = pd.DataFrame(pd.json_normalize(publisher), columns=['id_publisher', 'fields.publisher_name'])
publisher_add.columns = publisher_add.columns.str.replace('fields.', '', regex=True)
publisher_add.to_sql('publisher', con=engine, if_exists='append', index=False)

# ввод данных в таблицу publisher book
book_add = pd.DataFrame(pd.json_normalize(book), columns=['id_book', 'fields.title', 'fields.id_publisher'])
book_add.columns = book_add.columns.str.replace('fields.', '', regex=True)
book_add.to_sql('book', con=engine, if_exists='append', index=False)

# ввод данных в таблицу publisher shop
shop_add = pd.DataFrame(pd.json_normalize(shop), columns=['id_shop', 'fields.shop_name'])
shop_add.columns = shop_add.columns.str.replace('fields.', '', regex=True)
shop_add.to_sql('shop', con=engine, if_exists='append', index=False)

# ввод данных в таблицу publisher stock
stock_add = pd.DataFrame(pd.json_normalize(stock),
                         columns=['id_stock', 'fields.id_shop', 'fields.id_book', 'fields.count_stock'])
stock_add.columns = stock_add.columns.str.replace('fields.', '', regex=True)
stock_add.to_sql('stock', con=engine, if_exists='append', index=False)

# ввод данных в таблицу publisher sale
sale_add = pd.DataFrame(pd.json_normalize(sale),
                        columns=['id_sale', 'fields.price', 'fields.date_sale', 'fields.id_stock', 'fields.count_sale'])
sale_add.columns = sale_add.columns.str.replace('fields.', '', regex=True)
sale_add.to_sql('sale', con=engine, if_exists='append', index=False)


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
