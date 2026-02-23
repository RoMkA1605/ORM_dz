import json
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from models import create_tables, Publisher, Shop, Book, Stock, Sale
from config import get_dsn
from datetime import datetime


def load_data_with_validation(json_file_path, session):

    def validate_record(model_name, fields):
        if model_name == 'book' and 'id_publisher' in fields:
            if not session.get(Publisher, fields['id_publisher']):
                raise ValueError(f"Publisher with id {fields['id_publisher']} not found")

        elif model_name == 'stock':
            if 'id_book' in fields and not session.get(Book, fields['id_book']):
                raise ValueError(f"Book with id {fields['id_book']} not found")
            if 'id_shop' in fields and not session.get(Shop, fields['id_shop']):
                raise ValueError(f"Shop with id {fields['id_shop']} not found")

        elif model_name == 'sale':
            if 'id_stock' in fields and not session.get(Stock, fields['id_stock']):
                raise ValueError(f"Stock with id {fields['id_stock']} not found")

            if 'date_sale' in fields and isinstance(fields['date_sale'], str):
                fields['date_sale'] = datetime.strptime(fields['date_sale'], '%Y-%m-%d').date()

    model_mapping = {
        'publisher': Publisher,
        'shop': Shop,
        'book': Book,
        'stock': Stock,
        'sale': Sale,
    }

    try:
        with open(json_file_path, 'r', encoding='utf-8') as fd:
            data = json.load(fd)

        for record in sorted(data, key=lambda x: {
            'publisher': 1,
            'shop': 1,
            'book': 2,
            'stock': 3,
            'sale': 4
        }.get(x.get('model'), 5)):

            model_name = record.get('model')
            model_class = model_mapping.get(model_name)

            if model_class:
                fields = record.get('fields', {}).copy()

                validate_record(model_name, fields)

                model_instance = model_class(
                    id=record.get('pk'),
                    **fields
                )
                session.add(model_instance)
                session.flush()
                print(f"✓ Добавлен {model_name} с id={record.get('pk')}")

        session.commit()
        print(f"\n✅ Все данные успешно загружены из {json_file_path}")

    except Exception as e:
        session.rollback()
        print(f"❌ Ошибка: {e}")
        raise


if __name__ == "__main__":
    DSN = get_dsn()
    engine = sqlalchemy.create_engine(DSN)

    create_tables(engine)

    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        load_data_with_validation('fixtures/tests_data.json', session)

        print("\n📊 Статистика связей:")
        for publisher in session.query(Publisher).all():
            print(f"\nИздатель: {publisher.name}")
            for book in publisher.books:
                print(f"  Книга: {book.title}")
                for stock in book.stocks:
                    print(f"    В магазине {stock.shop.name}: {stock.count} шт.")
                    for sale in stock.sales:
                        print(f"      Продажа: {sale.price} руб., {sale.date_sale}")

    finally:
        session.close()