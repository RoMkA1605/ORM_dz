import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker
from models import Publisher, Shop, Book, Stock, Sale

DSN = 'postgresql://username:password@localhost:5432/db_name'
engine = sa.create_engine(DSN)
Session = sessionmaker(bind=engine)
session = Session()


def get_sales_by_publisher():
    publisher_input = input("Введите имя или ID издателя: ").strip()

    if publisher_input.isdigit():
        publisher = session.get(Publisher, int(publisher_input))
        if not publisher:
            print(f"Издатель с ID {publisher_input} не найден")
            return
        publisher_filter = Publisher.id == int(publisher_input)
        publisher_name = publisher.name
    else:
        publisher = session.query(Publisher).filter(
            Publisher.name.ilike(f'%{publisher_input}%')
        ).first()

        if not publisher:
            print(f"Издатель '{publisher_input}' не найден")
            return
        publisher_filter = Publisher.id == publisher.id
        publisher_name = publisher.name

    results = (
        session.query(
            Book.title,
            Shop.name.label('shop_name'),
            Sale.price,
            Sale.date_sale
        )
        .join(Stock, Stock.id_book == Book.id)
        .join(Shop, Shop.id == Stock.id_shop)
        .join(Sale, Sale.id_stock == Stock.id)
        .filter(Book.id_publisher == publisher.id)
        .order_by(Sale.date_sale.desc())
        .all()
    )

    if results:
        print(f"\nПродажи книг издателя '{publisher_name}':")
        print("-" * 60)
        for title, shop_name, price, date_sale in results:
            formatted_date = date_sale.strftime('%d-%m-%Y')
            print(f"{title} | {shop_name} | {price} | {formatted_date}")
    else:
        print(f"Нет продаж книг издателя '{publisher_name}'")

    session.close()


if __name__ == "__main__":
    get_sales_by_publisher()