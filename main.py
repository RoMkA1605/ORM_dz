import psycopg2


def create_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS customers (
                client_id SERIAL PRIMARY KEY,
                first_name VARCHAR(50) NOT NULL,
                last_name VARCHAR(50) NOT NULL,
                email VARCHAR(100) UNIQUE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS phones (
                phone_id SERIAL PRIMARY KEY,
                client_id INTEGER NOT NULL REFERENCES customers(client_id) ON DELETE CASCADE,
                phone_number VARCHAR(20) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT unique_client_phone UNIQUE (client_id, phone_number)
            );
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_phones_number ON phones(phone_number);
        """)

        conn.commit()
        print("✅ Таблицы успешно созданы")


def add_client(conn, first_name, last_name, email, phones=None):
    if '@' not in email or '.' not in email:
        print(f"❌ Некорректный email: {email}")
        return None

    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO customers (first_name, last_name, email)
                VALUES (%s, %s, %s)
                RETURNING client_id;
            """, (first_name.strip(), last_name.strip(), email.lower().strip()))

            client_id = cur.fetchone()[0]

            if phones:
                for phone in phones:
                    digits = ''.join([c for c in phone if c.isdigit()])
                    if len(digits) >= 5:
                        cur.execute("""
                            INSERT INTO phones (client_id, phone_number)
                            VALUES (%s, %s)
                            ON CONFLICT (client_id, phone_number) DO NOTHING;
                        """, (client_id, phone.strip()))
                    else:
                        print(f"⚠️ Некорректный телефон {phone} пропущен")

            conn.commit()
            print(f"✅ Клиент {first_name} {last_name} добавлен с ID: {client_id}")
            return client_id

    except psycopg2.IntegrityError as e:
        conn.rollback()
        if "customers_email_key" in str(e):
            print(f"❌ Клиент с email '{email}' уже существует")
        else:
            print(f"❌ Ошибка целостности данных: {e}")
        return None
    except Exception as e:
        conn.rollback()
        print(f"❌ Ошибка при добавлении клиента: {e}")
        return None


def add_phone(conn, client_id, phone):
    digits = ''.join([c for c in phone if c.isdigit()])
    if len(digits) < 5:
        print(f"❌ Некорректный номер телефона: {phone}")
        return False

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT client_id FROM customers WHERE client_id = %s;", (client_id,))
            if not cur.fetchone():
                print(f"❌ Клиент с ID {client_id} не найден")
                return False

            cur.execute("""
                INSERT INTO phones (client_id, phone_number)
                VALUES (%s, %s)
                ON CONFLICT (client_id, phone_number) DO NOTHING;
            """, (client_id, phone.strip()))

            if cur.rowcount == 0:
                print(f"⚠️ Телефон {phone} уже есть у клиента ID {client_id}")
                return False

            conn.commit()
            print(f"✅ Телефон {phone} добавлен клиенту ID {client_id}")
            return True

    except Exception as e:
        conn.rollback()
        print(f"❌ Ошибка при добавлении телефона: {e}")
        return False


def change_client(conn, client_id, first_name=None, last_name=None, email=None, phones=None):
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM customers WHERE client_id = %s;", (client_id,))
            client = cur.fetchone()
            if not client:
                print(f"❌ Клиент с ID {client_id} не найден")
                return False

            updates = []
            params = []

            if first_name is not None:
                updates.append("first_name = %s")
                params.append(first_name.strip())

            if last_name is not None:
                updates.append("last_name = %s")
                params.append(last_name.strip())

            if email is not None:
                if '@' not in email or '.' not in email:
                    print(f"❌ Некорректный email: {email}")
                    return False
                updates.append("email = %s")
                params.append(email.lower().strip())

            if updates:
                params.append(client_id)
                cur.execute(f"""
                    UPDATE customers 
                    SET {', '.join(updates)}
                    WHERE client_id = %s;
                """, params)

            if phones is not None:
                cur.execute("DELETE FROM phones WHERE client_id = %s;", (client_id,))

                for phone in phones:
                    digits = ''.join([c for c in phone if c.isdigit()])
                    if len(digits) >= 5:
                        cur.execute("""
                            INSERT INTO phones (client_id, phone_number)
                            VALUES (%s, %s);
                        """, (client_id, phone.strip()))
                    else:
                        print(f"⚠️ Некорректный телефон {phone} пропущен")

            conn.commit()
            print(f"✅ Данные клиента ID {client_id} обновлены")
            return True

    except psycopg2.IntegrityError as e:
        conn.rollback()
        if "customers_email_key" in str(e):
            print(f"❌ Email '{email}' уже используется другим клиентом")
        else:
            print(f"❌ Ошибка целостности данных: {e}")
        return False
    except Exception as e:
        conn.rollback()
        print(f"❌ Ошибка при обновлении клиента: {e}")
        return False


def delete_phone(conn, client_id, phone):
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT client_id FROM customers WHERE client_id = %s;", (client_id,))
            if not cur.fetchone():
                print(f"❌ Клиент с ID {client_id} не найден")
                return False

            cur.execute("""
                DELETE FROM phones 
                WHERE client_id = %s AND phone_number = %s
                RETURNING phone_id;
            """, (client_id, phone.strip()))

            if cur.fetchone():
                conn.commit()
                print(f"✅ Телефон {phone} удален у клиента ID {client_id}")
                return True
            else:
                print(f"❌ Телефон {phone} не найден у клиента ID {client_id}")
                return False

    except Exception as e:
        conn.rollback()
        print(f"❌ Ошибка при удалении телефона: {e}")
        return False


def delete_client(conn, client_id):
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT first_name, last_name, email 
                FROM customers WHERE client_id = %s;
            """, (client_id,))
            client = cur.fetchone()

            if not client:
                print(f"❌ Клиент с ID {client_id} не найден")
                return False

            cur.execute("DELETE FROM customers WHERE client_id = %s;", (client_id,))
            conn.commit()

            print(f"✅ Клиент {client[0]} {client[1]} (ID {client_id}) удален")
            return True

    except Exception as e:
        conn.rollback()
        print(f"❌ Ошибка при удалении клиента: {e}")
        return False


def find_client(conn, first_name=None, last_name=None, email=None, phone=None):
    try:
        with conn.cursor() as cur:
            query = """
                SELECT 
                    c.client_id,
                    c.first_name,
                    c.last_name,
                    c.email,
                    p.phone_number
                FROM customers c
                LEFT JOIN phones p ON c.client_id = p.client_id
            """

            conditions = []
            params = []

            if first_name:
                conditions.append("c.first_name ILIKE %s")
                params.append(f"%{first_name}%")

            if last_name:
                conditions.append("c.last_name ILIKE %s")
                params.append(f"%{last_name}%")

            if email:
                conditions.append("c.email ILIKE %s")
                params.append(f"%{email}%")

            if phone:
                conditions.append("""
                    c.client_id IN (
                        SELECT client_id FROM phones 
                        WHERE phone_number ILIKE %s
                    )
                """)
                params.append(f"%{phone}%")

            if conditions:
                query += " WHERE " + " AND ".join(conditions)

            query += " ORDER BY c.last_name, c.first_name, p.phone_id;"

            cur.execute(query, params)
            results = cur.fetchall()

            clients_dict = {}
            for row in results:
                client_id = row[0]
                if client_id not in clients_dict:
                    clients_dict[client_id] = {
                        'client_id': client_id,
                        'first_name': row[1],
                        'last_name': row[2],
                        'email': row[3],
                        'phones': []
                    }
                if row[4]:
                    clients_dict[client_id]['phones'].append(row[4])

            clients = list(clients_dict.values())

            print(f"🔍 Найдено клиентов: {len(clients)}")
            return clients

    except Exception as e:
        print(f"❌ Ошибка при поиске клиентов: {e}")
        return []


with psycopg2.connect(database="clients_db", user="postgres", password="postgres") as conn:
    pass  # вызывайте функции здесь