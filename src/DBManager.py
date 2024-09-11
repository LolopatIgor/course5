import psycopg2
from psycopg2 import sql
from src.utils import convert_salary


class DBManager:
    def __init__(self, dbname, user, password, host='localhost', port=5432):
        """
        Инициализация объекта DBManager.

        :param dbname: Имя базы данных.
        :param user: Имя пользователя базы данных.
        :param password: Пароль пользователя базы данных.
        :param host: Хост базы данных (по умолчанию 'localhost').
        :param port: Порт базы данных (по умолчанию 5432).
        """
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.conn = None
        self.cur = None
        self.schema_name = ""

    def connect(self, dbname=None):
        """
        Устанавливает соединение с базой данных.

        :param dbname: Имя базы данных (если не указано, используется текущее имя).
        """
        if dbname is None:
            dbname = self.dbname
        self.conn = psycopg2.connect(
            dbname=dbname,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port
        )
        self.cur = self.conn.cursor()

    def close(self):
        """Закрывает соединение и курсор с базой данных."""
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()

    def create_database(self, new_db_name):
        """
        Создает новую базу данных.

        :param new_db_name: Имя новой базы данных.
        """
        # Закрываем текущее соединение
        if self.conn:
            self.close()

        # Подключаемся к базе данных по умолчанию (обычно 'postgres')
        conn = psycopg2.connect(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port
        )
        conn.autocommit = True  # Отключаем транзакции для создания базы данных
        cur = conn.cursor()

        try:
            # Создаем новую базу данных
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(new_db_name)))
            print(f"База данных '{new_db_name}' успешно создана.")
        except Exception as e:
            print(f"Ошибка при создании базы данных: {e}")
        finally:
            cur.close()
            conn.close()

        # Обновляем имя базы данных и повторно подключаемся
        self.dbname = new_db_name
        self.connect()

    def create_tables(self, new_schema_name):
        """
        Создает таблицы в указанной схеме.

        :param new_schema_name: Имя новой схемы.
        """
        self.connect()  # Подключаемся к текущей базе данных

        try:
            # Создание схемы, если она не существует
            self.cur.execute(f"""
                CREATE SCHEMA IF NOT EXISTS "{new_schema_name}";
            """)
            self.schema_name = new_schema_name

            # Создание таблицы компаний
            self.cur.execute(f"""
                CREATE TABLE IF NOT EXISTS "{self.schema_name}"."COMPANY" (
                    "COMPANY_ID" SERIAL PRIMARY KEY,
                    "NAME" VARCHAR(255) NOT NULL
                );
            """)

            # Создание таблицы вакансий
            self.cur.execute(f"""
                CREATE TABLE IF NOT EXISTS "{self.schema_name}"."VACANCY" (
                    "VACANCY_ID" SERIAL PRIMARY KEY,
                    "NAME" VARCHAR(255) NOT NULL,
                    "SALARY_FROM" DECIMAL,
                    "SALARY_TO" DECIMAL,
                    "COMPANY_ID" INTEGER REFERENCES "{self.schema_name}"."COMPANY"("COMPANY_ID"),
                    "REQUIREMENT" TEXT,
                    "LOCATION" VARCHAR(255)
                );
            """)

            self.conn.commit()
            print("Таблицы успешно созданы.")
        except psycopg2.Error as e:
            print(f"Ошибка при создании таблиц: {e}")
            self.conn.rollback()
        finally:
            self.close()

    def insert_company(self, name, item_id):
        """
        Вставляет новую компанию в базу данных.

        :param name: Название компании.
        :param item_id: ID компании.
        :return: ID добавленной компании.
        """
        self.connect()
        try:
            self.cur.execute(
                f'INSERT INTO "{self.schema_name}"."COMPANY" ("NAME", "COMPANY_ID") VALUES (%s, %s) RETURNING "COMPANY_ID";',
                (name, item_id))
            company_id = self.cur.fetchone()[0]
            self.conn.commit()
            return company_id
        except psycopg2.Error as e:
            print(f"Ошибка при добавлении компании '{name}': {e}")
        finally:
            self.close()

    def insert_vacancy(self, name, salary_from, salary_to, company_id, requirement, location):
        """
        Вставляет новую вакансию в базу данных.

        :param name: Название вакансии.
        :param salary_from: Минимальная зарплата.
        :param salary_to: Максимальная зарплата.
        :param company_id: ID компании.
        :param requirement: Требования к вакансии.
        :param location: Местоположение вакансии.
        """
        self.connect()
        try:
            self.cur.execute(
                f"""
                INSERT INTO "{self.schema_name}"."VACANCY" ("NAME", "SALARY_FROM", "SALARY_TO", "COMPANY_ID", "REQUIREMENT", "LOCATION")
                VALUES (%s, %s, %s, %s, %s, %s);
                """,
                (name, salary_from, salary_to, company_id, requirement, location)
            )
            self.conn.commit()
        except psycopg2.Error as e:
            print(f"Ошибка при добавлении вакансии '{name}': {e}")
        finally:
            self.close()

    def clear_companies(self):
        """
        Очищает таблицу компаний.
        """
        self.connect()
        try:
            self.cur.execute(f'TRUNCATE "{self.schema_name}"."COMPANY" CASCADE;')
            self.conn.commit()
        except psycopg2.Error as e:
            print(f"Ошибка при очистке таблицы компаний: {e}")
            self.conn.rollback()
        finally:
            self.close()

    def insert_vacancy_from_json(self, vacancy_json):
        """
        Вставляет вакансию в базу данных из JSON объекта.

        :param vacancy_json: Объект вакансии в формате JSON.
        """
        if not vacancy_json:
            print("Ошибка: передан пустой объект вакансии")
            return

        name = vacancy_json.get("name", "Не указано")

        # Обработка зарплаты (зарплата может быть None)
        salary = vacancy_json.get("salary", {})
        salary_from = None
        salary_to = None
        if salary:
            salary_from = convert_salary(salary.get("from"), salary.get("currency"))
            salary_to = convert_salary(salary.get("to"), salary.get("currency"))

        # Проверка на наличие данных о компании
        employer = vacancy_json.get("employer", {})
        company_id = employer.get("id")
        if not company_id:
            print(f"Ошибка: Не удалось получить ID компании для вакансии '{name}'")
            return

        # Требования могут отсутствовать
        snippet = vacancy_json.get("snippet", {})
        requirement = snippet.get("requirement", "Не указано")

        # Локация может быть не указана
        area = vacancy_json.get("area", {})
        location = area.get("name", "Не указано")

        # Вызов функции insert_vacancy с полученными данными
        try:
            self.insert_vacancy(
                name=name,
                salary_from=salary_from,
                salary_to=salary_to,
                company_id=company_id,
                requirement=requirement,
                location=location
            )
        except Exception as e:
            print(f"Ошибка при добавлении вакансии '{name}': {e}")

    def company_exists(self, company_id):
        """
        Проверяет, существует ли компания с данным ID в базе данных.

        :param company_id: ID компании.
        :return: True, если компания существует, иначе False.
        """
        self.connect()
        try:
            self.cur.execute(f'SELECT 1 FROM "{self.schema_name}"."COMPANY" WHERE "COMPANY_ID" = %s', (company_id,))
            return self.cur.fetchone() is not None
        except psycopg2.Error as e:
            print(f"Ошибка при проверке существования компании: {e}")
            return False
        finally:
            self.close()

    def get_all_companies(self):
        """
        Получает список всех компаний из таблицы COMPANY.

        :return: Список кортежей (company_id, name).
        """
        self.connect()
        try:
            self.cur.execute(f'SELECT "COMPANY_ID", "NAME" FROM "{self.schema_name}"."COMPANY";')
            return self.cur.fetchall()
        except psycopg2.Error as e:
            print(f"Ошибка при получении всех компаний: {e}")
            return []
        finally:
            self.close()

    def get_companies_and_vacancies_count(self):
        """
        Получает количество вакансий для каждой компании.

        :return: Список кортежей (название компании, количество вакансий).
        """
        self.connect()
        try:
            self.cur.execute(f'''
                SELECT C."NAME", COUNT(V."VACANCY_ID") AS "VACANCIES_COUNT"
                FROM "{self.schema_name}"."COMPANY" C
                LEFT JOIN "{self.schema_name}"."VACANCY" V ON C."COMPANY_ID" = V."COMPANY_ID"
                GROUP BY C."NAME";
            ''')
            return self.cur.fetchall()
        except psycopg2.Error as e:
            print(f"Ошибка при получении количества вакансий для компаний: {e}")
            return []
        finally:
            self.close()

    def get_all_vacancies(self):
        """
        Получает все вакансии из базы данных.

        :return: Список кортежей (название вакансии, название компании, зарплата от, зарплата до).
        """
        self.connect()
        try:
            self.cur.execute(f"""
                SELECT V."NAME", C."NAME" AS "COMPANY_NAME", V."SALARY_FROM", V."SALARY_TO"
                FROM "{self.schema_name}"."VACANCY" V
                JOIN "{self.schema_name}"."COMPANY" C ON V."COMPANY_ID" = C."COMPANY_ID";
            """)
            return self.cur.fetchall()
        except psycopg2.Error as e:
            print(f"Ошибка при получении всех вакансий: {e}")
            return []
        finally:
            self.close()

    def get_avg_salary(self):
        """
        Получает среднюю зарплату по вакансиям.

        :return: Средняя зарплата или None в случае ошибки.
        """
        self.connect()
        try:
            self.cur.execute(f"""
                SELECT AVG((V."SALARY_FROM" + V."SALARY_TO") / 2)
                FROM "{self.schema_name}"."VACANCY" V
                WHERE V."SALARY_FROM" IS NOT NULL AND V."SALARY_TO" IS NOT NULL;
            """)
            return self.cur.fetchone()[0]
        except psycopg2.Error as e:
            print(f"Ошибка при получении средней зарплаты: {e}")
            return None
        finally:
            self.close()

    def get_vacancies_with_higher_salary(self):
        """
        Получает вакансии с зарплатой выше средней.

        :return: Список кортежей (название вакансии, зарплата от, зарплата до).
        """
        avg_salary = self.get_avg_salary()
        if avg_salary is None:
            return []

        self.connect()
        try:
            self.cur.execute(f"""
                SELECT V."NAME", V."SALARY_FROM", V."SALARY_TO"
                FROM "{self.schema_name}"."VACANCY" V
                WHERE V."SALARY_FROM" > %s OR V."SALARY_TO" > %s;
            """, (avg_salary, avg_salary))
            return self.cur.fetchall()
        except psycopg2.Error as e:
            print(f"Ошибка при получении вакансий с зарплатой выше средней: {e}")
            return []
        finally:
            self.close()

    def get_vacancies_with_keyword(self, keyword):
        """
        Получает вакансии, содержащие указанное ключевое слово.

        :param keyword: Ключевое слово для поиска.
        :return: Список кортежей (название вакансии).
        """
        self.connect()
        try:
            self.cur.execute(f"""
                SELECT V."NAME"
                FROM "{self.schema_name}"."VACANCY" V
                WHERE V."NAME" ILIKE %s;
            """, ('%' + keyword + '%',))
            return self.cur.fetchall()
        except psycopg2.Error as e:
            print(f"Ошибка при получении вакансий с ключевым словом '{keyword}': {e}")
            return []
        finally:
            self.close()
