import src.DBManager
import src.utils_hh
from src.vacancyManager import VacancyManager
from  src.config import load_config


def show_menu():
    """
    Показывает меню с доступными опциями.
    """
    print("""
    1. Показать компании и количество вакансий
    2. Показать все вакансии
    3. Показать среднюю зарплату
    4. Показать вакансии с зарплатой выше средней
    5. Показать вакансии с ключевым словом
    0. Выход
    """)


def menu(db_manager):
    """
    Основное меню программы. Позволяет пользователю выбрать опцию и выполнить соответствующее действие.
    """
    while True:
        show_menu()
        choice = input("Выберите опцию: ")

        if choice == "1":
            # Показать компании и количество вакансий
            companies = db_manager.get_companies_and_vacancies_count()
            if companies:
                print("Компании и количество вакансий:")
                for company in companies:
                    print(f"Компания: {company[0]}, Количество вакансий: {company[1]}")
            else:
                print("Нет данных о компаниях.")

        elif choice == "2":
            # Показать все вакансии
            vacancies = db_manager.get_all_vacancies()
            if vacancies:
                print("Все вакансии:")
                for vacancy in vacancies:
                    print(f"Вакансия: {vacancy[0]}, Компания: {vacancy[1]}, Зарплата от: {vacancy[2]}, до: {vacancy[3]}")
            else:
                print("Вакансии отсутствуют.")

        elif choice == "3":
            # Показать среднюю зарплату
            avg_salary = db_manager.get_avg_salary()
            if avg_salary is not None:
                print(f"Средняя зарплата: {avg_salary}")
            else:
                print("Нет данных о зарплатах.")

        elif choice == "4":
            # Показать вакансии с зарплатой выше средней
            vacancies = db_manager.get_vacancies_with_higher_salary()
            if vacancies:
                print("Вакансии с зарплатой выше средней:")
                for vacancy in vacancies:
                    print(f"Вакансия: {vacancy[0]}, Зарплата от: {vacancy[1]}, до: {vacancy[2]}")
            else:
                print("Нет вакансий с зарплатой выше средней.")

        elif choice == "5":
            # Показать вакансии с ключевым словом
            keyword = input("Введите ключевое слово для поиска вакансий: ")
            vacancies = db_manager.get_vacancies_with_keyword(keyword)
            if vacancies:
                print(f"Вакансии, содержащие '{keyword}':")
                for vacancy in vacancies:
                    print(f"Вакансия: {vacancy[0]}")
            else:
                print(f"Вакансии с ключевым словом '{keyword}' не найдены.")

        elif choice == "0":
            # Выход из программы
            print("Выход из программы.")
            break

        else:
            print("Неверный выбор. Попробуйте снова.")


config_data = load_config('database.ini')

# Ввод данных для подключения к базе данных
dbname = config_data['database']
user = config_data['user']
password = config_data['password']
host = config_data['host']
port = config_data['port']

# Создание объекта DBManager с указанными параметрами
db_manager = src.DBManager.DBManager(dbname, user, password, host, port)

# Ввод имени для новой базы данных и создание базы данных
new_db_name = input("Введите имя для новой базы данных: ")
db_manager.create_database(new_db_name)

# Ввод имени для новой схемы и создание таблиц в этой схеме
new_schema_name = input("Введите имя для новой схемы: ")
db_manager.create_tables(new_schema_name)

# Создаем менеджер вакансий, передавая db_manager
vacancy_manager = VacancyManager(db_manager)

# Получаем случайные вакансии
vacancies = vacancy_manager.get_random_vacancies(count=30)

# Извлекаем уникальных работодателей
employers = vacancy_manager.extract_unique_employers(vacancies)

# Интерактивное добавление компаний в базу данных
vacancy_manager.add_employers_interactive(employers)

# Добавляем вакансии для всех компаний, которые уже есть в базе данных
vacancy_manager.add_vacancies_for_all_companies()

# Запуск основного меню программы
menu(db_manager)
