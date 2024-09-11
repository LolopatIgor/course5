import requests
import random


class VacancyManager:
    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.base_url = "https://api.hh.ru/vacancies"

    def get_random_vacancies(self, count=10):
        """
        Получает случайные вакансии с HH.ru.
        Возвращает список вакансий.
        """
        params = {
            'per_page': count,  # количество вакансий на страницу
            'page': random.randint(0, 100),  # случайная страница
            'random': True  # произвольные вакансии
        }
        response = requests.get(self.base_url, params=params)
        if response.status_code == 200:
            return response.json()['items']
        else:
            print("Ошибка при получении данных с HH.ru")
            return []

    def show_employers(self, employers):
        """
        Показывает список работодателей.
        """
        for idx, (employer_id, employer_name) in enumerate(employers.items(), 1):
            print(f"{idx}. {employer_name} (ID: {employer_id})")
        print("0. Завершить добавление компаний")

    def add_employer_to_db(self, employer_id, employer_name):
        """
        Добавляет компанию в базу данных через DBManager.
        """
        try:
            self.db_manager.insert_company(employer_name, employer_id)
            print(f"Компания {employer_name} добавлена в базу.")
        except Exception as e:
            print(f"Ошибка при добавлении компании: {e}")

    def add_employers_interactive(self, employers):
        """
        Интерактивное добавление компаний в базу данных через ввод цифр.
        После добавления компании, она удаляется из списка работодателей.
        """
        while employers:
            self.show_employers(employers)

            # Получение выбора пользователя
            choice = input("Выберите номер компании для добавления или 0 для завершения: ")
            if choice == '0':
                print("Завершение добавления компаний.")
                break

            try:
                choice = int(choice)
                employer_id, employer_name = list(employers.items())[choice - 1]
                self.add_employer_to_db(employer_id, employer_name)

                # Удаляем добавленную компанию из списка
                employers.pop(employer_id)
            except (ValueError, IndexError):
                print("Неверный выбор, попробуйте снова.")

        if not employers:
            print("Все доступные компании добавлены.")

    def extract_unique_employers(self, vacancies):
        """
        Извлекает уникальных работодателей из списка вакансий, которые не присутствуют в базе данных.
        Возвращает словарь с id и именем работодателя.
        """
        employers = {}
        for vacancy in vacancies:
            employer = vacancy.get('employer', {})
            employer_id = employer.get('id')
            employer_name = employer.get('name')

            # Проверяем, существует ли работодатель в базе данных
            if employer_id and employer_name and not self.db_manager.company_exists(employer_id):
                employers[employer_id] = employer_name

        return employers

    def get_vacancies_by_company(self, company_id, count=10):
        """
        Получает вакансии для конкретной компании.
        """
        params = {
            'employer_id': company_id,
            'per_page': count,
        }
        response = requests.get(self.base_url, params=params)
        if response.status_code == 200:
            return response.json()['items']
        else:
            print(f"Ошибка при получении вакансий для компании с ID {company_id}")
            return []

    def add_vacancies_for_company(self, company_id, company_name, count=10):
        """
        Получает вакансии компании по её ID и добавляет их в базу данных.
        """
        vacancies = self.get_vacancies_by_company(company_id, count)

        for vacancy in vacancies:
            # Добавляем вакансию в базу данных
            try:
                self.db_manager.insert_vacancy_from_json(vacancy)
            except Exception as e:
                print(f"Ошибка при добавлении вакансии '': {e}")

    def add_vacancies_for_all_companies(self):
        """
        Получает список компаний из базы данных и добавляет вакансии для каждой компании в базу.
        """
        companies = self.db_manager.get_all_companies()

        for company_id, company_name in companies:
            print(f"Получение вакансий для компании: {company_name} (ID: {company_id})")
            self.add_vacancies_for_company(company_id, company_name)
