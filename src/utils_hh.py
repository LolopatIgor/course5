import requests


class HHAPI:
    def __init__(self):
        """
        Инициализация объекта HHAPI.
        Устанавливаются базовый URL для API, заголовки и параметры запроса.
        """
        self.url = 'https://api.hh.ru/vacancies'
        self.headers = {'User-Agent': 'HH-User-Agent'}  # Заголовок User-Agent для запросов
        self.params = {'employer_id': '', 'page': 0, 'per_page': 100}  # Параметры запроса по умолчанию

    def get_vacancies_by_company(self, employer_id):
        """
        Получает вакансии для указанного работодателя по его ID.

        :param employer_id: ID работодателя, для которого требуется получить вакансии.
        :return: Список вакансий.
        """
        self.params['employer_id'] = employer_id  # Устанавливаем ID работодателя в параметры запроса
        vacancies = []

        # Загрузка данных с максимальным числом страниц вакансий
        for page in range(20):  # Загрузим максимум 20 страниц вакансий
            self.params['page'] = page  # Устанавливаем номер страницы
            response = requests.get(self.url, headers=self.headers, params=self.params)  # Выполняем запрос к API
            data = response.json()  # Получаем данные в формате JSON
            vacancies.extend(data.get('items', []))  # Добавляем вакансии в список

            # Проверяем, если следующая страница доступна
            if data['pages'] <= page:
                break  # Если нет больше страниц, выходим из цикла

        return vacancies  # Возвращаем собранный список вакансий
