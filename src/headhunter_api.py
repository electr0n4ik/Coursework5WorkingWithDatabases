from requests import *
import json


class HeadHunter:
    """Класс для работы с платформой HeadHunter"""

    _api_link = "https://api.hh.ru/vacancies"

    def __init__(self):
        pass

    def __str__(self):
        return "headhunter.ru"

    @staticmethod
    def printj(data_dict) -> None:
        """Выводит словарь в json-подобном удобном формате с отступами (Для разработки)"""
        print(json.dumps(data_dict, indent=2, ensure_ascii=False))

    def get_vacancies_api(self, **kwargs):
        """
        :param kwargs:
        area - Код региона (1 - Москва)
        text - Поисковый запрос
        employer_id - id компании. Указывать фактические идентификаторы компаний, разделенные запятыми.
        'employer_id':1122462, 15478, 80, 78638, 1308904, 6, 8, 3529, 4181, 1429999"
        per_page - Количество вакансий на странице
        """

        params = {}
        for key, value in kwargs.items():
            params[key] = value

        response = get(self._api_link, params=params)

        if response.status_code == 200:
            data = response.text
            data_dict = json.loads(data)
            return data_dict
        else:
            print("Ошибка при выполнении запроса:", response.status_code)
            return None

    def get_json_files(self):
        """Сохранение файлов 10 компаний."""
        for one_id in [1122462, 15478, 80, 78638, 1308904, 6, 5947075, 3529, 4181, 1429999]:
            with open(f"src/data_json_employers/{one_id}.json", 'w', encoding="utf-8") as file:
                json.dump(self.get_vacancies_api(employer_id=one_id, per_page=100), file, indent=2, ensure_ascii=False)
