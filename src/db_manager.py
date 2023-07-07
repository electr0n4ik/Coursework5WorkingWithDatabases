import psycopg2
import os
import json


class DBManager:
    """Класс для работы с платформой HeadHunter"""

    def __init__(self):
        self.conn = psycopg2.connect(host="localhost",
                                     database="hhru_job",
                                     user="postgres",
                                     password="123",
                                     options="-c client_encoding=utf-8")

    def create_tables(self):
        """Создание таблиц"""
        query_1 = """
        CREATE TABLE IF NOT EXISTS Employers (
        employer_id SERIAL PRIMARY KEY,
        name VARCHAR NOT NULL)"""

        query_2 = """
        CREATE TABLE IF NOT EXISTS Vacancies (
        vacancy_id SERIAL PRIMARY KEY,
        title VARCHAR NOT NULL,
        description VARCHAR,
        
        employer_id INTEGER REFERENCES Employers (employer_id))"""

        for query in [query_1, query_2]:
            with self.conn.cursor() as cursor:
                cursor.execute(query)
                self.conn.commit()

    def fill_tables_from_files(self, file_path):
        """Заполнение таблиц с помощью файлов с вакансиями."""

        with open(f"src/data_json_employers/{file_path}", 'r', encoding="utf-8") as file:

            data = json.load(file)

            for vacancy in data["items"]:

                if len(vacancy["employer"]["name"]) != 0:
                    name_company = vacancy["employer"]["name"]
                else:
                    name_company = ""

                if len(vacancy["name"]) != 0:
                    name_vacancy = vacancy["employer"]["name"]
                else:
                    name_vacancy = ""

                if (vacancy["snippet"]["requirement"] != None):
                    vacancy_desc_1 = vacancy["snippet"]["requirement"]
                else:
                    vacancy_desc_1 = ""
                if vacancy["snippet"]["responsibility"] != None:
                    vacancy_desc_2 = vacancy["snippet"]["responsibility"]
                else:
                    vacancy_desc_2 = ""

                description = f"{vacancy_desc_1} {vacancy_desc_2}"

                with self.conn.cursor() as cursor:

                    cursor.execute("""
                    INSERT INTO Employers (name) 
                    SELECT %s
                    WHERE NOT EXISTS (
                    SELECT name 
                    FROM Employers
                    WHERE name = %s)
                    """, (name_company, name_company))

                    self.conn.commit()

                    cursor.execute("SELECT employer_id FROM employers ORDER BY employer_id DESC LIMIT 1")
                    employer_id = cursor.fetchone()

                    cursor.execute("""INSERT INTO Vacancies (title, description, employer_id)
                    VALUES (%s, %s, %s)""", [name_vacancy, description, employer_id])

                    self.conn.commit()

    def get_companies_and_vacancies_count(self):
        """Получает список всех компаний и количество вакансий у каждой компании."""
        query = """
        SELECT Employers.name, COUNT(Vacancies.vacancy_id) AS vacancy_count
        FROM Employers
        LEFT JOIN Vacancies ON Employers.employer_id = Vacancies.employer_id
        GROUP BY Employers.name
        ORDER BY Employers.name
        """

        with self.conn.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()

        return results

    def get_all_vacancies(self):
        """Получает список всех вакансий с указанием названия компании,
        названия вакансии и зарплаты и ссылки на вакансию."""
        query = """
        SELECT Employers.name, Vacancies.title, Vacancies.salary, Vacancies.link
        FROM Employers
        INNER JOIN Vacancies ON Employers.employer_id = Vacancies.employer_id
        ORDER BY Employers.name, Vacancies.title
        """

        with self.conn.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()

        return results

    def get_avg_salary(self):
        """Получает среднюю зарплату по вакансиям."""
        query = "SELECT AVG(salary) FROM Vacancies"

        with self.conn.cursor() as cursor:
            cursor.execute(query)
            avg_salary = cursor.fetchone()[0]

        return avg_salary

    def get_vacancies_with_higher_salary(self):
        """Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям."""
        avg_salary = self.get_avg_salary()
        query = f"SELECT * FROM Vacancies WHERE salary > {avg_salary}"

        with self.conn.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()

        return results

    def get_vacancies_with_keyword(self, keyword):
        """Получает список всех вакансий, в названии которых содержатся переданные в метод слова, например “python”."""
        query = f"SELECT * FROM Vacancies WHERE title ILIKE '%{keyword}%'"

        with self.conn.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()

        return results

    def closs_conn(self):
        self.conn.close()
