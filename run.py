import psycopg2
import os
from src.db_manager import DBManager
from src.headhunter_api import HeadHunter


if __name__ == "__main__":
    db_manager = DBManager()
    db_manager.create_tables()

    hh = HeadHunter()
    hh.get_json_files()

    i = 1

    for file in os.listdir("src/data_json_employers"):
        db_manager.fill_tables_from_files(file)
        print(file, "ОК", i)
        i += 1

    print()
    print(db_manager.get_companies_and_vacancies_count())
    print()
    print(db_manager.get_all_vacancies())
    print()
    print(db_manager.get_avg_salary(), "Средняя зарплата")
    print()
    for row in db_manager.get_vacancies_with_higher_salary():
        print(row)
        print()

    print()
    print(db_manager.get_vacancies_with_keyword("python"))

    db_manager.closs_conn()
