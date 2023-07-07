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

    db_manager.closs_conn()
