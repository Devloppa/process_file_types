import json
import pandas as pd
import csv
import re
import sqlite3


class MyTable:

    def __init__(self):
        self.file_name = input("Input file name\n")
        self.file_type = None
        self.df = None
        self.rows = None
        self.cols = None
        self.num_rect_cells = 0
        self.conn = None
        self.cursor = None
        self.num_of_insertions = 0

        self.define_file_type()
        self.process_file()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.closecommit()

    def define_file_type(self):
        if self.file_name.endswith(".xlsx"):
            self.file_type = "XLSX"
        elif self.file_name.endswith("[CHECKED].csv"):
            self.file_type = "CHECKED_CSV"
        elif self.file_name.endswith(".s3db"):
            self.file_type = "S3DB"
        elif self.file_name.endswith(".csv"):
            self.file_type = "CSV"

    def process_file(self):
        if self.file_type == "XLSX":
            self.convert_xlsx_to_df()
            self.get_df_shape()
            self.define_csv_name()
            self.write_csv_to_disk()
            self.print_xlsx_rows()
            self.convert_csv_to_df()
            self.rectify_csv()
            self.define_rect_csv_name()
            self.write_rect_to_disk()
            self.print_num_rect_cells()
            self.define_db_name()
            self.connect_to_db()
            self.create_table()
            self.persist_to_db()
            self.print_db_insertions()
            self.define_json_name()
            self.write_json_to_disk()
            self.print_num_vehicles()

        elif self.file_type == "CSV":
            self.convert_csv_to_df()
            self.get_df_shape()
            self.rectify_csv()
            self.define_rect_csv_name()
            self.write_rect_to_disk()
            self.print_num_rect_cells()
            self.define_db_name()
            self.connect_to_db()
            self.create_table()
            self.persist_to_db()
            self.print_db_insertions()
            self.define_json_name()
            self.write_json_to_disk()
            self.print_num_vehicles()

        elif self.file_type == "CHECKED_CSV":
            self.convert_csv_to_df()
            self.define_db_name()
            self.connect_to_db()
            self.create_table()
            self.persist_to_db()
            self.print_db_insertions()
            self.define_json_name()
            self.write_json_to_disk()
            self.get_df_shape()
            self.print_num_vehicles()

        elif self.file_type == "S3DB":
            self.connect_to_db()
            self.define_json_name()
            self.write_json_to_disk()
            self.get_df_shape()
            self.print_num_vehicles()


    def convert_xlsx_to_df(self):
        self.df = pd.read_excel(f"{self.file_name}", sheet_name="Vehicles", dtype=str)

    def get_df_shape(self):
        self.rows = self.df.shape[0]
        self.cols = self.df.shape[1]

    def define_csv_name(self):
        self.file_name = self.file_name.replace(".xlsx", ".csv")

    def write_csv_to_disk(self):
        self.df.to_csv(f"{self.file_name}", index=None)

    def print_xlsx_rows(self):
        print(f"{self.rows} {'lines were' if self.rows > 1 else 'line was'} added to {self.file_name}")

    def convert_csv_to_df(self):
        self.df = pd.read_csv(self.file_name)

    def rectify_csv(self):
        for row in range(self.rows):
            for column in range(self.cols):
                cell = self.df.values[row, column]
                if not str(cell).isdigit():
                    self.df.values[row, column] = re.sub(r"\D", "", cell)
                    self.num_rect_cells += 1

    def define_rect_csv_name(self):
        self.file_name = re.sub(r"\.csv", "[CHECKED].csv", self.file_name)

    def write_rect_to_disk(self):
        self.df.to_csv(f"{self.file_name}", index=None)

    def print_num_rect_cells(self):
        num = self.num_rect_cells
        if num > 0:
            print(f"{num} {'cells were corrected' if num > 1 else 'cell was corrected'} in {self.file_name}")

    def define_db_name(self):
        self.file_name = re.sub(r"\[CHECKED]\.csv", ".s3db", self.file_name)

    def connect_to_db(self):
        self.conn = sqlite3.connect(self.file_name)
        self.cursor = self.conn.cursor()

    def create_table(self):
        self.cursor.execute("DROP TABLE IF EXISTS convoy")
        self.cursor.execute("""CREATE TABLE convoy (
                            vehicle_id INTEGER PRIMARY KEY,
                            engine_capacity INT NOT NULL,
                            fuel_consumption INT NOT NULL,
                            maximum_load INT NOT NULL);
                            """)

    def persist_to_db(self):
        for veh_id, eng_cap, fuel_consum, max_load in self.df.values:
            self.cursor.execute(f"INSERT INTO convoy VALUES ({veh_id}, {eng_cap}, {fuel_consum}, {max_load});")
            self.num_of_insertions += 1
        self.conn.commit()

    def print_db_insertions(self):
        ins = self.num_of_insertions
        print(f"{ins} {'records were' if ins > 1 else 'record was'} inserted into {self.file_name}")

    def define_json_name(self):
        self.file_name = re.sub(r"\.s3db", ".json", self.file_name)

    def write_json_to_disk(self):
        self.df = pd.DataFrame(pd.read_sql("SELECT * FROM convoy", self.conn))

        with open(f"{self.file_name}", mode="w") as json_file:
            json.dump({"convoy": self.df.to_dict(orient="records")}, json_file, indent=4)

    def print_num_vehicles(self):
        print(f"{self.rows} {'vehicles were' if self.rows > 1 else 'vehicle was'} saved into {self.file_name}")


MyTable()
