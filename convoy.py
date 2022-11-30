import json
import pandas as pd
import re
import sqlite3


class MyTable:

    def __init__(self):
        self.file_name = input("Input file name\n")
        self.df = pd.DataFrame
        self.conn = None
        self.cursor = None
        self.num_rect_cells = 0
        self.num_of_insertions = 0

        self.process_file()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.closecommit()

    def process_file(self):
        if self.file_name.endswith(".xlsx"):
            self.convert_xlsx_to_df()
            self.define_csv_name()
            self.write_csv_to_disk()
            self.print_xlsx_rows()
        if self.file_name.endswith(".csv") and not self.file_name.endswith("[CHECKED].csv"):
            self.convert_csv_to_df()
            self.rectify_csv()
            self.define_rect_csv_name()
            self.write_rect_to_disk()
            self.print_num_rect_cells()
        if self.file_name.endswith("[CHECKED].csv"):
            self.convert_csv_to_df()
            self.calc_scoring()
            self.define_db_name()
            self.connect_to_db()
            self.create_table()
            self.persist_scores_to_db()
            self.print_db_insertions()
        if self.file_name.endswith(".s3db"):
            self.connect_to_db()
            self.read_db_to_df()
            self.write_scores_to_disk()

    def convert_xlsx_to_df(self):
        self.df = pd.read_excel(f"{self.file_name}", sheet_name="Vehicles", dtype=str)

    def define_csv_name(self):
        self.file_name = self.file_name.replace(".xlsx", ".csv")

    def write_csv_to_disk(self):
        self.df.to_csv(f"{self.file_name}", index=None)

    def print_xlsx_rows(self):
        rows = len(self.df.index)
        print(f"{rows} {'lines were' if rows > 1 else 'line was'} added to {self.file_name}")

    def convert_csv_to_df(self):
        self.df = pd.read_csv(self.file_name)

    def rectify_csv(self):
        for row in self.df.index:
            for column in self.df.columns:
                cell = self.df.loc[row, column]
                if not str(cell).isdigit():
                    self.df.loc[row, column] = re.sub(r"\D", "", cell)
                    self.num_rect_cells += 1

    def define_rect_csv_name(self):
        self.file_name = re.sub(r"\.csv", "[CHECKED].csv", self.file_name)

    def write_rect_to_disk(self):
        self.df.to_csv(f"{self.file_name}", index=None)

    def print_num_rect_cells(self):
        num = self.num_rect_cells
        if num > 0:
            print(f"{num} {'cells were corrected' if num > 1 else 'cell was corrected'} in {self.file_name}")

    def calc_scoring(self):
        self.df["score"] = 0
        vehicle_id, engine_capacity, fuel_consumption, maximum_load, score = self.df.columns
        for row in self.df.index:
            self.df.loc[row, score] = pitstop_score(self.df.loc[row, engine_capacity],
                                                    self.df.loc[row, fuel_consumption]) + \
                                      fuel_score(self.df.loc[row, fuel_consumption]) + \
                                      capacity_score(self.df.loc[row, maximum_load])

    def define_db_name(self):
        self.file_name = self.file_name.replace("[CHECKED].csv", ".s3db")

    def connect_to_db(self):
        self.conn = sqlite3.connect(self.file_name)
        self.cursor = self.conn.cursor()

    def create_table(self):
        self.cursor.execute("DROP TABLE IF EXISTS convoy")
        self.cursor.execute("""CREATE TABLE convoy (
                            vehicle_id INTEGER PRIMARY KEY,
                            engine_capacity INT NOT NULL,
                            fuel_consumption INT NOT NULL,
                            maximum_load INT NOT NULL,
                            score INT NOT NULL);
                            """)

    def persist_scores_to_db(self):
        for veh_id, eng_cap, fuel_consum, max_load, score in self.df.values:
            self.cursor.execute(f"INSERT INTO convoy VALUES ({veh_id}, {eng_cap}, {fuel_consum}, {max_load}, {score});")
            self.num_of_insertions += 1
        self.conn.commit()

    def print_db_insertions(self):
        ins = self.num_of_insertions
        print(f"{ins} {'records were' if ins > 1 else 'record was'} inserted into {self.file_name}")

    def read_db_to_df(self):
        self.df = pd.DataFrame(pd.read_sql("SELECT * FROM convoy", self.conn))

    def define_json_name(self):
        self.file_name = re.sub('\.\S+$', '.json', self.file_name)

    def write_scores_to_disk(self):
        df_records = self.df.query("score > 3").drop(columns="score").to_dict(orient="records")
        json_rows = self.df.query("score > 3").shape[0]
        self.define_json_name()
        with open(re.sub(r'\.\S+$', '.json', self.file_name), mode="w") as json_file:
            json.dump({"convoy": df_records}, json_file, indent=4)
        print(f"{json_rows} {'vehicle was' if json_rows == 1 else 'vehicles were'} saved into {self.file_name}")
        xml = self.df.query("score <= 3").drop(columns="score").to_xml(index=False,
                                                                       root_name="convoy",
                                                                       row_name="vehicle",
                                                                       xml_declaration=False)
        xml_rows = self.df.query("score <= 3").shape[0]
        self.define_xml_name()
        with open(self.file_name, 'w') as file:
            if xml_rows > 0:
                file.write(xml)
            else:
                file.write("<convoy></convoy>")
        print(f"{xml_rows} {'vehicle was' if xml_rows == 1 else 'vehicles were'} saved into {self.file_name}")

    def print_num_vehicles(self):
        rows = len(self.df.index)
        print(f"{rows} {'vehicle was' if rows == 1 else 'vehicles were'} saved into {self.file_name}")

    def define_xml_name(self):
        self.file_name = re.sub(r'\.\S+$', '.xml', self.file_name)


def pitstop_score(engine_capacity, fuel_consumption):
    fuel_distance = engine_capacity / fuel_consumption * 100
    route = 450
    num_of_stops = route // fuel_distance
    if num_of_stops >= 2:
        return 0
    elif num_of_stops == 1:
        return 1
    else:
        return 2


def fuel_score(fuel_consumption):
    route = 450
    fuel__per_km = fuel_consumption / 100
    total_consumption = route * fuel__per_km
    return 2 if total_consumption <= 230 else 1


def capacity_score(maximum_load):
    return 2 if maximum_load >= 20 else 0


MyTable()
