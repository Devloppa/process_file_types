import pandas as pd
import csv
import re


class MyTable:

    def __init__(self):
        self.file_name = input("Input file name\n")
        self.new_file_name = None
        self.output_file_name = None
        self.df = None
        self.csv = None
        self.num_corrected = 0
        self.corrected_lst = []

        self.check_file_type()
        self.fix_csv()
        self.write_fixed_csv()
        self.print_checked_stats()

    def convert_to_df(self):
        self.df = pd.read_excel(f"{self.file_name}", sheet_name="Vehicles", dtype=str)

    def convert_to_csv(self):
        self.csv = self.df.to_csv(f"{self.file_name.replace('.xlsx', '.csv')}", index=None)

    def print_df_stats(self):
        print(f"{self.df.shape[0]} line was added to {self.file_name.replace('.xlsx', '.csv')}")\
            if self.df.shape[0] == 1 else print(
            f"{self.df.shape[0]} lines were added to {self.file_name.replace('.xlsx', '.csv')}")

    def check_file_type(self):
        if self.file_name.endswith(".xlsx"):
            self.convert_to_df()
            self.convert_to_csv()
            self.print_df_stats()
            self.new_file_name = self.file_name.replace(".xlsx", ".csv")
            return True
        else:
            self.new_file_name = self.file_name
            return False

    def fix_csv(self):
        with open(f"{self.new_file_name}", mode="r", newline="") as csv_f:
            file_reader = csv.reader(csv_f, delimiter=",")
            for index, row in enumerate(file_reader):
                if index == 0:
                    self.corrected_lst.append(row)
                if index != 0:
                    self.corrected_lst.append(row)
                    for cell_index, cell in enumerate(row):
                        if not str(cell).isdigit():
                            self.corrected_lst[index][cell_index] = re.sub(r"\D", "", cell)
                            self.num_corrected += 1

    def write_fixed_csv(self):
        self.output_file_name = self.new_file_name.replace(".csv", "[CHECKED].csv")
        with open(f"{self.output_file_name}", mode="w", encoding="utf-8") as csv_f:
            file_writer = csv.writer(csv_f, delimiter=",", lineterminator="\n")
            file_writer.writerows(self.corrected_lst)

    def print_checked_stats(self):
        print(f"1 cell wes corrected in {self.output_file_name}") if self.num_corrected == 1 else\
              print(f"{self.num_corrected} cells were corrected in {self.output_file_name}")


MyTable()
