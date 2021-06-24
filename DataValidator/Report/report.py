import orderdict
import pandas as pd
import sqlalchemy
from Data.Connect import postgresql
from Data.Utils import db
from DataValidator.Model import Table_Count_Compare
from DataValidator.Model import Base
import json
import operator
from sqlalchemy import subquery


class Report:

    def __init__(self):
        self.table_diff_report = {}
        self.table_count_report = {}

    @db.connect('meta')
    def generatereport(self, db_instance=None):
        Base.metadata.create_all(db_instance.engine)

        sub_query1 = db_instance.session.query(Table_Count_Compare).filter(Table_Count_Compare.system_name == 'Redshift').subquery()
        sub_query2 = db_instance.session.query(Table_Count_Compare).filter(Table_Count_Compare.system_name == 'RDS').subquery()
        query = db_instance.session.query(sub_query2, sub_query1.c.object_count).join(sub_query1, sub_query2.c.object_name == sub_query1.c.object_name)

        for _row in query.all():
            print(_row[2], _row[3], _row[4])
            self.table_diff_report[_row[2]] = int(_row[3]) - int(_row[4])
            self.table_count_report[_row[2]] = int(_row[3])

        matching_table_count = 0
        mismatchtable_count = 0

        df = pd.DataFrame(self.table_diff_report.items())
        print(df)

        for index, value in self.table_diff_report.items():
            if value == 0:
                matching_table_count += 1
            else:
                mismatchtable_count += 1

        print(f"Summary:\n\tTotal table: {matching_table_count + mismatchtable_count} \n\tMatch Table: {matching_table_count} \n\tMismatch Table: {mismatchtable_count}")

    def print_table_count(self):
        sorted_dict = sorted(self.table_count_report.items(), key=operator.itemgetter(1))
        print(json.dumps(sorted_dict, indent=4, sort_keys=True))

if __name__ == '__main__':
    rep = Report()
    rep.generatereport()
    rep.print_table_count()

