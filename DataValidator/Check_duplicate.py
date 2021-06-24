from Data.Utils import db


class CheckDuplicate:
    @db.connect('redshift')
    def __init__(self, db_instance=None):
        self._table_list = []

        self.region_lookup = {'na': 'rds_na',
                              'sa': 'rds_sa',
                              'eu': 'rds_eu'
                             }

        self._result_list =[]
        self.db_instance = db_instance
        self.table_primary_key = {}

    def pop_primary_key(self):
        for item in self._table_list:
            self.table_primary_key[item] = 'id'

        self.table_primary_key['locations_regions'] = 'location_id'

        #self.table_primary_key['h3_lookup_del']

    def get_table_list(self, *, region, db_instance=None):
        print(self.region_lookup[region])
        for item in self.db_instance.get_table_list_by_schema(schema_name=self.region_lookup[region]).fetchall():
            print(item[0])
            if item[0] not in ('h3_lookup_del', 'h3_version_locations_del'):
                self._table_list.append(item[0])


    def run_query(self, *, region, primary_key):

        for item in self._table_list:
            print(f"select exists ("
                  f"select {self.table_primary_key[item]}, count(1) from "
                  f"{self.region_lookup[region]}.{item} "
                  f"group by {self.table_primary_key[item]} having count(1) > 1)")

            for subitem in self.db_instance.session.execute(f"select exists ("
                                                            f"select {self.table_primary_key[item]}, count(1) from "
                                                            f"{self.region_lookup[region]}.{item} "
                                                            f"group by {self.table_primary_key[item]} having count(1) > 1)").fetchall():
                print(f"{item}: {subitem[0]}")


if __name__ == '__main__':
    cd = CheckDuplicate()
    cd.get_table_list(region='na')
    cd.pop_primary_key()
    cd.run_query(region='na', primary_key='id')


