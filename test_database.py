import unittest
from database import Database

class TestDatabase(unittest.TestCase):
    def test_database(self):
        db_name = "test"
        db_path = "./ms_db_test_output"
        TABLE_1 = "table1"
        TABLE_2 = "table2"
        db = Database(db_name, db_path)
        db.add_table(TABLE_1, [1, 2, 3])
        db.add_row(TABLE_1, ["sdgfsdg", "asdfsdfgd", "sdfgdsfgd"])
        db.add_row(TABLE_1, {1: "ab", 2: "bd", 3: "cd"})
        db.add_table(TABLE_2, [4, 3, 2])
        db.add_row(TABLE_2, ["aaaaa", "bbbbbb", "cccccc"])
        db.add_row(TABLE_2, {4: "adf", 3: "bdgfbhd", 2: "ccxvd"})
        db.save()
        print(db)

if __name__ == '__main__':
    unittest.main()
