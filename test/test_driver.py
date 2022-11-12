from unittest import TestCase
from driver import datamunging, dbdrive
import pandas as pd


class Testing(TestCase):
    def test_datamunging(self):
        softball_golf_merge = datamunging()
        assert list(softball_golf_merge.columns) == ['name', 'date_of_birth', 'us_state', 'last_active',
                                             'score', 'joined_league', 'sport', 'company_name']

    def test_dbdrive(self):
        data_to_insert = pd.read_csv('softball_golf_merge_test.csv')
        return_val = dbdrive(data_to_insert)
        self.assertTrue(return_val == True)