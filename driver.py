import pandas as pd
import sqlite3
import numpy as np
from db.dbactions import dbactions_perform

db_perfom = dbactions_perform()


class dataintegration:
    def dbdrive(self, data_to_insert):
        # Create a master database
        cur, conn = db_perfom.create_db('Master.db')
        # create tables under master db
        db_create_success_flag = db_perfom.create_table(cur, conn)
        if db_create_success_flag:
            db_perfom.insert_db(data_to_insert)
        print('db insertion success')

    def datamunging(self):
        companies = pd.read_csv('data/companies.csv')
        unity_golf_club = pd.read_csv('data/unity_golf_club.csv')
        us_softball_league = pd.read_csv('data/us_softball_league.tsv', sep='\t')

        us_softball_league_columns = ['name', 'date_of_birth', 'company_id', 'last_active', 'score',
                                      'joined_league', 'us_state']

        unity_golf_club_columns = ['first_name', 'last_name', 'dob', 'company_id', 'last_active', 'score',
                                   'member_since', 'state']

        companies_columns = ['id', 'name']

        # Performing data munging on unity golf club data
        unity_golf_club['first_name'] = unity_golf_club.first_name.fillna('')
        unity_golf_club['last_name'] = unity_golf_club.last_name.fillna('')

        unity_golf_club['first_name'] = unity_golf_club['first_name'].astype('str')
        unity_golf_club['last_name'] = unity_golf_club['last_name'].astype('str')
        unity_golf_club['name'] = unity_golf_club['first_name'] + unity_golf_club['last_name']
        unity_golf_club.drop(columns=['first_name', 'last_name'], inplace=True)
        date_format = '%Y/%m/%d'
        unity_golf_club['last_active'] = pd.to_datetime(unity_golf_club['last_active'], format=date_format)
        unity_golf_club['dob'] = pd.to_datetime(unity_golf_club['dob'], format=date_format)
        unity_golf_club['sport'] = 'Golf'

        unity_golf_club.columns = ['data_of_birth', 'company_id', 'last_active', 'score', 'member_since', 'us_state',
                                   'name', 'sport']
        unity_golf_club = unity_golf_club[['name', 'data_of_birth', 'us_state', 'company_id', 'last_active', 'score',
                                           'member_since', 'sport']]
