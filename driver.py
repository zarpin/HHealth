import pandas as pd
from db.dbactions import create_db,create_table,insert_db
import re


def datamunging():
    companies = pd.read_csv('data/companies.csv')
    unity_golf_club = pd.read_csv('data/unity_golf_club.csv')
    us_softball_league = pd.read_csv('data/us_softball_league.tsv', sep='\t')

    companies.columns = ['id', 'company_name']

    # Performing data munging on unity golf club data
    unity_golf_club['first_name'] = unity_golf_club.first_name.fillna('')
    unity_golf_club['last_name'] = unity_golf_club.last_name.fillna('')

    unity_golf_club['first_name'] = unity_golf_club['first_name'].astype('str')
    unity_golf_club['last_name'] = unity_golf_club['last_name'].astype('str')
    unity_golf_club['name'] = unity_golf_club['first_name'] + ' ' + unity_golf_club['last_name']
    unity_golf_club.drop(columns=['first_name', 'last_name'], inplace=True)
    date_format = '%Y/%m/%d'
    unity_golf_club['last_active'] = pd.to_datetime(unity_golf_club['last_active'], format=date_format).dt.date
    unity_golf_club['dob'] = pd.to_datetime(unity_golf_club['dob'], format=date_format).dt.date
    unity_golf_club['sport'] = 'Golf'

    unity_golf_club.columns = ['date_of_birth', 'company_id', 'last_active', 'score', 'joined_league', 'us_state',
                               'name', 'sport']
    unity_golf_club = unity_golf_club[['name', 'date_of_birth', 'us_state', 'company_id', 'last_active', 'score',
                                       'joined_league', 'sport']]

    unity_golf_club_final = pd.merge(unity_golf_club, companies, left_on='company_id', right_on='id')
    unity_golf_club_final.drop(columns=['company_id', 'id'], inplace=True)

    # Performing data munging on unity softball club data

    us_softball_league.columns = ['name', 'date_of_birth', 'company_id', 'last_active', 'score',
                                  'joined_league', 'us_state']
    us_softball_league['sport'] = 'Softball'
    us_softball_league = us_softball_league[['name', 'date_of_birth', 'us_state', 'company_id', 'last_active',
                                             'score', 'joined_league', 'sport']]
    date_format = '%m/%d/%Y'

    us_softball_league['last_active'] = pd.to_datetime(us_softball_league['last_active'],
                                                       format=date_format).dt.date
    us_softball_league['date_of_birth'] = pd.to_datetime(us_softball_league['date_of_birth'],
                                                         format=date_format).dt.date

    states = {
        'AK': 'Alaska', 'AL': 'Alabama', 'AR': 'Arkansas', 'AS': 'American Samoa', 'AZ': 'Arizona',
        'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 'DC': 'District of Columbia', 'DE': 'Delaware',
        'FL': 'Florida', 'GA': 'Georgia', 'GU': 'Guam', 'HI': 'Hawaii', 'IA': 'Iowa', 'IL': 'Illinois',
        'IN': 'Indiana', 'KS': 'Kansas', 'KY': 'Kentucky', 'LA': 'Louisiana', 'MA': 'Massachusetts',
        'MD': 'Maryland', 'ME': 'Maine',
        'MI': 'Michigan', 'MN': 'Minnesota', 'MO': 'Missouri', 'MP': 'Northern Mariana Islands',
        'MS': 'Mississippi', 'MT': 'Montana', 'NA': 'National', 'NC': 'North Carolina',
        'ND': 'North Dakota', 'NE': 'Nebraska', 'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico',
        'NV': 'Nevada', 'NY': 'New York', 'OH': 'Ohio', 'OK': 'Oklahoma', 'OR': 'Oregon', 'PA': 'Pennsylvania',
        'PR': 'Puerto Rico', 'RI': 'Rhode Island', 'SC': 'South Carolina', 'SD': 'South Dakota', 'TN': 'Tennessee',
        'TX': 'Texas', 'UT': 'Utah', 'VA': 'Virginia', 'VI': 'Virgin Islands', 'VT': 'Vermont', 'WA': 'Washington',
        'WI': 'Wisconsin', 'WV': 'West Virginia', 'WY': 'Wyoming'
    }

    def best_match(x):
        if len(x) == 2:  # Try another way for 2-letter codes
            for a, n in states.items():
                if len(n.split()) == 2:
                    if "".join([c[0] for c in n.split()]).lower() == x.lower():
                        return a.lower()
        new_rx = re.compile(r"\w*".join([ch for ch in x]), re.I)
        for a, n in states.items():
            if new_rx.match(n):
                return a.upper()

    us_softball_league['us_state'] = us_softball_league['us_state'].apply(lambda x: best_match(x))

    us_softball_league_final = pd.merge(us_softball_league, companies, left_on='company_id', right_on='id')
    us_softball_league_final.drop(columns=['company_id', 'id'], inplace=True)

    softball_golf_merge  =  pd.concat([unity_golf_club_final,us_softball_league_final])

    return softball_golf_merge


def dbdrive(data_to_insert):
    # Create a master database
    cur, conn = create_db('Master.db')
    # create tables under master db
    cur.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='sports' ''')
    if cur.fetchone()[0] == 1:
        print('Table already exists.')
        db_create_success_flag = True
    else:
        print('Table does not exist.')
        db_create_success_flag = create_table(cur, conn)
    if db_create_success_flag:
        insert_db(cur, conn, data_to_insert)
    print('db insertion success')