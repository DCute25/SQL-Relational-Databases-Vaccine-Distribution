"""
Initializes the Postgres database
"""
import psycopg2
import sqlalchemy
from pathlib import Path
import os
import numpy as np
import pandas as pd
import datetime as dt
import math

WEEKDAYS = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
RUN_LOCALLY = True  # Use local database connection parameters or not

################################################################################
### Connection parameters

if RUN_LOCALLY:
    db_params = {
        'database': 'postgres',
        'user': 'postgres',
        'password': 'password',
        'host': 'localhost'
    }
else:
    db_params = {
        'host': 'dbcourse2022.cs.aalto.fi',
        'database': 'grp14_vaccinedist',
        'user': 'grp14',
        'password': '3c$H(08s'
    }


################################################################################
### Helper functions

def run_sql_from_file(sql_file, conn):
    '''
	read a SQL file with multiple stmts and process it
	adapted from an idea by JF Santos
	Note: not really needed when using dataframes.
    '''
    results = {}
    index = 1
    sql_command = ''
    for line in sql_file:
        #if line.startswith('VALUES'):
     # Ignore commented lines
        if not line.startswith('--') and line.strip('\n'):
        # Append line to the command string, prefix with space
           sql_command +=  ' ' + line.strip('\n')
           #sql_command = ' ' + sql_command + line.strip('\n')
        # If the command string ends with ';', it is a full statement
        if sql_command.endswith(';'):
            # Try to execute statement and commit it
            try:
                #print("running " + sql_command+".")
                # psql_conn.execute(text(sql_command))
                results[index] = pd.read_sql(sqlalchemy.text(sql_command), conn)
                index += 1
                #psql_conn.commit()
            # Assert in case of error
            except:
                print('Error at command:'+sql_command + ".")
                results[index] = "ERROR"
                index += 1
                ret_ =  False
            # Finally, clear command string
            finally:
                sql_command = ''
                ret_ = True
    return results

def query_all_tables(all_keys, first_n=95):
    for i, key in enumerate(all_keys):
        query = f"SELECT * FROM {key};"
        res = pd.read_sql(query, conn)
        print('---------------------------------------------------------------')
        print(f'[{i}] Query result for {key}:\n', res.head(first_n))


def get_db_url(user, password, host, database):
    DIALECT = 'postgresql+psycopg2://'
    URL = f'{user}:{password}@{host}/{database}'
    return DIALECT + URL


def execute_sql_from_file(path, engine):
    assert path.exists(), f"Given file at path {path} does not exist!"
    text = sqlalchemy.text(open(path, 'r').read())
    engine.execute(text)


def read_data(path):
    df_dict = pd.read_excel(path, sheet_name=None)
    # Convert dictionary keys and dataframe column names to lower case
    # --> postgres changes everything automatically to lower case
    df_dict = {key.lower(): value for key, value in df_dict.items()}
    for key in df_dict.keys():
        df_dict[key].columns = df_dict[key].columns.str.lower()
    return df_dict


def write_data_to_db(df_dict, conn):
    print('Writing data to database...')

    # ORDER of writing the tables first time is necessary due to FOREIGN KEY
    # constraints. The actual PKs the foreign key is referring to has to be written
    # first ==> this is defined by the order
    ORDER = [
        ['manufacturer', 'vaccinetype', 'vaccinationstations',
         'patients', 'symptoms'],
        ['vaccinebatch', 'staffmembers', 'diagnosis'],
        ['transportationlog', 'shifts', 'vaccinations'],

        # Extra tables: disabled currently since they're missing from the preprocessing step
        ['vaccinepatients'],

        ['workon']
    ]
    # GO THROUGH KEYS INTRODUCED IN EXCEL FILE
    for keys in ORDER:
        for key in keys:
            assert key == key.lower(), "Keys should be in lower case"
            assert key in df_dict.keys(), f"key '{key}' not found from df_dict"
            print(f"--> Writing {key}")

            df_dict[key].to_sql(key, con=conn, if_exists='append', index=False)

    print('Done writing data!')


def preprocessing(df_dict):
    # ----- FIXING ILLEGAL VALUES ------

    idx = (df_dict['diagnosis']['date'] == '2021-02-29')  # row index
    df_dict['diagnosis'].loc[idx, 'date'] = dt.datetime(2021,2,28).date()  # changed to closest valid date


    idx = (df_dict['diagnosis']['date'] == 44237)  # row index
    s1 = pd.to_datetime(pd.to_numeric(df_dict['diagnosis'].loc[idx, 'date'], errors='coerce'), errors='coerce', origin='1900-01-01', unit='D') #numeric to date
    df_dict['diagnosis'].loc[idx, 'date'] = s1 #valid date

    # ------ RENAMING ------
    # Rename
    df_dict['transportationlog'] = df_dict.pop('transportation log')
    df_dict['transportationlog'].rename(columns={'departure ': 'departure'}, inplace=True)
    df_dict['patients'].rename(columns={'date of birth': 'birthday'}, inplace=True)
    df_dict['manufacturer'].rename(columns={'id': 'manuid'}, inplace=True)
    df_dict['staffmembers'].rename(inplace=True,
                                   columns={
                                       'social security number': 'ssno',
                                       'date of birth': 'birthday',
                                       'vaccination status': 'vaccinestatus'
                                   })
    df_dict['vaccinetype'].rename(columns={'id': 'vaccineid'}, inplace=True)

    df_dict['staffmembers'].rename(columns={'hospital': 'station'}, inplace=True)
    df_dict['vaccinations'].rename(columns={"location ": "station"}, inplace=True)
    df_dict['vaccinepatients'].rename(columns={'patientssno': 'patient'}, inplace=True)

    # ------ CREATING NEW DATAFRAMES ------
    ### SHIFTIDs
    SHIFTS_COPY = df_dict['shifts'].copy()

    df_dict['shifts'].drop(labels=['worker'], axis=1, inplace=True)
    df_dict['shifts'] = df_dict['shifts'][['station', 'weekday']].copy().drop_duplicates()
    SHIFT_IDS = np.arange(0, df_dict['shifts'].shape[0], dtype=int)
    df_dict['shifts']['shiftid'] = SHIFT_IDS

    # Create workon dataframe
    df_dict['workon'] = SHIFTS_COPY.merge(df_dict['shifts'], on=['station', 'weekday']).drop(
        labels=['station', 'weekday'], axis=1)
    df_dict['workon'].rename(columns={'worker': 'staff'}, inplace=True)

    # ADDING SHIFTID to VACCINATIONS
    # Obtain weekday of vaccine event Date
    weekday_vacc_event =  df_dict['vaccinations']['date']
    weekday_vacc_event = weekday_vacc_event.dt.dayofweek

    for i in range(len(weekday_vacc_event)):
        weekday = WEEKDAYS[weekday_vacc_event[i]]
        weekday_vacc_event[i] = weekday
    #print(weekday_vacc_event)

    df_dict['vaccinations']['weekday'] = weekday_vacc_event
    df_dict['vaccinations'] = df_dict['vaccinations'].merge(df_dict['shifts'], how='left', on = ['station', 'weekday']).drop(
        labels=['weekday'], axis=1)


    return df_dict


################################################################################
# Connect

if __name__ == '__main__':
    engine = sqlalchemy.create_engine(get_db_url(**db_params), echo=False)
    conn = engine.connect()

    ROOT_PATH = Path('../')  # Assumes we're currently in /data folder
    DATA_PATH = ROOT_PATH / 'data' / 'vaccine-distribution-data.xlsx'

    df_dict = read_data(DATA_PATH)
    df_dict = preprocessing(df_dict)

    ALL_KEYS = list(df_dict.keys())

    print(f'Read in excel sheets are:')
    for i, key in enumerate(df_dict.keys()):
        print(f'[{i}] --> {key}')

    # Run .sql schema table definitions
    execute_sql_from_file(ROOT_PATH / 'database' / 'db_schema.sql', conn)

    # Write .xlsx data to database
    write_data_to_db(df_dict, conn)

    query_all_tables(ALL_KEYS)

    QUERIES_PATH = ROOT_PATH / 'code' / 'postgres_queries_pt2.sql'
    sql_file = open(QUERIES_PATH, 'r').readlines()
    res = run_sql_from_file(sql_file, conn)
    print(res)