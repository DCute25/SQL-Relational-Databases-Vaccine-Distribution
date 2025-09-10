"""
Vaccine distribution project part 3
"""
import sqlalchemy
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import datetime as dt
import numpy as np
import pandas as pd
import psycopg2
from datetime import timedelta
from psycopg2 import Error
from postgres_init import *


def question1():
    # QUESTION 1
    patientsymptoms = \
    """
    SELECT ssno, gender, birthday As dateOfBirth, symptom, date AS diagnosisDate
    FROM patients AS P, diagnosis AS D
    WHERE P.ssno = D.patient;
    """
    patientsymptoms = pd.read_sql_query(patientsymptoms, conn)
    df_dict['patient_symptoms'] = pd.DataFrame(patientsymptoms)

    # print dataframe
    print('---------------------------------------------------------------')
    print("Dataframe for question 1")
    print(df_dict['patient_symptoms'])

    # create table
    patientsymptoms_psql = "patient_symptoms"
    df_dict['patient_symptoms'].to_sql(patientsymptoms_psql, conn, if_exists='replace')

def question2():
    # QUESTION 2
    patientvaccineinfo = \
    """
    SELECT A.patient, date1, vaccinetype1, date2, vaccinetype2
    FROM
        (SELECT date1, patient, VP.location, type AS vaccinetype1 
        FROM
            (SELECT min(date) AS date1, patient, location
            FROM vaccinepatients
            GROUP BY patient, location) AS VP, vaccinations AS V, vaccinebatch AS VB
            WHERE VP.date1 = V.date AND VP.location = V.station AND
            VB.batchid = V.batchid) AS A 
        LEFT JOIN 
        (SELECT date2, patient, VP.location, type AS vaccinetype2 
        FROM
            (SELECT max(date) AS date2, patient, location
            FROM vaccinepatients
            GROUP BY patient, location
            HAVING count(*) = 2) AS VP, vaccinations AS V, vaccinebatch AS VB
            WHERE VP.date2 = V.date AND VP.location = V.station AND
            VB.batchid = V.batchid) AS B
        ON A.patient = B.patient;
    """

    patientvaccineinfo = pd.read_sql_query(patientvaccineinfo, conn)
    df_dict['patient_vaccine_info'] = pd.DataFrame(patientvaccineinfo)
    # print dataframe
    print('---------------------------------------------------------------')
    print("Dataframe for question 2")
    print(df_dict['patient_vaccine_info'])

    #create table
    patientvaccineinfo_psql = "patientvaccineinfo"
    df_dict['patient_vaccine_info'].to_sql(patientvaccineinfo_psql, conn, if_exists='replace')


def question3():
    #QUESTION 3
    print('---------------------------------------------------------------')
    print("Dataframe for question 3")
    df_male = df_dict['patient_symptoms'][df_dict['patient_symptoms']['gender'] == "M"]
    print(df_male)
    grp_1 = df_male.groupby(['symptom'])['symptom'].count().reset_index(name='count')
    print(grp_1.nlargest(3,['count']))

    df_female = df_dict['patient_symptoms'][df_dict['patient_symptoms']['gender']=="F"]
    print(df_female)
    grp_2 = df_female.groupby(['symptom'])['symptom'].count().reset_index(name='count')
    print(grp_2.nlargest(3,['count']))


def question4():
    # QUESTION 4
    sql_ = \
    """
    SELECT ssno, name, birthday, gender,
        CASE
        WHEN age < 10 THEN '0-9'
        WHEN 10<= age AND age < 20 THEN '10-19'
        WHEN 20<= age AND age < 40 THEN '20-39'
        WHEN 40<= age AND age < 60 THEN '40-59'
        WHEN age >= 60 THEN '60+'
        END AS age_group
    FROM(
        SELECT *, DATE_PART('year', age(CURRENT_DATE,date(birthday))) as age 
        FROM patients) AS patients_age_group;
    """
    age_group = pd.read_sql_query(sql_, conn)
    df_dict['age_group'] = pd.DataFrame(age_group)
    print('---------------------------------------------------------------')
    print("Dataframe for question 4")
    print(df_dict['age_group'])


def question5():
    # QUESTION 5
    sql_ = \
    """
    SELECT ssno,
           CASE
           WHEN count >= 2 THEN 2
           WHEN count = 1 THEN 1
           WHEN count IS NULL OR count = 0 THEN 0
           END AS vaccine_status
      FROM
        (SELECT *
        FROM patients AS P
        LEFT JOIN(
            SELECT patient, COUNT( * ) AS count
            FROM vaccinepatients AS VP
            GROUP BY patient) AS Vaccinated
        ON P.ssno = Vaccinated.patient) AS totaldose;
    """
    vaccine_status = pd.read_sql_query(sql_, conn)
    df_dict['vaccine_status'] = pd.DataFrame(vaccine_status)
    df_dict['vaccine_status'] = df_dict['age_group'].merge(df_dict['vaccine_status'], on = ['ssno'])
    print('---------------------------------------------------------------')
    print("Dataframe for question 5")
    print(df_dict['vaccine_status'])


def question6():
    # QUESTION 6
    df_dict['vaccine_status_count']  = df_dict['vaccine_status'].groupby(['vaccine_status','age_group'])['vaccine_status'].count().rename('count')
    print(df_dict['vaccine_status_count'])
    df_dict['vaccine_status_count'] = pd.DataFrame(df_dict['vaccine_status_count'])
    df_dict['%'] = 100.00 * df_dict['vaccine_status_count'] / df_dict['vaccine_status_count'].groupby(['age_group']).transform('sum')
    df_dict['%'] = df_dict['vaccine_status_count'].merge(df_dict['%'], on = ['age_group', 'vaccine_status'])
    df_dict['%'] = df_dict['%'].pivot_table('count_y', ['vaccine_status'], 'age_group' )
    df_dict['%'] = pd.DataFrame(df_dict['%'])

    # print dataframe
    print('---------------------------------------------------------------')
    print("Dataframe for question 6")
    print(df_dict['%'])


def question7():
    # QUESTION 7
    frequency = \
    """
    SELECT A.type, A.symptom, symptomcount, typecount, symptomcount*100.00/typecount AS frequency
    FROM
    /* Find the total number of patients suffered from each symptom for each vaccinetype*/
        (SELECT VB.type, D.symptom, COUNT(*) AS symptomcount
        FROM diagnosis AS D,
        vaccinepatients AS VP,
        vaccinations AS V,
        vaccinebatch AS VB
        WHERE D.patient = VP.patient AND
        VP.date = V.date AND
        V.station = VP.location AND
        V.batchid = VB.batchid AND
        D.date >= VP.date
        GROUP BY type, symptom) AS A JOIN
    /* Find the total number of patients for each vaccinetype*/
        (SELECT VB.type, COUNT(*) AS typecount
        FROM vaccinepatients AS VP,
        vaccinations AS V,
        vaccinebatch AS VB
        WHERE VP.date = V.date AND
        V.station = VP.location AND
        V.batchid = VB.batchid
        GROUP BY type) AS B ON A.type = B.type;
    """
    frequency = pd.read_sql_query(frequency, conn)
    df_dict['frequency'] = pd.DataFrame(frequency)

    for i in df_dict['frequency']['frequency']:
        if i >= 10:
            df_dict['frequency']['frequency'] = df_dict['frequency']['frequency'].replace([i],'very common')
        elif i >= 5:
            df_dict['frequency']['frequency'] = df_dict['frequency']['frequency'].replace([i], 'common')
        elif i > 0:
            df_dict['frequency']['frequency'] = df_dict['frequency']['frequency'].replace([i], 'rare')

    df_dict['frequency']= pd.DataFrame(df_dict['frequency'].pivot(index="symptom", columns='type',values="frequency"))

    df_sym = pd.DataFrame({'symptom': df_dict['symptoms']['name']})
    df_dict['rela_frequency']  = pd.merge(df_sym,df_dict['frequency'],how='left',on = 'symptom')

    df_dict['rela_frequency']['V01']            = df_dict['rela_frequency']['V01'].fillna('-')
    df_dict['rela_frequency']['V02']            = df_dict['rela_frequency']['V02'].fillna('-')
    df_dict['rela_frequency']['V03']            = df_dict['rela_frequency']['V03'].fillna('-')

    print('---------------------------------------------------------------')
    print("Dataframe for question 7")
    print(df_dict['rela_frequency'])

    #CREATE TABLE
    relafrequency_psql = "relafrequency"
    df_dict['rela_frequency'] = df_dict['rela_frequency'].to_sql(relafrequency_psql, conn, if_exists='replace')


def question8():
    # QUESTION 8
    vaccine_avg = """
    SELECT A.date, location, patientcount, vaccineamount, (patientcount*100.0/vaccineamount) AS attendpercent
    FROM
    (SELECT date, location, count(*) AS patientcount
    FROM vaccinepatients
    GROUP BY location, date) AS A,
    (SELECT date, station, amount AS vaccineamount
    FROM vaccinations, vaccinebatch
    WHERE vaccinations.batchid = vaccinebatch.batchid) AS B
    WHERE A.location = B.station AND
    A.date = B.date;
    """
    vaccine_avg = pd.read_sql_query(vaccine_avg, conn)
    df_dict['vaccine_avg'] = pd.DataFrame(vaccine_avg)
    print('---------------------------------------------------------------')
    print('Dataframe for question 8')
    print(df_dict['vaccine_avg'])
    vaccine_mean = df_dict['vaccine_avg']['attendpercent'].mean()
    vaccine_std = df_dict['vaccine_avg']['attendpercent'].std()
    print('The amount of vaccines (as a percentage) that should be reserved for each vaccination is',
          vaccine_mean + vaccine_std)


def question9():
    print('---------------------------------------------------------------')
    print("----- QUESTION 9 -----")
    info = df_dict['patient_vaccine_info'].copy()

    info['date1'] = pd.to_datetime(info['date1'], format="%Y-%m-%d")
    info['date2'] = pd.to_datetime(info['date2'], format="%Y-%m-%d")

    shape = info.date1.shape
    info['count'] = np.ones(shape)

    count1 = info.groupby('date1').sum().cumsum()
    count2 = info.groupby('date2').sum().cumsum()

    plt.plot(count1.index, count1['count'], label='1st vaccine')
    plt.plot(count2.index, count2['count'], label='2nd vaccine')
    plt.legend()
    plt.title('Q9: Total number of vaccinated patients wrt. time')
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=25))
    plt.gcf().autofmt_xdate()
    plt.tight_layout()
    plt.savefig('q9-vaccines-plot.png')
    plt.show()


def question10():
    # QUESTION 10
    print('---------------------------------------------------------------')
    print("----- QUESTION 10 -----")
    #Information on staffmembers working on vaccination days
    vaccinestaff = \
    """
    SELECT V.date, V.station, staff, name
        FROM vaccinations AS V, workon AS W, staffmembers AS SM
        WHERE V.shiftid = W.shiftid AND
        W.staff = SM.ssno;
    """
    vaccinestaff = pd.read_sql_query(vaccinestaff, conn)
    df_vaccinestaff = pd.DataFrame(vaccinestaff)

    #Information on patients
    vacpatient = \
    """
    SELECT date, location AS station, patient, name
        FROM vaccinepatients AS VP, patients AS P
        WHERE VP.patient = P.ssno;
    """
    vacpatient = pd.read_sql_query(vacpatient, conn)
    df_vacpatient = pd.DataFrame(vacpatient)



    def subtract_days_from_date(date, days):
        """Subtract days from a date and return the date.

        Args:
            date (string): Date string in YYYY-MM-DD format.
            days (int): Number of days to subtract from date

        Returns:
            date (date): Date in YYYY-MM-DD with X days subtracted.
        """

        subtracted_date = pd.to_datetime(date) - timedelta(days=days)
        subtracted_date = subtracted_date.strftime("%Y-%m-%d")

        return subtracted_date

    try:
        # Ask for the ssno and the date the staff test for corona
        staffssno = input('SsNo of the staff:')
        coviddate = pd.to_datetime(input('The date of the test(YYYY-MM-DD):'))

        # obtain the date of 10 days ago
        date = subtract_days_from_date(coviddate, 10)

        # info of staff base on ssno and tested positive date
        info = df_vaccinestaff[(df_vaccinestaff['staff'] == staffssno)]

        # dataframe for patients with closed contact
        df_patient = pd.merge(info,df_vacpatient, how='left', on=['date','station'])
        df_patient = df_patient[(pd.to_datetime(df_patient['date']) <= pd.to_datetime(coviddate)) & (pd.to_datetime(df_patient['date']) >= pd.to_datetime(date))]
        df_patient = df_patient.drop(["station","staff","name_x"], axis=1).drop_duplicates()
        df_patient = df_patient.reset_index()
        del df_patient['index']

        # dataframe for staffs with closed contact
        df_staff = pd.merge(info,df_vaccinestaff,how='left',on = ['date','station'])
        df_staff = df_staff[(pd.to_datetime(df_staff['date']) <= pd.to_datetime(coviddate)) & (pd.to_datetime(df_staff['date']) >= pd.to_datetime(date))]
        df_staff = df_staff.drop(["station","staff_x", "name_x"], axis=1).drop_duplicates()
        df_staff = df_staff.reset_index()
        del df_staff['index']

        # print dataframe
        print(f'\nSsno and name of patients who are in close contact with staff {staffssno} in the past 10 days:')
        print(df_patient)
        print(f'\nSsno and name of staffs who are in close contact with staff {staffssno} in the past 10 days:')
        print(df_staff)
    except (Exception, Error) as error:
        print("Error:", error)

########################################################################################################################
# Create connection
engine = sqlalchemy.create_engine(get_db_url(**db_params), echo=False)
conn = engine.connect()

ROOT_PATH = Path('../')  # Assumes we're currently in /data folder
DATA_PATH = ROOT_PATH / 'data' / 'vaccine-distribution-data.xlsx'

# Read in excel data & preprocess for later use
df_dict = read_data(DATA_PATH)
df_dict = preprocessing(df_dict)

# Run questions
question1()
question2()
question3()
question4()
question5()
question6()
question7()
question8()
question9()
question10()

conn.close()

########################################################################################################################
