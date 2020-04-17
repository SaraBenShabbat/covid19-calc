#!/usr/bin/env python3

import redis
import ast
from elasticsearch import Elasticsearch
import time
from datetime import datetime, timezone
from dateutil.parser import parse
import pandas as pd
import numpy as np

host = 'https://search-covid198-es-2-x6zr2th7oiq7sjzp653cs3k3xm.eu-west-1.es.amazonaws.com/'
region = 'eu-west-1'
es = Elasticsearch(
    hosts=host,
)
es.ping()
doc = {
    'size': 10000,
    'query': {
        'match_all': {}
    }
}
source_to_update = {
    "doc": {
        "_score": 2.0
    }
}

# Redis connection.
elasticache_endpoint = "cv19redis-001.d9jy7a.0001.euw1.cache.amazonaws.com"
r = redis.StrictRedis(host=elasticache_endpoint, port=6379, db=0)

ms_utc = int(datetime.utcnow().timestamp() * 1000)

# Create df's that represent the measure values and severity.
df_breath = pd.DataFrame({'min': {0: 0, 1: 9, 2: 12, 3: 21, 4: 25},
                          'max': {0: 8.9, 1: 11.9, 2: 20.9, 3: 24.9, 4: 35},
                          'severity': {0: 3, 1: 1, 2: 0, 3: 2, 4: 3}, })
df_pso2 = pd.DataFrame({'min': {0: 0, 1: 92, 2: 94, 3: 96},
                        'max': {0: 91.9, 1: 93.9, 2: 95.9, 3: 100},
                        'severity': {0: 3, 1: 2, 2: 1, 3: 0}, })
df_BPM = pd.DataFrame({'min': {0: 0, 1: 41, 2: 51, 3: 91, 4: 111, 5: 131},
                       'max': {0: 40.9, 1: 50.9, 2: 90.9, 3: 110.9, 4: 130.9, 5: 200},
                       'severity': {0: 3, 1: 1, 2: 0, 3: 1, 4: 2, 5: 3}, })
df_BloodPressure = pd.DataFrame({'min': {0: 0, 1: 91, 2: 101, 3: 111, 4: 220},
                                 'max': {0: 90.9, 1: 100.9, 2: 110.9, 3: 219.9, 4: 300},
                                 'severity': {0: 3, 1: 2, 2: 1, 3: 0, 4: 3}, })
df_fever = pd.DataFrame({'min': {0: 0, 1: 35.1, 2: 36.1, 3: 37.8, 4: 38.1, 5: 39.1},
                         'max': {0: 35, 1: 36, 2: 37.7, 3: 38, 4: 39, 5: 45},
                         'severity': {0: 3, 1: 1, 2: 0, 3: 1, 4: 2, 5: 3}, })

df_breath_high_fever = pd.DataFrame({'min': {0: 0, 1: 7.7, 2: 10.2, 3: 17.8, 4: 20.5},
                                     'max': {0: 7.6, 1: 10.1, 2: 17.7, 3: 20.4, 4: 35},
                                     'severity': {0: 3, 1: 1, 2: 0, 3: 2, 4: 3}, })
df_BPM_high_fever = pd.DataFrame({'min': {0: 0, 1: 35, 2: 43.3, 3: 77.3, 4: 94.3, 5: 111.3},
                                  'max': {0: 34.9, 1: 43.2, 2: 77.2, 3: 94.2, 4: 111.2, 5: 200},
                                  'severity': {0: 3, 1: 1, 2: 0, 3: 1, 4: 2, 5: 3}, })
df_BloodPressure_high_fever = pd.DataFrame({'min': {0: 0, 1: 77.3, 2: 85.8, 3: 34.4, 4: 187},
                                            'max': {0: 77.2, 1: 85.7, 2: 94.3, 3: 186.9, 4: 300},
                                            'severity': {0: 3, 1: 2, 2: 1, 3: 0, 4: 3}, })

df_names = [df_breath, df_pso2, df_BloodPressure, df_BPM, df_fever]
df_names_low = [df_breath_high_fever, df_pso2, df_BloodPressure_high_fever, df_BPM_high_fever, df_fever]
measure_names = ['breath_rate', 'saturation', 'blood_pressure_h', 'bpm', 'fever']
score_record_names = ['BreathRate', 'SpO2', 'BloodPressure', 'BPM', 'Fever']


def get_prev_score(patient_id: str):
    my_list = []
    res = es.search(index='patient_status')
    for item in res['hits']['hits']:
        if (item['_source']['PatientID'] == patient_id):
            my_list.append(item['_source'])
    # Check if the current patient - has no prev records.
    if (my_list == []):
        return None

    data_sorted = sorted(my_list, key=lambda item: item['Timestamp'])

    return data_sorted[len(data_sorted) - 1]['Score']['Total']


def score_alert(prev_score, score_record):
    current_score = score_record['Score']['Total']
    desc = ''
    severity = 0
    # Deterioration
    if(current_score > prev_score):
        if(current_score >= 7):
            desc = 'Critical deterioration'
            severity = 3
        elif(current_score >= 5 and current_score <= 6):
            desc = 'Medium deterioration'
            severity = 2
        elif(current_score >= 2 and current_score <= 4):
            desc = 'Slight deterioration'
            severity = 1
    # Improvement
    elif(current_score < prev_score and current_score <= 2):
            desc = 'Improvement'
            severity = 0

    if(desc != ''):
        es.index(index='patient_event', id=score_record['Id'], body={'PtientId': score_record['PatientID'], 'Timestamp': score_record['Timestamp'], 'Event': desc, 'Severity': severity})


def scoring_measure(df_in_use, priority_names, i):
    return np.dot(
                (priority_names[i][measure_names[i]] >= df_in_use[i]['min'].values) &
                (priority_names[i][measure_names[i]] <= df_in_use[i]['max'].values),
                df_in_use[i]['severity'].values
            )


def initial_vars():
    return 0, 0, {}


def get_desired_data(record):
    record = ast.literal_eval(record.decode('ascii'))
    general_measure = record
    primary_measure = record['primery_priority']
    secondary_measure = record['secondery_priority']
    return record, general_measure, primary_measure,secondary_measure


def es_no_cache():
    # Refresh the ES indexes I use.
    es.indices.clear_cache(index='patient_status')
    es.indices.refresh(index="patient_status")

    es.indices.clear_cache(index='patient_event')
    es.indices.refresh(index="patient_event")


def update_expired_measure(id: str, patient_id:str, measure:str):
    # Removing the expired measure from 'LastKnown' index.
    current_patinet = r.hget('LastKnown', patient_id)
    current_patinet = ast.literal_eval(current_patinet.decode('ascii'))
    if(measure == 'breath_rate' or measure =='wheezing'):
        current_patinet['primery_priority'].pop(measure)
    else:
        current_patinet['secondery_priority'].pop(measure)
    r.hset('LastKnown', patient_id, str(current_patinet))

    # Issue a message to ES.
    es.index(index='patient_event', id=id, body={'PatientID': patient_id, 'Timestamp': ms_utc, 'Event': 'Over 12 hours without receiving new information about ' + measure + ' .', 'Severity': 1})


def get_expired_status(measure_datetime) -> bool:
    # A function gets the last datetime a measure was measures, and check if the measure has been expired.
    measure_datetime = int('1495072949453')
    diff = (ms_utc - measure_datetime)/3600000
    if(diff >= 12):
        return True
    return False


def check_expired():
    measure_check = measure_names + ['wheezing']
    for record in r.hvals('last_update'):
        # Get the records belong to the current patient_id from 'last_update' and 'LastKnown' redis indexes.
        record = ast.literal_eval(record.decode('ascii'))
        record = ast.literal_eval(r.hget('last_update', record['patientId']).decode('ascii'))
        last_known = r.hget('LastKnown', record['patientId'])
        last_known = ast.literal_eval(last_known.decode('ascii'))

        # Iterate over all measures I need to check the receiving data about them.
        for measure in measure_check:
            # Checks whether the measure has ever been measured for the current patient
            if(measure in record['updates']):
            # Check if the measure hasn't been remove from 'LastKnown' redis index (- every measure in its appropiate location in the dict.)
            # If yes, Check if the measure has been expired. If yes, update about an expired measure.
                if (measure == 'breath_rate' or measure == 'wheezing'):
                    if(measure in  last_known['primery_priority']):
                        if(get_expired_status(record['updates'][measure]) == True):
                            update_expired_measure(record['Id'],  record['patientId'],  measure)
                elif (measure in last_known['secondery_priority']):
                    if(get_expired_status(record['updates'][measure]) == True):
                        update_expired_measure(record['Id'], record['patientId'], measure)


def main_func():
    check_expired()

    # Getting the data from 'LastKnown' index in redis.
    records = r.hvals("LastKnown")

    # Iterate over the patients and scoring each one.
    for record in records:
        # Get the desired data about every patient.
        record, general_measure, primary_measure, secondary_measure = get_desired_data(record)

        # Creating the record (- document, in ES terminology.) for pushing to elastic patient_data table.
        score_record = {}
        score_record['Id'] = general_measure['Id']
        score_record['PatientID'] = general_measure['patientId']
        score_record['Timestamp'] = general_measure['timeTag']

        # Scoring patient measures.
        total_score, score_per_measure, measure_dict = initial_vars()

        # Check if this measure exists.
        if('age' in general_measure):
            if(general_measure['age'] < 65):
                if(general_measure['age'] < 44):
                    score_per_measure = 0
                else:
                    score_per_measure = 1
            else:
                score_per_measure = 3 
            measure_dict['AgeScore'] = score_per_measure
            total_score = total_score + score_per_measure

        # Every iteration, I have to update the var; as, the content of the measures changes ang they're not updated.
        priority_names = [primary_measure, secondary_measure, secondary_measure, secondary_measure, secondary_measure]

        # Check if the patient's fever is high, if yes - we have to use other df's for the scoring.
        if('fever' in secondary_measure):
            if(secondary_measure['fever'] > 37.5):
                df_in_use = df_names_low
            else:
                df_in_use = df_names
        else:
            df_in_use = df_names

        for i in range(len(measure_names)):
            # Check if the measure exists.
            if(measure_names[i] in priority_names[i]):
                score_per_measure=scoring_measure(df_in_use, priority_names, i)
                measure_dict[score_record_names[i]] = np.int(score_per_measure.item())
                total_score = total_score + score_per_measure

        # Check if this measure exists.
        if('wheezing' in primary_measure):
            score_per_measure = measure_dict['RespiratoryFindings'] = 0 if primary_measure['wheezing'] == False else 2
            total_score = total_score + score_per_measure

        measure_dict['Total'] = np.int(total_score.item())
        score_record['Score'] = measure_dict

        # Get previous score for the current patient, and if necessary - issue an appropriate message. (- It should work fine.)
        prev_score = get_prev_score(score_record['PatientID'])
        if(prev_score != None):
              score_alert(prev_score, score_record)

        # Writing the document to ES - Why doesn't it work ???
        es.index(index='patient_status', id=score_record['Id'], body=score_record)



main_func()

