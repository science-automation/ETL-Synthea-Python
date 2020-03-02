import pandas as pd
import datetime
import dateutil.parser

#
# given a synthea object, covert it to it's equivalent omop objects
#
class SyntheaToOmop:
    #
    # Check the model matches
    #
    def __init__(self, model_schema):
       self.model_schema = model_schema

    # hash function for patient id to convert synthea string to omop integer
    def patienthash(self, id):
        return hash(id) & ((1<<64)-1)

    # given date in synthea format return the year
    def getYearFromSyntheaDate(self, date):
        return datetime.datetime.strptime(date, "%Y-%m-%d").year

    # given date in synthea format return the month
    def getMonthFromSyntheaDate(self, date):
        return datetime.datetime.strptime(date, "%Y-%m-%d").month

    # given date in synthea format return the day
    def getDayFromSyntheaDate(self, date):
        return datetime.datetime.strptime(date, "%Y-%m-%d").day

    # given gender as M or F return the OMOP concept code
    def getGenderConceptCode(self, gender):
        gendre = gender.upper()
        if gender=='M':
            return '8507'
        elif gender=='F':
            return '8532'
        else:
            return 0

    # given synthea race code return omop code
    def getRaceConceptCode(self, race):
        race = race.upper()
        if race=='WHITE':
            return '8527'
        elif race=='BLACK':
            return '8516'
        elif race=='ASIAN':
            return 8515
        else:
            return '0'

    def getEthnicityConceptCode(self, eth):
        eth = eth.upper()
        #if race=='HISPANIC' or eth=='CENTRAL_AMERICAN' or eth=='DOMINICAN' or eth=='MEXICAN' or eth=='PUERTO_RICAN' or eth=='SOUTH_AMERICAN':
        if eth=='CENTRAL_AMERICAN' or eth=='DOMINICAN' or eth=='MEXICAN' or eth=='PUERTO_RICAN' or eth=='SOUTH_AMERICAN':
            return '38003563'
        else:
            return '0'

    # convert a synthea timestamp like 2020-02-16T05:05:49Z to omop datestamp like 2020-02-16
    def isoTimestampToDate(self, timestamp):
        date = dateutil.parser.parse(timestamp)
        return datetime.date.strftime(date, '%Y-%m-%d')

    # given a datestamp, return on timestamp with default 0 hour
    def getDefaultTimestamp(self, datestamp):
        return datestamp + " 00:00:00"

    #
    # synthea patients to omop
    #
    def patientsToOmop(self, df, personmap, person_id, location_id):
        #df = df.sort_values('Id') sort to get better match to original synthea to omop conversion for comparison
        df['persontmp'] = df.index + person_id # copy index into a temp column. If accessed directly corrupts dataframe
        df['locationtmp'] = df.index + location_id # copy index into a temp column. If accessed directly corrupts dataframe
        person = pd.DataFrame(columns=self.model_schema['person'].keys())
        person['person_id'] = df['persontmp']
        person['gender_concept_id'] = df['GENDER'].apply(self.getGenderConceptCode)
        person['year_of_birth'] = df['BIRTHDATE'].apply(self.getYearFromSyntheaDate)
        person['month_of_birth'] = df['BIRTHDATE'].apply(self.getMonthFromSyntheaDate)
        person['day_of_birth'] = df['BIRTHDATE'].apply(self.getDayFromSyntheaDate)
        person['race_concept_id'] =  df['RACE'].apply(self.getRaceConceptCode)
        person['ethnicity_concept_id'] = df['ETHNICITY'].apply(self.getEthnicityConceptCode)
        person['location_id'] = df['locationtmp']
        person['gender_source_value'] = df['GENDER']
        person['person_source_value'] = df['Id']
        person['gender_source_concept_id'] = '0'
        person['race_source_value'] = df['RACE']
        person['race_source_concept_id'] = '0'
        person['ethnicity_source_value'] = df['ETHNICITY']
        person['ethnicity_source_concept_id'] = '0'
        print(person.head(5))
        personappend = pd.DataFrame(columns=["person_id","synthea_patient_id"])
        personappend["person_id"] = person['person_id']
        personappend["synthea_patient_id"] = df['Id']
        personmap = personmap.append(personappend)
        person = person[person['gender_concept_id'] != 0]   # filter out person's with missing or unknown gender
        location = pd.DataFrame(columns=self.model_schema['location'].keys())
        location['location_id'] = df['locationtmp']
        location['address_1'] = df['ADDRESS']
        location['city'] = df['CITY']
        location['state'] = df['STATE']
        location['zip'] = df['ZIP']
        location['county'] = df['COUNTY']
        location['location_source_value'] = df['Id']
        death = pd.DataFrame(columns=self.model_schema['death'].keys())
        death['person_id'] = df['Id'].apply(self.patienthash)
        death['deathdate'] = df['DEATHDATE']
        death =  death[death.deathdate.notnull()]  # remove records where no death occurred
        return (person, location, death, personmap)

    def conditionsToOmop(self, df, srctostdvm, condition_occurrence_id, drug_exposure_id, observation_id, personmap, visitmap):
        df['conditiontmp'] = df.index + condition_occurrence_id # copy index into a temp column.
        df['drugexposuretmp'] = df.index + drug_exposure_id # copy index into a temp column.
        df['observationtmp'] = df.index + observation_id # copy index into a temp column. 
        df = pd.merge(df, personmap, left_on='PATIENT', right_on='synthea_patient_id', how='left')
        df = pd.merge(df, visitmap, left_on='ENCOUNTER', right_on='synthea_encounter_id', how='left')
        condition_occurrence = pd.DataFrame(columns=self.model_schema['condition_occurrence'].keys())
        condition_occurrence['condition_occurrence_id'] = df['conditiontmp']
        condition_occurrence['person_id'] = df['person_id']
        condition_occurrence['condition_concept_id'] = pd.merge(df['CODE'],srctostdvm[(srctostdvm["target_domain_id"]=='Condition') & (srctostdvm["target_vocabulary_id"]=='SNOMED') & (srctostdvm["source_vocabulary_id"]=='SNOMED') & (srctostdvm["target_standard_concept"]=='SNOMED') & (srctostdvm["target_invalid_reason"].isnull())], left_on='CODE', right_on='source_code', how='left')
        condition_occurrence['condition_concept_id'].fillna('0')
        condition_occurrence['condition_start_date'] = df['START']
        condition_occurrence['condition_end_date'] = df['STOP']
        condition_occurrence['condition_concept_id'] = df['CODE']
        condition_occurrence['condition_type_concept_id'] = '32020'
        condition_occurrence['stop_reason'] = '0'
        condition_occurrence['visit_occurrence_id'] = df['visit_occurrence_id']
        condition_occurrence['visit_detail_id'] = '0'
        condition_occurrence['condition_source_value'] = df['CODE']
        condition_occurrence['condition_source_concept_id'] = df['CODE']
        drug_exposure = pd.DataFrame(columns=self.model_schema['drug_exposure'].keys())
        drug_exposure['drug_exposure_id'] = df['drugexposuretmp'] 
        drug_exposure['person_id'] = df['person_id']
        drug_exposure['drug_exposure_start_date'] = df['START']
        drug_exposure['drug_exposure_end_date'] = df['STOP']
        drug_exposure['verbatim_end_date'] = df['STOP']
        drug_exposure['visit_occurrence_id'] = df['visit_occurrence_id']
        drug_exposure['drug_concept_id'] = df['CODE']
        drug_exposure['drug_source_value'] = df['CODE']
        drug_exposure['drug_source_concept_id'] = df['CODE']
        drug_exposure['drug_type_concept_id'] = '581452'
        drug_exposure['days_supply'] = '1' # how does synthea-etl handle days_supply for immunization?
        observation = pd.DataFrame(columns=self.model_schema['observation'].keys())
        observation['observation_id'] = df['observationtmp']
        observation['person_id'] = df['person_id']
        observation['observation_date'] = df['START']
        observation['visit_occurrence_id'] = df['visit_occurrence_id']
        observation['observation_concept_id'] = df['CODE']
        observation['observation_source_value'] = df['CODE']
        observation['observation_source_concept_id'] = df['CODE']
        observation['observation_type_concept_id'] = '38000280'
        return (condition_occurrence, drug_exposure, observation, condition_occurrence_id, drug_exposure_id, observation_id)

    def careplansToOmop(self, df):
        pass

    def observationsToOmop(self, df, measurement_id, personmap,visitmap):
        df['measurementtmp'] = df.index + measurement_id # copy index into a temp column.
        df = pd.merge(df, personmap, left_on='PATIENT', right_on='synthea_patient_id', how='left')
        df = pd.merge(df, visitmap, left_on='ENCOUNTER', right_on='synthea_encounter_id', how='left')
        measurement = pd.DataFrame(columns=self.model_schema['measurement'].keys())
        measurement['measurement_id'] = df['measurementtmp']
        measurement['person_id'] = df['person_id']
        measurement['measurement_date'] = df['DATE']
        measurement['measurement_datetime'] = df['DATE'].apply(self.getDefaultTimestamp)
        measurement['measurement_time'] = df['DATE']  # check
        measurement['visit_occurrence_id'] = df['visit_occurrence_id']
        measurement['visit_detail_id'] = '0'
        measurement['measurement_concept_id'] = df['CODE']
        measurement['measurement_source_value'] = df['CODE']
        measurement['measurement_source_concept_id'] = df['CODE']
        measurement['measurement_type_concept_id'] = '5001'
        measurement['operator_concept_id'] = '0'
        measurement['value_as_number'] = df['VALUE']
        measurement['value_as_concept_id'] = '0'
        measurement['unit_source_value'] = df['UNITS']
        measurement['value_source_value'] = df['VALUE']
        return measurement

    def proceduresToOmop(self, df, procedure_occurrence_id, personmap):
        df['proceduretmp'] = df.index + procedure_occurrence_id # copy index into a temp column.
        df = pd.merge(df, personmap, left_on='PATIENT', right_on='synthea_patient_id', how='left')
        # do procedures really map to measurements?  There is no value and units?
        #measurement = pd.DataFrame(columns=self.model_schema['measurement'].keys())
        #measurement['person_id'] = df['PATIENT'].apply(self.patienthash)
        #measurement['measurement_date'] = df['DATE']
        #measurement['measurement_time'] = df['DATE']  # check
        #measurement['value_as_number'] = df['VALUE']
        #measurement['visit_occurrence_id'] = df['CODE']
        #measurement['measurement_concept_id'] = df['CODE']
        #measurement['measurement_type_concept_id'] = '5001'
        #measurement['measurement_source_value'] = df['CODE']
        #measurement['measurement_source_concept_id'] = df['CODE']
        #measurement['unit_source_value'] = df['UNITS']
        #measurement['value_source_value'] = df['VALUE']
        procedure_occurrence = pd.DataFrame(columns=self.model_schema['procedure_occurrence'].keys())
        procedure_occurrence['procedure_occurrence_id'] = df['proceduretmp']
        procedure_occurrence['person_id'] = df['person_id']
        procedure_occurrence['procedure_date'] = df['DATE']
        procedure_occurrence['visit_occurrence_id'] = df['ENCOUNTER']
        procedure_occurrence['procedure_concept_id'] = df['CODE']
        procedure_occurrence['procedure_source_value'] = df['CODE']
        procedure_occurrence['procedure_source_concept_id'] = df['CODE']
        return procedure_occurrence

    def immunizationsToOmop(self, df, drug_exposure_id, personmap):
        df['drugexposuretmp'] = df.index + drug_exposure_id # copy index into a temp column.
        df = pd.merge(df, personmap, left_on='PATIENT', right_on='synthea_patient_id', how='left')
        drug_exposure = pd.DataFrame(columns=self.model_schema['drug_exposure'].keys())
        drug_exposure['drug_exposure_id'] = df['drugexposuretmp']
        drug_exposure['person_id'] = df['person_id']
        drug_exposure['drug_exposure_start_date'] = df['DATE']
        drug_exposure['drug_exposure_end_date'] = df['DATE']
        drug_exposure['verbatim_end_date'] = df['DATE']
        drug_exposure['visit_occurrence_id'] = df['ENCOUNTER']
        drug_exposure['drug_concept_id'] = df['CODE']
        drug_exposure['drug_source_value'] = df['CODE']
        drug_exposure['drug_source_concept_id'] = df['CODE']
        drug_exposure['drug_type_concept_id'] = '581452'
        drug_exposure['days_supply'] = '1' # how does synthea-etl handle days_supply for immunization?
        return drug_exposure

    def encountersToOmop(self, df, observation_period_id, visit_occurrence_id, personmap, visitmap):
        df['visittmp'] = df.index + visit_occurrence_id # copy index into a temp column.
        df = pd.merge(df, personmap, left_on='PATIENT', right_on='synthea_patient_id', how='left')
        # preprocess df 
        df['observation_period_start_date'] = df['START'].apply(self.isoTimestampToDate)
        df['observation_period_end_date'] = df['STOP'].apply(self.isoTimestampToDate)
        start = df.groupby('person_id')['observation_period_start_date'].agg(['first']).reset_index()
        stop = df.groupby('person_id')['observation_period_end_date'].agg(['last']).reset_index()
        observation_tmp = pd.merge(start, stop, on='person_id', how='inner')
        observation_period = pd.DataFrame(columns=self.model_schema['observation_period'].keys())
        observation_period['observationtmp'] = observation_tmp.index + observation_period_id
        observation_period['observation_period_id'] = observation_period['observationtmp']
        observation_period['person_id'] = observation_tmp['person_id']
        observation_period['observation_period_start_date'] = observation_tmp['first']
        observation_period['observation_period_end_date'] = observation_tmp['last']
        observation_period['period_type_concept_id'] = '44814724'
        observation_period = observation_period.drop('observationtmp', 1)
        observation_period_id = observation_period_id + len(observation_period)
        visit_occurrence = pd.DataFrame(columns=self.model_schema['visit_occurrence'].keys())
        visit_occurrence['visit_occurrence_id'] = df['visittmp']
        visit_occurrence['person_id'] = df['person_id']
        visit_occurrence['visit_start_date'] = df['START']
        visit_occurrence['visit_end_date'] = df['STOP']
        visit_occurrence['visit_concept_id'] = df['ENCOUNTERCLASS']
        visit_occurrence['visit_source_value'] = df['ENCOUNTERCLASS']
        visit_occurrence['visit_type_concept_id'] = '44818517'
        visitappend = pd.DataFrame(columns=["visit_occurrence_id","synthea_encounter_id"])
        visitappend["visit_occurrence_id"] = visit_occurrence['visit_occurrence_id']
        visitappend["synthea_encounter_id"] = df['Id']
        visitmap = visitmap.append(visitappend)
        return (observation_period, visit_occurrence, observation_period_id, visitmap)

    def organizationsToOmop(self, df, care_site_id):
        care_site = pd.DataFrame(columns=self.model_schema['care_site'].keys())
        return care_site

    def providersToOmop(self, df, provider_id):
        provider = pd.DataFrame(columns=self.model_schema['provider'].keys())
        return provider

    def payertransitionToOmop(self, df):
        pass

    def allergiesToOmop(self, df, observation_id, personmap):
        observation = pd.DataFrame(columns=self.model_schema['observation'].keys())
        return observation

    def medicationsToOmop(self, df, drug_exposure_id, personmap):
        df['drugexposuretmp'] = df.index + drug_exposure_id # copy index into a temp column.
        df = pd.merge(df, personmap, left_on='PATIENT', right_on='synthea_patient_id', how='left')
        drug_exposure = pd.DataFrame(columns=self.model_schema['drug_exposure'].keys())
        drug_exposure['drug_exposure_id'] = df['drugexposuretmp']
        drug_exposure['person_id'] = df['person_id']
        drug_exposure['drug_exposure_start_date'] = df['START']
        drug_exposure['drug_exposure_end_date'] = df['STOP']
        drug_exposure['verbatim_end_date'] = df['STOP']
        drug_exposure['visit_occurrence_id'] = df['ENCOUNTER']
        drug_exposure['drug_concept_id'] = df['CODE']
        drug_exposure['drug_source_vaule'] = df['CODE']
        drug_exposure['drug_source_concept_id'] = df['CODE']
        drug_exposure['drug_type_concept_id'] = '38000177'
        drug_exposure['days_supply'] = '1' # how does synthea-etl handle days_supply for medication?
        return drug_exposure
