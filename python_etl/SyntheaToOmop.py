import pandas as pd
import datetime

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
        else:
            return '8532'
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
        if eth=='HISPANIC':
            return '38003563'
        else:
            return '0'
    #
    # synthea patients to omop
    #
    def patientsToOmop(self, df):
        model_schema = {}
        #
        # Standardized vocabulary
        person = pd.DataFrame(columns=self.model_schema['person'].keys())
        person['person_id'] = df['Id'].apply(self.patienthash)
        person['gender_concept_id'] = df['GENDER'].apply(self.getGenderConceptCode)
        person['year_of_birth'] = df['BIRTHDATE'].apply(self.getYearFromSyntheaDate)
        person['month_of_birth'] = df['BIRTHDATE'].apply(self.getMonthFromSyntheaDate)
        person['day_of_birth'] = df['BIRTHDATE'].apply(self.getDayFromSyntheaDate)
        person['race_concept_id'] =  df['RACE'].apply(self.getRaceConceptCode)
        person['ethnicity_concept_id'] = df['ETHNICITY'].apply(self.getEthnicityConceptCode)
        person['location_id'] = df['Id'].apply(self.patienthash)
        person['person_source_value'] = df['Id']
        person['race_source_value'] = df['RACE']
        person['ethnicity_source_value'] = df['ETHNICITY']

        # create location record
        location = pd.DataFrame(columns=self.model_schema['location'].keys())
        location['location_id'] = df['Id'].apply(self.patienthash)
        location['address_1'] = df['ADDRESS']
        location['city'] = df['CITY']
        location['state'] = df['STATE']
        location['zip'] = df['ZIP']
        location['county'] = df['COUNTY']
        location['location_source_value'] = df['Id']

        # create death record
        death = pd.DataFrame(columns=self.model_schema['death'].keys())
        death['person_id'] = df['Id'].apply(self.patienthash)
        death['deathdate'] = df['DEATHDATE']
        death =  death[death.deathdate.notnull()]
        return (person, location, death)

    def conditionsToOmop(self, df):
        condition_occurrence = pd.DataFrame(columns=self.model_schema['condition_occurrence'].keys())
        condition_occurrence['person_id'] = df['PATIENT'].apply(self.patienthash)
        condition_occurrence['condition_start_date'] = df['START']
        condition_occurrence['condition_end_date'] = df['STOP']
        condition_occurrence['visit_occurrence_id'] = df['ENCOUNTER']
        condition_occurrence['condition_concept_id'] = df['CODE']
        condition_occurrence['condition_source_value'] = df['CODE']
        condition_occurrence['condition_source_concept_id'] = df['CODE']
        condition_occurrence['condition_type_concept_id'] = '32020'
        drug_exposure = pd.DataFrame(columns=self.model_schema['drug_exposure'].keys())
        observation = pd.DataFrame(columns=self.model_schema['observation'].keys())
        return (condition_occurrence, drug_exposure, observation)

    def careplansToOmop(self, df):
        pass

    def observationsToOmop(self, df):
        measurement = pd.DataFrame(columns=self.model_schema['measurement'].keys())
        return measurement

    def proceduresToOmop(self, df):
        measurement = pd.DataFrame(columns=self.model_schema['measurement'].keys())
        procedure_occurrence = pd.DataFrame(columns=self.model_schema['procedure_occurrence'].keys())
        procedure_occurrence['person_id'] = df['PATIENT'].apply(self.patienthash)
        procedure_occurrence['procedure_date'] = df['DATE']
        procedure_occurrence['visit_occurrence_id'] = df['ENCOUNTER']
        procedure_occurrence['procedure_concept_id'] = df['CODE']
        procedure_occurrence['procedure_source_value'] = df['CODE']
        procedure_occurrence['procedure_source_concept_id'] = df['CODE']
        return (measurement, procedure_occurrence)

    def immunizationsToOmop(self, df):
        drug_exposure = pd.DataFrame(columns=self.model_schema['drug_exposure'].keys())
        drug_exposure['person_id'] = df['PATIENT'].apply(self.patienthash)
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

    def encountersToOmop(self, df):
        observation_period = pd.DataFrame(columns=self.model_schema['observation_period'].keys())
        observation_period['person_id'] = df['PATIENT'].apply(self.patienthash)
        observation_period['observation_period_start_date'] = df['START']
        observation_period['observation_period_end_date'] = df['STOP']
        observation_period['period_type_concept_id'] = '44814724'
        visit_occurrence = pd.DataFrame(columns=self.model_schema['visit_occurrence'].keys())
        return (observation_period, visit_occurrence)

    def organizationsToOmop(self, df):
        care_site = pd.DataFrame(columns=self.model_schema['care_site'].keys())
        return care_site

    def providersToOmop(self, df):
        provider = pd.DataFrame(columns=self.model_schema['provider'].keys())
        return provider

    def payertransitionToOmop(self, df):
        pass

    def allergiesToOmop(self, df):
        observation = pd.DataFrame(columns=self.model_schema['observation'].keys())
        return observation

    def medicationsToOmop(self, df):
        drug_exposure = pd.DataFrame(columns=self.model_schema['drug_exposure'].keys())
        drug_exposure['person_id'] = df['PATIENT'].apply(self.patienthash)
        drug_exposure['drug_exposure_start_date'] = df['START']
        drug_exposure['drug_exposure_end_date'] = df['STOP']
        drug_exposure['verbatim_end_date'] = df['STOP']
        drug_exposure['visit_occurrence_id'] = df['encounter']
        drug_exposure['drug_concept_id'] = df['CODE']
        drug_exposure['drug_source_vaule'] = df['CODE']
        drug_exposure['drug_source_concept_id'] = df['CODE']
        drug_exposure['drug_type_concept_id'] = '38000177'
        drug_exposure['days_supply'] = '1' # how does synthea-etl handle days_supply for medication?
        return drug_exposure
