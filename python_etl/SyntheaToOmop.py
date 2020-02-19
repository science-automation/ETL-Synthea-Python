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
    def syntheaPatientsToOmop(self, df):
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
