import pandas as pd
import datetime
import dateutil.parser
import Utils

#
# given a synthea object, covert it to it's equivalent omop objects
#
class SyntheaToOmop6:
    #
    # Check the model matches
    #
    def __init__(self, model_schema, utils):
       self.model_schema = model_schema
       self.utils = utils

    #
    # synthea patients to omop
    #
    def patientsToOmop(self, df, personmap, person_id, location_id):
        #df = df.sort_values('Id') sort to get better match to original synthea to omop conversion for comparison
        df['persontmp'] = df.index + person_id # copy index into a temp column. If accessed directly corrupts dataframe
        df['locationtmp'] = df.index + location_id # copy index into a temp column. If accessed directly corrupts dataframe
        person = pd.DataFrame(columns=self.model_schema['person'].keys())
        person['person_id'] = df['persontmp']
        person['gender_concept_id'] = df['GENDER'].apply(self.utils.getGenderConceptCode)
        person['year_of_birth'] = df['BIRTHDATE'].apply(self.utils.getYearFromSyntheaDate)
        person['month_of_birth'] = df['BIRTHDATE'].apply(self.utils.getMonthFromSyntheaDate)
        person['day_of_birth'] = df['BIRTHDATE'].apply(self.utils.getDayFromSyntheaDate)
        person['race_concept_id'] =  df['RACE'].apply(self.utils.getRaceConceptCode)
        person['ethnicity_concept_id'] = df['ETHNICITY'].apply(self.utils.getEthnicityConceptCode)
        person['birth_datetime'] = df['BIRTHDATE'].apply(self.utils.getDefaultTimestamp)
        person['death_datetime'] = df['DEATHDATE'].apply(self.utils.getDefaultTimestamp)
        person['location_id'] = df['locationtmp']
        person['gender_source_value'] = df['GENDER']
        person['person_source_value'] = df['Id']
        person['gender_source_concept_id'] = '0'
        person['race_source_value'] = df['RACE']
        person['race_source_concept_id'] = '0'
        person['ethnicity_source_value'] = df['ETHNICITY']
        person['ethnicity_source_concept_id'] = '0'
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
        location['latitude'] = df['LAT']
        location['longitude'] = df['LON']
        # create empty death dataframe
        death = pd.DataFrame()
        return (person, location, death, personmap, person_id + len(person), location_id + len(location))

    def conditionsToOmop(self, df, srctostdvm, condition_occurrence_id, drug_exposure_id, observation_id, personmap, visitmap):
        df['conditiontmp'] = df.index + condition_occurrence_id # copy index into a temp column.
        df['drugexposuretmp'] = df.index + drug_exposure_id # copy index into a temp column.
        df['observationtmp'] = df.index + observation_id # copy index into a temp column. 
        df = pd.merge(df, personmap, left_on='PATIENT', right_on='synthea_patient_id', how='left')
        df = pd.merge(df, visitmap, left_on='ENCOUNTER', right_on='synthea_encounter_id', how='left')
        condition_occurrence = pd.DataFrame(columns=self.model_schema['condition_occurrence'].keys())
        condition_occurrence['condition_occurrence_id'] = df['conditiontmp']
        condition_occurrence['person_id'] = df['person_id']
        srctostdvm_filtered = srctostdvm[(srctostdvm["target_domain_id"]=='Condition') & (srctostdvm["target_vocabulary_id"]=='SNOMED') & (srctostdvm["target_standard_concept"]=='S') & (srctostdvm["target_invalid_reason"].isnull())]
        concept_df = pd.merge(df['CODE'],srctostdvm_filtered[['source_code','target_concept_id']], left_on='CODE', right_on='source_code', how='left')
        condition_occurrence['condition_concept_id'] = concept_df['target_concept_id'].fillna('0').astype(int)
        condition_occurrence['condition_start_date'] = df['START']
        condition_occurrence['condition_start_datetime'] = df['START'].apply(self.utils.getDefaultTimestamp)
        condition_occurrence['condition_end_date'] = df['STOP']
        condition_occurrence['condition_end_datetime'] = df['STOP'].apply(self.utils.getDefaultTimestamp)
        condition_occurrence['condition_type_concept_id'] = '32020'
        condition_occurrence['stop_reason'] = '0'
        condition_occurrence['visit_occurrence_id'] = df['visit_occurrence_id']
        condition_occurrence['visit_detail_id'] = '0'
        condition_occurrence['condition_source_value'] = df['CODE']
        condition_occurrence['condition_source_concept_id'] = df['CODE']
        drug_exposure = pd.DataFrame(columns=self.model_schema['drug_exposure'].keys())
        drug_exposure['drug_exposure_id'] = df['drugexposuretmp'] 
        drug_exposure['person_id'] = df['person_id']
        srctostdvm_filtered = srctostdvm[(srctostdvm["target_domain_id"]=='Drug') & (srctostdvm["target_vocabulary_id"]=='RxNorm') & (srctostdvm["target_standard_concept"]=='S') & (srctostdvm["target_invalid_reason"].isnull())]
        concept_df = pd.merge(df['CODE'],srctostdvm_filtered[['source_code','target_concept_id']], left_on='CODE', right_on='source_code', how='left')
        drug_exposure['drug_concept_id'] = concept_df['target_concept_id'].fillna('0').astype(int)
        drug_exposure['drug_exposure_start_date'] = df['START']
        drug_exposure['drug_exposure_start_datetime'] = df['START'].apply(self.utils.getDefaultTimestamp)
        drug_exposure['drug_exposure_end_date'] = df['STOP']
        drug_exposure['drug_exposure_end_datetime'] = df['STOP'].apply(self.utils.getDefaultTimestamp)
        drug_exposure['verbatim_end_date'] = df['STOP']
        drug_exposure['visit_occurrence_id'] = df['visit_occurrence_id']
        drug_exposure['drug_source_value'] = df['CODE']
        drug_exposure['drug_source_concept_id'] = df['CODE']
        drug_exposure['drug_type_concept_id'] = '581452'
        drug_exposure['refills'] = '0'
        drug_exposure['quantity'] = '0'
        drug_exposure['days_supply'] = '0'
        drug_exposure['route_concept_id'] = '0'
        drug_exposure['lot_number'] = '0'
        drug_exposure['visit_detail_id'] = '0'
        observation = pd.DataFrame(columns=self.model_schema['observation'].keys())
        observation['observation_id'] = df['observationtmp']
        observation['person_id'] = df['person_id']
        srctostdvm_filtered = srctostdvm[(srctostdvm["target_domain_id"]=='Observation') & (srctostdvm["target_vocabulary_id"]=='SNOMED') & (srctostdvm["target_invalid_reason"].isnull())]
        concept_df = pd.merge(df['CODE'],srctostdvm_filtered[['source_code','target_concept_id']], left_on='CODE', right_on='source_code', how='left')
        observation['observation_concept_id'] = concept_df['target_concept_id'].fillna('0').astype(int)
        observation['observation_date'] = df['START']
        observation['observation_datetime'] = df['START'].apply(self.utils.getDefaultTimestamp)
        observation['value_as_concept_id'] = '0'
        observation['qualifier_concept_id'] = '0'
        observation['unit_concept_id'] = '0'
        observation['visit_occurrence_id'] = df['visit_occurrence_id']
        observation['visit_detail_id'] = '0'
        observation['observation_source_value'] = df['CODE']
        observation['observation_source_concept_id'] = df['CODE']
        observation['observation_type_concept_id'] = '38000280'
        return (condition_occurrence, drug_exposure, observation, condition_occurrence_id + len(condition_occurrence) , drug_exposure_id + len(drug_exposure), observation_id + len(observation))

    def careplansToOmop(self, df):
        pass

    def observationsToOmop(self, df, srctostdvm, srctosrcvm, measurement_id, personmap,visitmap):
        # filter synthea observations with no encounter (original etl does this)
        df['measurementtmp'] = df.index + measurement_id # copy index into a temp column.
        df = df[~df.ENCOUNTER.isnull()]
        df = pd.merge(df, personmap, left_on='PATIENT', right_on='synthea_patient_id', how='left')
        df = pd.merge(df, visitmap, left_on='ENCOUNTER', right_on='synthea_encounter_id', how='left')
        measurement = pd.DataFrame(columns=self.model_schema['measurement'].keys())
        measurement['measurement_id'] = df['measurementtmp']
        measurement['person_id'] = df['person_id']
        measurement['measurement_date'] = df['DATE']
        measurement['measurement_datetime'] = df['DATE'].apply(self.utils.getDefaultTimestamp)
        measurement['measurement_time'] = df['DATE']  # check
        measurement['visit_occurrence_id'] = df['visit_occurrence_id']
        measurement['visit_detail_id'] = '0'
        srctostdvm_filtered = srctostdvm[(srctostdvm["target_domain_id"]=='Measurement') & (srctostdvm["target_standard_concept"]=='S') & (srctostdvm["target_invalid_reason"].isnull())]
        concept_df = pd.merge(df[['CODE']],srctostdvm_filtered[['source_code','target_concept_id']], left_on='CODE', right_on='source_code', how='left')
        measurement['measurement_concept_id'] = concept_df['target_concept_id'].fillna('0').astype(int)
        measurement['measurement_source_value'] = df['CODE']
        measurement['measurement_source_concept_id'] = df['CODE']
        measurement['measurement_type_concept_id'] = '5001'
        measurement['operator_concept_id'] = '0'
        measurement['value_as_number'] = df['VALUE']
        measurement['value_as_concept_id'] = '0'
        measurement['unit_source_value'] = df['UNITS']
        measurement['value_source_value'] = df['VALUE']
        return (measurement, measurement_id + len(measurement))

    def proceduresToOmop(self, df, srctostdvm, procedure_occurrence_id, personmap, visitmap):
        df['proceduretmp'] = df.index + procedure_occurrence_id # copy index into a temp column.
        df = pd.merge(df, personmap, left_on='PATIENT', right_on='synthea_patient_id', how='left')
        df = pd.merge(df, visitmap, left_on='ENCOUNTER', right_on='synthea_encounter_id', how='left')
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
        srctostdvm_filtered = srctostdvm[(srctostdvm["target_domain_id"]=='Procedure') & (srctostdvm["target_vocabulary_id"]=='SNOMED') & (srctostdvm["target_standard_concept"]=='S') & (srctostdvm["target_invalid_reason"].isnull())]
        concept_df = pd.merge(df[['CODE']],srctostdvm_filtered[['source_code','target_concept_id']], left_on='CODE', right_on='source_code', how='left')
        procedure_occurrence['procedure_concept_id'] = concept_df['target_concept_id'].fillna('0').astype(int)
        procedure_occurrence['procedure_date'] = df['DATE']
        procedure_occurrence['procedure_datetime'] = df['DATE'].apply(self.utils.getDefaultTimestamp)
        procedure_occurrence['visit_occurrence_id'] = df['visit_occurrence_id']
        procedure_occurrence['visit_detail_id'] = '0'
        procedure_occurrence['procedure_type_concept_id'] = '38000275'
        procedure_occurrence['modifier_concept_id'] = '0'
        procedure_occurrence['procedure_source_value'] = df['CODE']
        procedure_occurrence['procedure_source_concept_id'] = df['CODE']
        return (procedure_occurrence, procedure_occurrence_id + len(procedure_occurrence))

    def immunizationsToOmop(self, df, srctostdvm, drug_exposure_id, personmap, visitmap):
        df['drugexposuretmp'] = df.index + drug_exposure_id # copy index into a temp column.
        df = pd.merge(df, personmap, left_on='PATIENT', right_on='synthea_patient_id', how='left')
        df = pd.merge(df, visitmap, left_on='ENCOUNTER', right_on='synthea_encounter_id', how='left')
        drug_exposure = pd.DataFrame(columns=self.model_schema['drug_exposure'].keys())
        drug_exposure['drug_exposure_id'] = df['drugexposuretmp']
        drug_exposure['person_id'] = df['person_id']
        srctostdvm_filtered = srctostdvm[(srctostdvm["target_domain_id"]=='Drug') & (srctostdvm["target_vocabulary_id"]=='CVX') & (srctostdvm["target_standard_concept"]=='S') & (srctostdvm["target_invalid_reason"].isnull())]
        concept_df = pd.merge(df['CODE'],srctostdvm_filtered[['source_code','target_concept_id']], left_on='CODE', right_on='source_code', how='left')
        drug_exposure['drug_concept_id'] = concept_df['target_concept_id'].fillna('0').astype(int)
        drug_exposure['drug_exposure_start_date'] = df['DATE']
        drug_exposure['drug_exposure_start_datetime'] = df['DATE'].apply(self.utils.getDefaultTimestamp)
        drug_exposure['drug_exposure_end_date'] = df['DATE']
        drug_exposure['drug_exposure_end_datetime'] = df['DATE'].apply(self.utils.getDefaultTimestamp)
        drug_exposure['verbatim_end_date'] = df['DATE']
        drug_exposure['visit_occurrence_id'] = df['visit_occurrence_id']
        drug_exposure['drug_source_value'] = df['CODE']
        drug_exposure['drug_source_concept_id'] = df['CODE']
        drug_exposure['drug_type_concept_id'] = '581452'
        drug_exposure['refills'] = '0'
        drug_exposure['quantity'] = '0'
        drug_exposure['days_supply'] = '0'
        drug_exposure['route_concept_id'] = '0'
        drug_exposure['lot_number'] = '0'
        drug_exposure['visit_detail_id'] = '0'
        return (drug_exposure, drug_exposure_id + len(drug_exposure))

    def encountersToOmop(self, df, observation_period_id, visit_occurrence_id, personmap, visitmap):
        df['visittmp'] = df.index + visit_occurrence_id # copy index into a temp column.
        df = pd.merge(df, personmap, left_on='PATIENT', right_on='synthea_patient_id', how='left')
        # preprocess df 
        df['observation_period_start_date'] = df['START'].apply(self.utils.isoTimestampToDate)
        df['observation_period_end_date'] = df['STOP'].apply(self.utils.isoTimestampToDate)
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
        visit_occurrence['visit_concept_id'] = df['ENCOUNTERCLASS'].apply(self.utils.getVisitConcept)
        visit_occurrence['visit_source_value'] = df['ENCOUNTERCLASS']
        visit_occurrence['visit_type_concept_id'] = '44818517'
        visitappend = pd.DataFrame(columns=["visit_occurrence_id","synthea_encounter_id"])
        visitappend["visit_occurrence_id"] = visit_occurrence['visit_occurrence_id']
        visitappend["synthea_encounter_id"] = df['Id']
        visitmap = visitmap.append(visitappend)
        return (observation_period, visit_occurrence, visit_occurrence_id + len(visit_occurrence), observation_period_id + len(observation_period), visitmap)

    def organizationsToOmop(self, df, care_site_id):
        care_site = pd.DataFrame(columns=self.model_schema['care_site'].keys())
        return (care_site, care_site_id + len(care_site))

    def providersToOmop(self, df, provider_id):
        provider = pd.DataFrame(columns=self.model_schema['provider'].keys())
        return (provider, provider_id)

    def payertransitionToOmop(self, df):
        pass

    def allergiesToOmop(self, df, srctostdvm, observation_id, personmap, visitmap):
        df['observationtmp'] = df.index + observation_id # copy index into a temp column.
        df = pd.merge(df, personmap, left_on='PATIENT', right_on='synthea_patient_id', how='left')
        df = pd.merge(df, visitmap, left_on='ENCOUNTER', right_on='synthea_encounter_id', how='left')
        observation = pd.DataFrame(columns=self.model_schema['observation'].keys())
        observation['observation_id'] = df['observationtmp']
        observation['person_id'] = df['person_id']
        srctostdvm_filtered = srctostdvm[(srctostdvm["target_domain_id"]=='Observation') & (srctostdvm["target_vocabulary_id"]=='SNOMED') & (srctostdvm["target_invalid_reason"].isnull())]
        concept_df = pd.merge(df['CODE'],srctostdvm_filtered[['source_code','target_concept_id']], left_on='CODE', right_on='source_code', how='left')
        observation['observation_concept_id'] = concept_df['target_concept_id'].fillna('0').astype(int)
        observation['observation_date'] = df['START']
        observation['observation_datetime'] = df['START'].apply(self.utils.getDefaultTimestamp)
        observation['value_as_concept_id'] = '0'
        observation['qualifier_concept_id'] = '0'
        observation['unit_concept_id'] = '0'
        observation['visit_occurrence_id'] = df['visit_occurrence_id']
        observation['visit_detail_id'] = '0'
        observation['observation_source_value'] = df['CODE']
        observation['observation_source_concept_id'] = df['CODE']
        observation['observation_type_concept_id'] = '38000280'        
        return (observation, observation_id + len(observation))

    def medicationsToOmop(self, df, srctostdvm, drug_exposure_id, personmap):
        df['drugexposuretmp'] = df.index + drug_exposure_id # copy index into a temp column.
        df = pd.merge(df, personmap, left_on='PATIENT', right_on='synthea_patient_id', how='left')
        drug_exposure = pd.DataFrame(columns=self.model_schema['drug_exposure'].keys())
        drug_exposure['drug_exposure_id'] = df['drugexposuretmp']
        drug_exposure['person_id'] = df['person_id']
        srctostdvm_filtered = srctostdvm[(srctostdvm["target_domain_id"]=='Drug') & (srctostdvm["target_vocabulary_id"]=='RxNorm') & (srctostdvm["target_standard_concept"]=='S') & (srctostdvm["target_invalid_reason"].isnull())]
        concept_df = pd.merge(df['CODE'],srctostdvm_filtered[['source_code','target_concept_id']], left_on='CODE', right_on='source_code', how='left')
        drug_exposure['drug_concept_id'] = concept_df['target_concept_id'].fillna('0').astype(int)
        drug_exposure['drug_exposure_start_date'] = df['START']
        drug_exposure['drug_exposure_end_date'] = df['STOP']
        drug_exposure['verbatim_end_date'] = df['STOP']
        drug_exposure['visit_occurrence_id'] = df['ENCOUNTER']
        drug_exposure['drug_source_value'] = df['CODE']
        drug_exposure['drug_source_concept_id'] = df['CODE']
        drug_exposure['drug_type_concept_id'] = '38000177'
        drug_exposure['days_supply'] = '1' # how does synthea-etl handle days_supply for medication?
        return (drug_exposure, drug_exposure_id + len(drug_exposure))
