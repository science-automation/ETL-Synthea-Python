import pandas as pd
import os
import dotenv
import ModelSyntheaPandas
import ModelOmopPandas
import Utils
import datetime

#------------------------------------------------------
# This scripts performs an ETL from Synthea to omop CDM.
# It uses pandas dataframes
#------------------------------------------------------

# ------------------------
# load env
# ------------------------
dotenv.load_dotenv(".env")

# -----------------------------------
# - Configuration
# -----------------------------------

# Edit your .env file to change which directories to use in the ETL process
# Path to the directory where control files should be saved (input/output
BASE_ETL_CONTROL_DIRECTORY      = os.environ['BASE_ETL_CONTROL_DIRECTORY']
# Path to the directory containing the downloaded SynPUF files
BASE_SYNTHEA_INPUT_DIRECTORY     = os.environ['BASE_SYNTHEA_INPUT_DIRECTORY']
# Path to the directory containing the OMOP Vocabulary v5 files (can be downloaded from http://www.ohdsi.org/web/athena/)
BASE_OMOP_INPUT_DIRECTORY       = os.environ['BASE_OMOP_INPUT_DIRECTORY']
# Path to the directory where CDM-compatible CSV files should be saved
BASE_OUTPUT_DIRECTORY           = os.environ['BASE_OUTPUT_DIRECTORY']
# List of synthea input files
SYNTHEA_FILE_LIST =  ['patients','conditions','careplans','observations','procedures','immunizations','imaging_studies','imaging_studies','encounters','organizations','providers','payer_transitions','allergies','medications']
SYNTHEA_FILE_LIST =  ['patients']
# Synthea input file chunk size.
INPUT_CHUNK_SIZE = int(os.environ['INPUT_CHUNK_SIZE'])


# hash function for patient id to convert synthea string to omop integer
def patienthash(id):
    return hash(id) & ((1<<64)-1)

# given date in synthea format return the year
def getYearFromSyntheaDate(date):
    return datetime.datetime.strptime(date, "%Y-%m-%d").year

# given date in synthea format return the month
def getMonthFromSyntheaDate(date):
    return datetime.datetime.strptime(date, "%Y-%m-%d").month

# given date in synthea format return the day
def getDayFromSyntheaDate(date):
    return datetime.datetime.strptime(date, "%Y-%m-%d").day

# given gender as M or F return the OMOP concept code
def getGenderConceptCode(gender):
    gendre = gender.upper()
    if gender=='M':
        return '8507'
    else:
        return '8532'

# given synthea race code return omop code
def getRaceConceptCode(race):
    race = race.upper()
    if race=='WHITE':
        return '8527'
    elif race=='BLACK':
        return '8516'
    elif race=='ASIAN':
        return 8515
    else:
        return '0'

def getEthnicityConceptCode(eth):
    eth = eth.upper()
    if eth=='HISPANIC':
        return '38003563'
    else:
        return '0'

#---------------------------------
# start of the program
#---------------------------------
if __name__ == '__main__':
    if not os.path.exists(BASE_OUTPUT_DIRECTORY): os.makedirs(BASE_OUTPUT_DIRECTORY)
    if not os.path.exists(BASE_ETL_CONTROL_DIRECTORY): os.makedirs(BASE_ETL_CONTROL_DIRECTORY)

    print('BASE_SYNTHEA_INPUT_DIRECTORY     =' + BASE_SYNTHEA_INPUT_DIRECTORY)
    print('BASE_OUTPUT_DIRECTORY           =' + BASE_OUTPUT_DIRECTORY)
    print('BASE_ETL_CONTROL_DIRECTORY      =' + BASE_ETL_CONTROL_DIRECTORY)

    # load utils
    util = Utils.Utils()

    # check files look ok

    # load the synthea model
    model_synthea = ModelSyntheaPandas.ModelSyntheaPandas()
    model_omop = ModelOmopPandas.ModelOmopPandas()
    #model_synthea = ModelSyntheaPandasObject.ModelSyntheaPandas()

    # we only need to consider one synthea input file at a time to make the mapping
    # so only put one in memory at a time.  
    # we can read csv in chunks
    for datatype in SYNTHEA_FILE_LIST:
        inputfile = datatype + ".csv.gz"
        inputdata = os.path.join(BASE_SYNTHEA_INPUT_DIRECTORY,inputfile)
        output = os.path.join(BASE_OUTPUT_DIRECTORY,inputfile)
        header = True
        mode = 'w'
        for df in pd.read_csv(inputdata, dtype=model_synthea.model_schema[datatype], chunksize=INPUT_CHUNK_SIZE):
            if (datatype == 'patients'):
                 person = pd.DataFrame(columns=model_omop.model_schema['person'].keys())
                 person['person_id'] = df['Id'].apply(patienthash)
                 person['gender_concept_id'] = df['GENDER'].apply(getGenderConceptCode)
                 person['year_of_birth'] = df['BIRTHDATE'].apply(getYearFromSyntheaDate)
                 person['month_of_birth'] = df['BIRTHDATE'].apply(getMonthFromSyntheaDate)
                 person['day_of_birth'] = df['BIRTHDATE'].apply(getDayFromSyntheaDate)
                 person['race_concept_id'] =  df['RACE'].apply(getRaceConceptCode)
                 person['ethnicity_concept_id'] = df['ETHNICITY'].apply(getEthnicityConceptCode)
                 person['person_source_value'] = df['Id']
                 person['race_source_value'] = df['RACE']
                 person['ethnicity_source_value'] = df['ETHNICITY']
                 # write output.  write header only if this is the first chunk
                 output = os.path.join(BASE_OUTPUT_DIRECTORY,'person.csv')
                 person.to_csv(output, mode=mode, header=header, index=False)

                 # create location record 
                 location = pd.DataFrame(columns=model_omop.model_schema['location'].keys())
                 location['location_id'] = df['Id'].apply(patienthash)
                 location['address_1'] = df['ADDRESS']
                 location['city'] = df['CITY']
                 location['state'] = df['STATE']
                 location['zip'] = df['ZIP']
                 location['county'] = df['COUNTY']
                 location['location_source_value'] = df['Id']
                 # write output.  write header only if this is the first chunk
                 output = os.path.join(BASE_OUTPUT_DIRECTORY,'location.csv')
                 location.to_csv(output, mode=mode, header=header, index=False)

                 # create death record
                 death = pd.DataFrame(columns=model_omop.model_schema['death'].keys())
                 death['person_id'] = df['Id'].apply(patienthash)
                 death['deathdate'] = df['DEATHDATE']
                 death =  death[death.deathdate.notnull()]
                 # write output.  write header only if this is the first chunk
                 output = os.path.join(BASE_OUTPUT_DIRECTORY,'death.csv')
                 death.to_csv(output, mode=mode, header=header, index=False)
                 # no longer write header and append to file
                 header=False
                 mode='a'



















