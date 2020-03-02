import pandas as pd
import os
import dotenv
import ModelSyntheaPandas
import ModelOmopPandas
import SyntheaToOmop
import Utils
import sys
import shutil

#------------------------------------------------------
# This scripts performs an ETL from Synthea to omop CDM.
# It uses pandas dataframes
#------------------------------------------------------

# ------------------------
# load env
# ------------------------
dotenv.load_dotenv(".env")
dotenv.load_dotenv(".envbase")  # base numbers for id that is updated after run

# -----------------------------------
# - Configuration
# -----------------------------------

# Edit your .env file to change which directories to use in the ETL process
# Path to the directory containing the downloaded SynPUF files
BASE_SYNTHEA_INPUT_DIRECTORY     = os.environ['BASE_SYNTHEA_INPUT_DIRECTORY']
# Path to the directory containing the OMOP Vocabulary v5 files (can be downloaded from http://www.ohdsi.org/web/athena/)
BASE_OMOP_INPUT_DIRECTORY       = os.environ['BASE_OMOP_INPUT_DIRECTORY']
# Path to the directory where CDM-compatible CSV files should be saved
BASE_OUTPUT_DIRECTORY           = os.environ['BASE_OUTPUT_DIRECTORY']
# Synthea input file chunk size.
INPUT_CHUNK_SIZE = int(os.environ['INPUT_CHUNK_SIZE'])
# List of synthea input files
#SYNTHEA_FILE_LIST =  ['patients']
# patients and encounters are first so that we can create dataframes to lookup ids
SYNTHEA_FILE_LIST =  ['patients','encounters','conditions','careplans','observations','procedures','immunizations','imaging_studies','organizations','providers','payer_transitions','allergies','medications']
# List of omop output files
OMOP_FILE_LIST = ['person','location','death','condition_occurrence','drug_exposure','observation','measurement','procedure_occurrence','observation_period','visit_occurrence','care_site','provider']

#---------------------------------
# start of the program
#---------------------------------
if __name__ == '__main__':
    if not os.path.exists(BASE_OUTPUT_DIRECTORY): 
        os.makedirs(BASE_OUTPUT_DIRECTORY)
    else:
        shutil.rmtree(BASE_OUTPUT_DIRECTORY)
        os.makedirs(BASE_OUTPUT_DIRECTORY)

    print('BASE_SYNTHEA_INPUT_DIRECTORY     =' + BASE_SYNTHEA_INPUT_DIRECTORY)
    print('BASE_OUTPUT_DIRECTORY           =' + BASE_OUTPUT_DIRECTORY)

    # load utils
    util = Utils.Utils()

    # check files look ok

    # load the synthea model
    model_synthea = ModelSyntheaPandas.ModelSyntheaPandas()
    model_omop = ModelOmopPandas.ModelOmopPandas()
    convert = SyntheaToOmop.SyntheaToOmop(model_omop.model_schema)

    # write the headers for the output files
    for initfile in OMOP_FILE_LIST:
        df = pd.DataFrame(columns=model_omop.model_schema[initfile].keys())
        initfile = initfile + ".csv"
        df.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,initfile), mode='w', header=True, index=False)

    # load the vocabulary into memory
    vocab_concept = util.loadConceptVocabulary(BASE_OMOP_INPUT_DIRECTORY, model_omop)

    # create source to standard mapping
    srctostdvm = util.sourceToStandardVocabMap(vocab_concept,model_omop) 

    # base numbers to start id's
    person_id = int(os.environ['PERSON_ID_BASE'])
    location_id = int(os.environ['LOCATION_ID_BASE'])
    observation_period_id = int(os.environ['OBSERVATION_PERIOD_ID_BASE'])
    specimen_id = int(os.environ['SPECIMEN_ID_BASE'])
    visit_occurrence_id = int(os.environ['VISIT_OCCURRENCE_ID_BASE'])
    visit_detail_id = int(os.environ['VISIT_DETAIL_ID_BASE'])
    procedure_occurrence_id = int(os.environ['PROCEDURE_OCCURRENCE_ID_BASE'])
    drug_exposure_id = int(os.environ['DRUG_EXPOSURE_ID_BASE'])
    condition_era_id = int(os.environ['CONDITION_ERA_ID_BASE'])
    condition_occurrence_id = int(os.environ['CONDITION_OCCURRENCE_ID_BASE'])
    device_exposure_id = int(os.environ['DEVICE_EXPOSURE_BASE'])
    observation_id = int(os.environ['OBSERVATION_ID_BASE'])
    measurement_id = int(os.environ['MEASUREMENT_ID_BASE'])
    note_id = int(os.environ['NOTE_ID_BASE'])
    note_id_nlp = int(os.environ['NOTE_ID_NLP_BASE'])
    care_site_id = int(os.environ['CARE_SITE_ID_BASE'])
    provider_id = int(os.environ['PROVIDER_ID_BASE'])

    # create mapping for to lookup id's
    personmap = pd.DataFrame(columns=["person_id","synthea_patient_id"])
    visitmap = pd.DataFrame(columns=["visit_occurrence_id","synthea_encounter_id"])
    
    # we dont need a header when appending
    header = False
    mode='a'

    # start looping through the synthea files
    # we only need to consider one synthea input file at a time to make the mapping
    # so only put one in memory at a time and read in chunks to avoid memory issues
    for datatype in SYNTHEA_FILE_LIST:
        if (os.path.exists(os.path.join(BASE_SYNTHEA_INPUT_DIRECTORY,datatype + '.csv'))):
            inputfile = datatype + '.csv'
            compression=None
        elif (os.path.exists(os.path.join(BASE_SYNTHEA_INPUT_DIRECTORY,datatype + '.csv.gz'))):
            inputfile = datatype + '.csv.gz'
            compression='gzip'
        else:
            print("Error:  Could not find " + inputfile + " synthea file")
            exit(1)
        inputdata = os.path.join(BASE_SYNTHEA_INPUT_DIRECTORY,inputfile)
        output = os.path.join(BASE_OUTPUT_DIRECTORY,inputfile)
        #print("")
        #print(datatype),
        for df in pd.read_csv(inputdata, dtype=model_synthea.model_schema[datatype], chunksize=INPUT_CHUNK_SIZE, iterator=True, compression=compression):
            if (datatype == 'patients'):
                (person, location, death, personmap) = convert.patientsToOmop(df, personmap, person_id, location_id)
                person.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'person.csv'), mode=mode, header=header, index=False)
                location.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'location.csv'), mode=mode, header=header, index=False)
                death.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'death.csv'), mode=mode, header=header, index=False)
                personmap.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'personmap.csv'), mode='w', header=True, index=False)
            elif (datatype == 'conditions'):
                (condition_occurrence, drug_exposure, observation, condition_occurrence_id, drug_exposure_id, observation_id) = convert.conditionsToOmop(df, srctostdvm, condition_occurrence_id, drug_exposure_id, observation_id, personmap, visitmap)
                condition_occurrence.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'condition_occurrence.csv'), mode=mode, header=header, index=False)
                drug_exposure.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'drug_exposure.csv'), mode=mode, header=header, index=False)
                observation.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'observation.csv'), mode=mode, header=header, index=False)
            elif (datatype == 'careplans'):
                pass
            elif (datatype == 'observations'):
                measurement = convert.observationsToOmop(df, measurement_id, personmap, visitmap)
                measurement.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'measurement.csv'), mode=mode, header=header, index=False)
            elif (datatype == 'procedures'):
                procedure_occurrence = convert.proceduresToOmop(df, procedure_occurrence_id, personmap, visitmap)
                #measurement.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'measurement.csv'), mode=mode, header=header, index=False)
                procedure_occurrence.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'procedure_occurrence.csv'), mode=mode, header=header, index=False)
            elif (datatype == 'immunizations'):
                drug_exposure = convert.immunizationsToOmop(df, drug_exposure_id, personmap, visitmap)
                drug_exposure.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'drug_exposure.csv'), mode=mode, header=header, index=False)
            elif (datatype == 'imaging_studies'):
                pass
            elif (datatype == 'encounters'):
                (observation_period, visit_occurrence, observation_period_id, visitmap) = convert.encountersToOmop(df, observation_period_id, visit_occurrence_id, personmap, visitmap)
                observation_period.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'observation_period.csv'), mode=mode, header=header, index=False)
                visit_occurrence.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'visit_occurrence.csv'), mode=mode, header=header, index=False)
                visitmap.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'visitmap.csv'), mode='w', header=True, index=False)
            elif (datatype == 'organizations'):
                care_site = convert.organizationsToOmop(df, care_site_id)
                care_site.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'care_site.csv'), mode=mode, header=header, index=False)
            elif (datatype == 'providers'):
                provider = convert.providersToOmop(df, provider_id)
                provider.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'provider.csv'), mode=mode, header=header, index=False)
            elif (datatype == 'payer_transitions'):
                pass
            elif (datatype == 'allergies'):
                observation = convert.allergiesToOmop(df, observation_id, personmap)
                person.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'observation.csv'), mode=mode, header=header, index=False)
            elif (datatype == 'medications'):
                drug_exposure = convert.medicationsToOmop(df, drug_exposure_id, personmap)
                drug_exposure.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'drug_exposure.csv'), mode=mode, header=header, index=False)
            else:
                print("Unknown input type: " + datatype)
            # no longer write header and append to file. write . so we know program is still running
            print('.'),
            sys.stdout.flush()



