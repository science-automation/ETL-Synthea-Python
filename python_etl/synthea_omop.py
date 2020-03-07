import pandas as pd
import os
from dotenv import load_dotenv
import ModelSyntheaPandas
import ModelOmopPandas
import SyntheaToOmop
import Utils
import sys

#------------------------------------------------------
# This scripts performs an ETL from Synthea to omop CDM.
# It uses pandas dataframes
#------------------------------------------------------

# ------------------------
# load env
# ------------------------
load_dotenv(verbose=True)

# -----------------------------------
# - Configuration
# -----------------------------------

# Edit your .env file to change which directories to use in the ETL process
# Path to the directory containing the downloaded SynPUF files
BASE_SYNTHEA_INPUT_DIRECTORY    = os.environ['BASE_SYNTHEA_INPUT_DIRECTORY']
# Path to the directory containing the OMOP Vocabulary v5 files (can be downloaded from http://www.ohdsi.org/web/athena/)
BASE_OMOP_INPUT_DIRECTORY       = os.environ['BASE_OMOP_INPUT_DIRECTORY']
# Path to the directory where CDM-compatible CSV files should be saved
BASE_OUTPUT_DIRECTORY           = os.environ['BASE_OUTPUT_DIRECTORY']
# counter file name
COUNTER_FILE                    = os.environ['COUNTER_FILE']
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
        # cleanup old files in output directory but dont delete directory
        filesToRemove = [f for f in os.listdir(BASE_OUTPUT_DIRECTORY)]
        for f in filesToRemove:
            os.remove(os.path.join(BASE_OUTPUT_DIRECTORY, f))

    print('BASE_SYNTHEA_INPUT_DIRECTORY     =' + BASE_SYNTHEA_INPUT_DIRECTORY)
    print('BASE_OUTPUT_DIRECTORY            =' + BASE_OUTPUT_DIRECTORY)
    print('COUNTER_FILE                     =' + COUNTER_FILE)

    # load utils
    util = Utils.Utils()

    # init the counter file if it does not exist and then read values into a dict
    if not os.path.exists(COUNTER_FILE):
        util.initCounterFile(OMOP_FILE_LIST, 1, COUNTER_FILE)
    c = util.getCounter(COUNTER_FILE)

    # check files look ok

    # load the model files to define structure
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

    # create source to standard and source to source mapping
    srctostdvm = util.sourceToStandardVocabMap(vocab_concept,model_omop) 
    srctosrcvm = util.sourceToSourceVocabMap(vocab_concept,model_omop)

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
            print("Error:  Could not find synthea file for " + datatype)
            exit(1)
        inputdata = os.path.join(BASE_SYNTHEA_INPUT_DIRECTORY,inputfile)
        output = os.path.join(BASE_OUTPUT_DIRECTORY,inputfile)
        print("")
        print(datatype),
        for df in pd.read_csv(inputdata, dtype=model_synthea.model_schema[datatype], chunksize=INPUT_CHUNK_SIZE, iterator=True, compression=compression):
            if (datatype == 'patients'):
                (person, location, death, personmap, c['person_id'], c['location_id']) = convert.patientsToOmop(df, personmap, c['person_id'], c['location_id'])
                person.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'person.csv'), mode=mode, header=header, index=False)
                location.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'location.csv'), mode=mode, header=header, index=False)
                death.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'death.csv'), mode=mode, header=header, index=False)
                personmap.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'personmap.csv'), mode='w', header=True, index=False)
            elif (datatype == 'conditions'):
                (condition_occurrence, drug_exposure, observation, c['condition_occurrence_id'], c['drug_exposure_id'], c['observation_id']) = convert.conditionsToOmop(df, srctostdvm, c['condition_occurrence_id'], c['drug_exposure_id'], c['observation_id'], personmap, visitmap)
                condition_occurrence.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'condition_occurrence.csv'), mode=mode, header=header, index=False)
                drug_exposure.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'drug_exposure.csv'), mode=mode, header=header, index=False)
                observation.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'observation.csv'), mode=mode, header=header, index=False)
            elif (datatype == 'careplans'):
                pass
            elif (datatype == 'observations'):
                (measurement, c['measurement_id']) = convert.observationsToOmop(df, srctostdvm, srctosrcvm, c['measurement_id'], personmap, visitmap)
                measurement.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'measurement.csv'), mode=mode, header=header, index=False)
            elif (datatype == 'procedures'):
                (procedure_occurrence, c['procedure_occurrence_id']) = convert.proceduresToOmop(df, srctosrcvm, c['procedure_occurrence_id'], personmap, visitmap)
                #measurement.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'measurement.csv'), mode=mode, header=header, index=False)
                procedure_occurrence.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'procedure_occurrence.csv'), mode=mode, header=header, index=False)
            elif (datatype == 'immunizations'):
                (drug_exposure, c['drug_exposure_id']) = convert.immunizationsToOmop(df, srctosrcvm, c['drug_exposure_id'], personmap, visitmap)
                drug_exposure.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'drug_exposure.csv'), mode=mode, header=header, index=False)
            elif (datatype == 'imaging_studies'):
                pass
            elif (datatype == 'encounters'):
                (observation_period, visit_occurrence, c['observation_period_id'], c['visit_occurrence_id'], visitmap) = convert.encountersToOmop(df, c['observation_period_id'], c['visit_occurrence_id'], personmap, visitmap)
                observation_period.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'observation_period.csv'), mode=mode, header=header, index=False)
                visit_occurrence.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'visit_occurrence.csv'), mode=mode, header=header, index=False)
                visitmap.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'visitmap.csv'), mode='w', header=True, index=False)
            elif (datatype == 'organizations'):
                (care_site, c['care_site_id']) = convert.organizationsToOmop(df, c['care_site_id'])
                care_site.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'care_site.csv'), mode=mode, header=header, index=False)
            elif (datatype == 'providers'):
                (provider, c['provider_id']) = convert.providersToOmop(df, c['provider_id'])
                provider.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'provider.csv'), mode=mode, header=header, index=False)
            elif (datatype == 'payer_transitions'):
                pass
            elif (datatype == 'allergies'):
                (observation, c['observation_id']) = convert.allergiesToOmop(df, srctostdvm, c['observation_id'], personmap, visitmap)
                observation.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'observation.csv'), mode=mode, header=header, index=False)
            elif (datatype == 'medications'):
                (drug_exposure, c['drug_exposure_id']) = convert.medicationsToOmop(df, srctosrcvm, c['drug_exposure_id'], personmap)
                drug_exposure.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'drug_exposure.csv'), mode=mode, header=header, index=False)
            else:
                print("Unknown input type: " + datatype)
            # no longer write header and append to file. write . so we know program is still running
            print('.'),
            sys.stdout.flush()
        # write the counter file
        util.writeCounter(c,COUNTER_FILE)



