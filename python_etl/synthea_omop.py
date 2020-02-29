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
# Base number to start the condition_occurrence ID index
CONDITION_ID_BASE = os.environ['CONDITION_ID_BASE']
# Base number to start the observation_id index
OBSERVATION_ID_BASE = os.environ['OBSERVATION_ID_BASE']
# Synthea input file chunk size.
INPUT_CHUNK_SIZE = int(os.environ['INPUT_CHUNK_SIZE'])
# List of synthea input files
#SYNTHEA_FILE_LIST =  ['patients']
SYNTHEA_FILE_LIST =  ['patients','conditions','careplans','observations','procedures','immunizations','imaging_studies','encounters','organizations','providers','payer_transitions','allergies','medications']
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

    # we only need to consider one synthea input file at a time to make the mapping
    # so only put one in memory at a time and read in chunks to avoid memory issues  
    condition_id = int(CONDITION_ID_BASE)
    observation_id = int(OBSERVATION_ID_BASE)
    header = False
    mode='a'
    for datatype in SYNTHEA_FILE_LIST:
        inputfile = datatype + ".csv.gz"
        inputdata = os.path.join(BASE_SYNTHEA_INPUT_DIRECTORY,inputfile)
        output = os.path.join(BASE_OUTPUT_DIRECTORY,inputfile)
        print("")
        print(datatype),
        for df in pd.read_csv(inputdata, dtype=model_synthea.model_schema[datatype], chunksize=INPUT_CHUNK_SIZE):
            if (datatype == 'patients'):
                (person, location, death) = convert.patientsToOmop(df)
                person.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'person.csv'), mode=mode, header=header, index=False)
                location.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'location.csv'), mode=mode, header=header, index=False)
                death.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'death.csv'), mode=mode, header=header, index=False)
            elif (datatype == 'conditions'):
                (condition_occurrence, drug_exposure, observation, condition_id, observation_id) = convert.conditionsToOmop(df, srctostdvm, condition_id, observation_id)
                condition_occurrence.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'condition_occurrence.csv'), mode=mode, header=header, index=False)
                drug_exposure.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'drug_exposure.csv'), mode=mode, header=header, index=False)
                observation.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'observation.csv'), mode=mode, header=header, index=False)
            elif (datatype == 'careplans'):
                pass
            elif (datatype == 'observations'):
                measurement = convert.observationsToOmop(df)
                measurement.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'measurement.csv'), mode=mode, header=header, index=False)
            elif (datatype == 'procedures'):
                procedure_occurrence = convert.proceduresToOmop(df)
                measurement.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'measurement.csv'), mode=mode, header=header, index=False)
                procedure_occurrence.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'procedure_occurrence.csv'), mode=mode, header=header, index=False)
            elif (datatype == 'immunizations'):
                drug_exposure = convert.immunizationsToOmop(df)
                drug_exposure.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'drug_exposure.csv'), mode=mode, header=header, index=False)
            elif (datatype == 'imaging_studies'):
                pass
            elif (datatype == 'encounters'):
                (observation_period, visit_occurrence) = convert.encountersToOmop(df)
                observation_period.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'observation_period.csv'), mode=mode, header=header, index=False)
                visit_occurrence.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'visit_occurrence.csv'), mode=mode, header=header, index=False)
            elif (datatype == 'organizations'):
                care_site = convert.organizationsToOmop(df)
                care_site.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'care_site.csv'), mode=mode, header=header, index=False)
            elif (datatype == 'providers'):
                provider = convert.providersToOmop(df)
                provider.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'provider.csv'), mode=mode, header=header, index=False)
            elif (datatype == 'payer_transitions'):
                pass
            elif (datatype == 'allergies'):
                observation = convert.allergiesToOmop(df)
                person.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'observation.csv'), mode=mode, header=header, index=False)
            elif (datatype == 'medications'):
                drug_exposure = convert.medicationsToOmop(df)
                drug_exposure.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'drug_exposure.csv'), mode=mode, header=header, index=False)
            else:
                print("Unknown input type: " + datatype)
            # no longer write header and append to file. write . so we know program is still running
            print('.'),
            sys.stdout.flush()



