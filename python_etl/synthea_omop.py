import pandas as pd
import os
import dotenv
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
    convert = SyntheaToOmop.SyntheaToOmop(model_omop.model_schema)

    # load the vocabulary into memory
    #vocab = util.loadVocabulary(BASE_OMOP_INPUT_DIRECTORY)

    # we only need to consider one synthea input file at a time to make the mapping
    # so only put one in memory at a time.  
    # we can read csv in chunks
    for datatype in SYNTHEA_FILE_LIST:
        inputfile = datatype + ".csv.gz"
        inputdata = os.path.join(BASE_SYNTHEA_INPUT_DIRECTORY,inputfile)
        output = os.path.join(BASE_OUTPUT_DIRECTORY,inputfile)
        header = True
        mode = 'w'
        print(datatype),
        for df in pd.read_csv(inputdata, dtype=model_synthea.model_schema[datatype], chunksize=INPUT_CHUNK_SIZE):
            if (datatype == 'patients'):
                (person, location, death) = convert.syntheaPatientsToOmop(df)
                person.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'person.csv'), mode=mode, header=header, index=False)
                location.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'location.csv'), mode=mode, header=header, index=False)
                death.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'death.csv'), mode=mode, header=header, index=False)
                # no longer write header and append to file. write . so we know program is still running
                print('.'),
                sys.stdout.flush()
                header=False
                mode='a'
            elif (datatype == 'conditions'):
                pass
            elif (datatype == 'careplans'):
                pass
            elif (datatype == 'observations'):
                pass
            elif (datatype == 'procedures'):
                pass
            elif (datatype == 'immunizations'):
                pass
            elif (datatype == 'imaging_studies'):
                pass
            elif (datatype == 'encounters'):
                pass
            elif (datatype == 'organizations'):
                pass
            elif (datatype == 'providers'):
                pass
            elif (datatype == 'payer_transitions'):
                pass
            elif (datatype == 'allergies'):
                pass
            elif (datatype == 'medications'):
                pass
 













