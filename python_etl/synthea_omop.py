import pandas as pd
import os
import dotenv
import ModelSyntheaPandas
import ModelSyntheaPandasObject
import ModelOmopPandas
import Utils

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
SYNTHEA_DIR_FORMAT               = os.environ['SYNTHEA_DIR_FORMAT']
SYNTHEA_FILE_LIST =  ['conditions','careplans','observations','procedures','immunizations','imaging_studies','imaging_studies','encounters','organizations','providers','providers','payer_transitions','allergies','patients','medications']

#---------------------------------
# start of the program
#---------------------------------
if __name__ == '__main__':
    if not os.path.exists(BASE_OUTPUT_DIRECTORY): os.makedirs(BASE_OUTPUT_DIRECTORY)
    if not os.path.exists(BASE_ETL_CONTROL_DIRECTORY): os.makedirs(BASE_ETL_CONTROL_DIRECTORY)

    print('SYNTHEA_ETL starting')
    print('BASE_SYNTHEA_INPUT_DIRECTORY     =' + BASE_SYNTHEA_INPUT_DIRECTORY)
    print('BASE_OUTPUT_DIRECTORY           =' + BASE_OUTPUT_DIRECTORY)
    print('BASE_ETL_CONTROL_DIRECTORY      =' + BASE_ETL_CONTROL_DIRECTORY)

    # load utils
    util = Utils.Utils()

    # check files look ok

    # load the synthea model
    model_synthea = ModelSyntheaPandas.ModelSyntheaPandas()
    #model_synthea = ModelSyntheaPandasObject.ModelSyntheaPandas()

    # we only need to consider one synthea input file at a time to make the mapping
    for datatype in SYNTHEA_FILE_LIST:
        inputfile = datatype + ".csv.gz"
        inputdata = os.path.join(BASE_SYNTHEA_INPUT_DIRECTORY,inputfile)
        #print(model_synthea.model_schema[datatype])
        df = pd.read_csv(inputdata, dtype=model_synthea.model_schema[datatype])
        print(datatype + ": " + util.mem_usage(df))
        #print(df.dtypes)
        #if (datatype = "observations") :
            
