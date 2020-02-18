import pandas as pd:
import dotenv
import ModelSyntheaPandas
import ModelOmopPandas

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

#---------------------------------
# start of the program
#---------------------------------
if __name__ == '__main__':
    if not os.path.exists(BASE_OUTPUT_DIRECTORY): os.makedirs(BASE_OUTPUT_DIRECTORY)
    if not os.path.exists(BASE_ETL_CONTROL_DIRECTORY): os.makedirs(BASE_ETL_CONTROL_DIRECTORY)

    current_stats_filename = os.path.join(BASE_OUTPUT_DIRECTORY,'etl_stats.txt')
    print('SYNTHEA_ETL starting')
    print('BASE_SYNTHEA_INPUT_DIRECTORY     =' + BASE_SYNTHEA_INPUT_DIRECTORY)
    print('BASE_OUTPUT_DIRECTORY           =' + BASE_OUTPUT_DIRECTORY)
    print('BASE_ETL_CONTROL_DIRECTORY      =' + BASE_ETL_CONTROL_DIRECTORY)

    # check files look ok

    # Build the object to manage access to all the files
    write_header_records()

    # load the vocab flies
    #build_maps()
   
    # we only need to consider one synthea input file at a time to make the mapping
    synthea_file_list =  ['conditions.csv','careplans.csv','observations.csv','procedures.csv','immunizations.csv','imaging_studies.csv','imaging_studies.csv','encounters.csv','organizations.csv','providers.csv','providers.csv','payer_transitions.csv','allergies.csv','patients.csv','medications.csv']
    synthea_file_list = ['patients.csv']
    for inputfile in synthea_file_list:
        inputdata = os.path.join(BASE_SYNTHEA_INPUT_DIRECTORY,inputfile)
        df = pd.read_csv(inputdata, dtype='object')

