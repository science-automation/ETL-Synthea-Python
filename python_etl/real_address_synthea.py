import pandas as pd
import os
from dotenv import load_dotenv
import ModelSyntheaPandas
import Utils
import sys

#---------------------------------------------------------------------------------
# This script takes synthea data with fake address and changes them to real addresses
#---------------------------------------------------------------------------------

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
# Path to the file that contains the real country addresses 
ADDRESS_FILE       = os.environ['ADDRESS_FILE']
# Synthea input file chunk size.
INPUT_CHUNK_SIZE = int(os.environ['INPUT_CHUNK_SIZE'])
# List of synthea input files

#---------------------------------
# start of the program
#---------------------------------
if __name__ == '__main__':
    if not os.path.exists(BASE_SYNTHEA_INPUT_DIRECTORY): 
        print("Synthea input directory should exist")
        exit(1)

    print('BASE_SYNTHEA_INPUT_DIRECTORY     =' + BASE_SYNTHEA_INPUT_DIRECTORY)
    print('ADDRESS_FILE                     =' + ADDRESS_FILE)

    # load utils
    util = Utils.Utils()

    # load the synthea model
    model_synthea = ModelSyntheaPandas.ModelSyntheaPandas()

    # read the synthea patient file
    datatype='patients'
    if (os.path.exists(os.path.join(BASE_SYNTHEA_INPUT_DIRECTORY,datatype + '.csv'))):
        inputfile = datatype + '.csv'
        compression=None
    elif (os.path.exists(os.path.join(BASE_SYNTHEA_INPUT_DIRECTORY,datatype + '.csv.gz'))):
        inputfile = datatype + '.csv.gz'
        compression='gzip'
    else:
        print("Error:  Could not find synthea file for " + datatype)
        exit(1)
    inputdata = BASE_SYNTHEA_INPUT_DIRECTORY + "/" + inputfile
    patient = pd.read_csv(inputdata, dtype=model_synthea.model_schema[datatype], compression=compression)
    print(patient)

    # load the location data
    compression=None
    addresses = pd.read_csv(ADDRESS_FILE, compression=compression)
    # remove all records with null street number
    addresses = addresses[addresses['STREET'].notnull()]
    addresses = addresses[addresses['NUMBER'].notnull()]
    #print(addresses)

    # call the function to switch out the address
    patient['temp'] = patient['ZIP'].apply(util.getRealAddress,args=(addresses,))
    print(patient)
