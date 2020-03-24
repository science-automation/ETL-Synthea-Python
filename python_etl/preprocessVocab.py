import pandas as pd
import os
import dotenv
import ModelSyntheaPandas
import ModelOmopPandas
import ExtractVocab
import Utils
import sys

#------------------------------------------------------
# This script extracts only the vocabulary needed for the ETL
# and discards the rest to lower memory requirements at runtime
#------------------------------------------------------

# ------------------------
# load env
# ------------------------
dotenv.load_dotenv(".env")

# -----------------------------------
# - Configuration
# -----------------------------------

# Edit your .env file to change which directories to use in the ETL process
# Path to the directory containing the downloaded SynPUF files
BASE_SYNTHEA_INPUT_DIRECTORY    = os.environ['BASE_SYNTHEA_INPUT_DIRECTORY']
# Path to the directory containing the OMOP Vocabulary v5 files (can be downloaded from http://www.ohdsi.org/web/athena/)
BASE_OMOP_INPUT_DIRECTORY       = os.environ['BASE_OMOP_INPUT_DIRECTORY']
# directory to copy the processed vocabulary files
BASE_VOCAB_OUTPUT_DIRECTORY     = os.environ['BASE_VOCAB_OUTPUT_DIRECTORY']
# patients and encounters are first so that we can create dataframes to lookup ids
SYNTHEA_FILE_LIST =  ['patients','encounters','conditions','careplans','observations','procedures','immunizations','imaging_studies','allergies','medications']
# Synthea input file chunk size.  We will only process one large chunk for each type
INPUT_CHUNK_SIZE_EXTRACT = int(os.environ['INPUT_CHUNK_SIZE_EXTRACT'])

#---------------------------------
# start of the program
#---------------------------------
if __name__ == '__main__':
    if not os.path.exists(BASE_VOCAB_OUTPUT_DIRECTORY): 
        os.makedirs(BASE_VOCAB_OUTPUT_DIRECTORY)
    else:
        # cleanup old files in output directory but dont delete directory
        filesToRemove = [f for f in os.listdir(BASE_VOCAB_OUTPUT_DIRECTORY)]
        for f in filesToRemove:
            os.remove(os.path.join(BASE_VOCAB_OUTPUT_DIRECTORY, f))

    print('BASE_SYNTHEA_INPUT_DIRECTORY     =' + BASE_SYNTHEA_INPUT_DIRECTORY)
    print('BASE_VOCAB_OUTPUT_DIRECTORY      =' + BASE_VOCAB_OUTPUT_DIRECTORY)

    # load utils
    util = Utils.Utils()

    # load the model files to define structure
    model_synthea = ModelSyntheaPandas.ModelSyntheaPandas()
    model_omop = ModelOmopPandas.ModelOmopPandas()
    extract = ExtractVocab.ExtractVocab(model_omop.model_schema)

    # load the full concept vocabulary into memory
    vocab_concept = util.loadConceptVocabulary(BASE_OMOP_INPUT_DIRECTORY, model_omop)

    # init the concept extraction dataframe
    conceptextract = pd.DataFrame(columns=model_omop.model_schema['concept'])

    # start looping through the synthea files
    # we only need to consider one synthea input file at a time to make the extraction
    # so only put one in memory at a time and read in chunks to avoid memory issues
    for datatype in SYNTHEA_FILE_LIST:
        if (os.path.exists(os.path.join(BASE_SYNTHEA_INPUT_DIRECTORY,datatype + '.csv'))):
            inputfile = datatype + '.csv'
            compression=None
        elif (os.path.exists(os.path.join(BASE_SYNTHEA_INPUT_DIRECTORY,datatype + '.csv.gz'))):
            inputfile = datatype + '.csv.gz'
            compression='gzip'
        else:
            print("Error:  Could not find " + datatype + " synthea file")
            exit(1)
        inputdata = os.path.join(BASE_SYNTHEA_INPUT_DIRECTORY,inputfile)
        output = os.path.join(BASE_VOCAB_OUTPUT_DIRECTORY,inputfile)
 
        print("")
        print(datatype),
        for df in pd.read_csv(inputdata, dtype=model_synthea.model_schema[datatype], chunksize=INPUT_CHUNK_SIZE_EXTRACT, iterator=True, compression=compression):
            if (datatype == 'patients'):
                pass
            elif (datatype == 'conditions'):
                conceptextract = conceptextract.append(extract.conditionsExtract(df, vocab_concept['concept']))
            elif (datatype == 'careplans'):
                pass
            elif (datatype == 'observations'):
                conceptextract = conceptextract.append(extract.observationsExtract(df, vocab_concept['concept']))
            elif (datatype == 'procedures'):
                conceptextract = conceptextract.append(extract.proceduresExtract(df, vocab_concept['concept']))
            elif (datatype == 'immunizations'):
               pass 
            elif (datatype == 'imaging_studies'):
                pass
            elif (datatype == 'encounters'):
                conceptextract = conceptextract.append(extract.encountersExtract(df, vocab_concept['concept']))
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
            else:
                print("Unknown input type: " + datatype)
            sys.stdout.flush()
    # Now use the concepts to find every related concept relationship
    conceptrelextract = extract.getConceptRelationshipExtract(vocab_concept['concept_relationship'], conceptextract)

    # sort rows.  This will help when putting the files to git to see what has changed
    conceptextract = conceptextract.sort_values('concept_id')
    conceptrelextract = conceptrelextract.sort_values(['concept_id_1','concept_id_2']) 

    # write to files
    conceptextract.to_csv(os.path.join(BASE_VOCAB_OUTPUT_DIRECTORY,'CONCEPT.csv'), sep='\t', mode='w', header=True, index=False)
    conceptrelextract.to_csv(os.path.join(BASE_VOCAB_OUTPUT_DIRECTORY,'CONCEPT_RELATIONSHIP.csv'), sep='\t', mode='w', header=True, index=False)
