import os
import pandas as pd

class Utils:
    def __init__(self):
        pass

    #
    # check memory usage of a pandas dataframe
    #
    def mem_usage(self, pandas_obj):
        if isinstance(pandas_obj,pd.DataFrame):
            usage_b = pandas_obj.memory_usage(deep=True).sum()
        else: # we assume if not a df it's a series
            usage_b = pandas_obj.memory_usage(deep=True)
        usage_mb = usage_b / 1024 ** 2 # convert bytes to megabytes
        return "{:03.2f} MB".format(usage_mb)

    # load the vocabulary into memory
    def loadVocabulary(self, BASE_OMOP_INPUT_DIRECTORY):
        vocab = {}
        vocab['concept'] = pd.read_csv(os.path.join(BASE_OMOP_INPUT_DIRECTORY,'CONCEPT.csv.gz'), sep='\t', dtype=model_omop.model_schema['concept'], error_bad_lines=False)
        print('concept: ' + util.mem_usage(vocab['concept']))
        vocab['concept_relationship'] = pd.read_csv(os.path.join(BASE_OMOP_INPUT_DIRECTORY,'CONCEPT_RELATIONSHIP.csv.gz'), sep='\t', dtype=model_omop.model_schema['concept_relationship'])
        print('concept_relationship: ' + util.mem_usage(vocab['concept_relationship']))
        vocab['concept_synonym'] = pd.read_csv(os.path.join(BASE_OMOP_INPUT_DIRECTORY,'CONCEPT_SYNONYM.csv.gz'), sep='\t', dtype=model_omop.model_schema['concept_synonym'])
        print('concept_synonym: ' + util.mem_usage(vocab['concept_synonym']))
        vocab['concept_ancestor'] = pd.read_csv(os.path.join(BASE_OMOP_INPUT_DIRECTORY,'CONCEPT_ANCESTOR.csv.gz'), sep='\t', dtype=model_omop.model_schema['concept_ancestor'])
        print('concept_ancestor: ' + util.mem_usage(vocab['concept_synonym']))
        vocab['concept_class'] = pd.read_csv(os.path.join(BASE_OMOP_INPUT_DIRECTORY,'CONCEPT_CLASS.csv.gz'), sep='\t', dtype=model_omop.model_schema['concept_class'])
        print('concept_class: ' + util.mem_usage(vocab['concept_class']))
        #vocab['drug_strength'] = pd.read_csv(os.path.join(BASE_OMOP_INPUT_DIRECTORY,'DRUG_STRENGTH.csv.gz'), sep='\t', dtype=model_omop.model_schema['drug_strength'])
        #print(util.mem_usage(vocab['drug_strength']))
