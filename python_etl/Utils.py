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
 
    #
    # load the concept vocabulary into dataframes
    #
    def loadConceptVocabulary(self, BASE_OMOP_INPUT_DIRECTORY, model_omop):
        vocab = {}
        vocab['concept'] = pd.read_csv(os.path.join(BASE_OMOP_INPUT_DIRECTORY,'CONCEPT.csv.gz'), \
                sep='\t', dtype=model_omop.model_schema['concept'], error_bad_lines=False)
        vocab['concept_relationship'] = pd.read_csv(os.path.join(BASE_OMOP_INPUT_DIRECTORY,'CONCEPT_RELATIONSHIP.csv.gz'), \
                sep='\t', dtype=model_omop.model_schema['concept_relationship'])
        return vocab

    #
    # load the vocabulary into dataframes
    #
    def loadVocabulary(self, BASE_OMOP_INPUT_DIRECTORY, model_omop):
        vocab = {}
        vocab['concept'] = pd.read_csv(os.path.join(BASE_OMOP_INPUT_DIRECTORY,'CONCEPT.csv.gz'), \
             sep='\t', dtype=model_omop.model_schema['concept'], error_bad_lines=False)
        print('concept: ' + self.mem_usage(vocab['concept']))
        vocab['concept_relationship'] = pd.read_csv(os.path.join(BASE_OMOP_INPUT_DIRECTORY,'CONCEPT_RELATIONSHIP.csv.gz'), \
             sep='\t', dtype=model_omop.model_schema['concept_relationship'])
        print('concept_relationship: ' + self.mem_usage(vocab['concept_relationship']))
        vocab['concept_synonym'] = pd.read_csv(os.path.join(BASE_OMOP_INPUT_DIRECTORY,'CONCEPT_SYNONYM.csv.gz'), \
             sep='\t', dtype=model_omop.model_schema['concept_synonym'])
        print('concept_synonym: ' + self.mem_usage(vocab['concept_synonym']))
        vocab['concept_ancestor'] = pd.read_csv(os.path.join(BASE_OMOP_INPUT_DIRECTORY,'CONCEPT_ANCESTOR.csv.gz'), \
             sep='\t', dtype=model_omop.model_schema['concept_ancestor'])
        print('concept_ancestor: ' + self.mem_usage(vocab['concept_synonym']))
        vocab['concept_class'] = pd.read_csv(os.path.join(BASE_OMOP_INPUT_DIRECTORY,'CONCEPT_CLASS.csv.gz'), \
             sep='\t', dtype=model_omop.model_schema['concept_class'])
        print('concept_class: ' + self.mem_usage(vocab['concept_class']))
        vocab['drug_strength'] = pd.read_csv(os.path.join(BASE_OMOP_INPUT_DIRECTORY,'DRUG_STRENGTH.csv.gz'), \
             sep='\t', dtype=model_omop.model_schema['drug_strength'])
        print(util.mem_usage(vocab['drug_strength']))
        return vocab

    #
    # following standard omop query for source to standard mapping is implemented in python pandas
    # passing in a vocabulary dictionary, return the source to standard dataframe
    #
    #   SELECT c.concept_code AS SOURCE_CODE, c.concept_id AS SOURCE_CONCEPT_ID,
    #         c.concept_name AS SOURCE_CODE_DESCRIPTION,
    #         c.vocabulary_id AS SOURCE_VOCABULARY_ID, c.domain_id AS SOURCE_DOMAIN_ID,
    #         c.CONCEPT_CLASS_ID AS SOURCE_CONCEPT_CLASS_ID, c.VALID_START_DATE AS SOURCE_VALID_START_DATE,
    #         c.VALID_END_DATE AS SOURCE_VALID_END_DATE, c.INVALID_REASON AS SOURCE_INVALID_REASON,
    #         c1.concept_id AS TARGET_CONCEPT_ID, c1.concept_name AS TARGET_CONCEPT_NAME,
    #         c1.VOCABULARY_ID AS TARGET_VOCABULARY_ID, c1.domain_id AS TARGET_DOMAIN_ID,
    #         c1.concept_class_id AS TARGET_CONCEPT_CLASS_ID,
    #         c1.INVALID_REASON AS TARGET_INVALID_REASON,
    #         c1.standard_concept AS TARGET_STANDARD_CONCEPT
    #   FROM CONCEPT C
    #   JOIN CONCEPT_RELATIONSHIP CR
    #         ON C.CONCEPT_ID = CR.CONCEPT_ID_1
    #         AND CR.invalid_reason IS NULL
    #         AND lower(cr.relationship_id) = 'maps to'
    #   JOIN CONCEPT C1
    #         ON CR.CONCEPT_ID_2 = C1.CONCEPT_ID
    #         AND C1.INVALID_REASON IN (NULL,'')
    def sourceToStandardVocabMap(self, vocab, model_omop):
        concept = vocab['concept']
        concept_relationship = vocab['concept_relationship']
        source = concept[model_schema['source_to_standard_source'].keys()]  # get rid of columns we don't need
        source = source.rename(columns=model_schema['source_to_standard_source'])
        target = concept[model_schema['source_to_standard_target'].keys()]  # get rid of columns we don't need
        target = target.rename(columns=model_schema['source_to_standard_target'])
        source_result = pd.merge(source,concept_relationship[concept_relationship["invalid_reason"].isnull() & concept_relationship["relationship_id"].str.contains('Maps to')], \
            how='inner', left_on='source_concept_id', right_on='concept_id_1')
        target_result = pd.merge(target,concept_relationship[concept_relationship["invalid_reason"].isnull()], \
            how='inner', left_on='target_concept_id', right_on='concept_id_2')
        result = pd.merge(source_result, target_result, how='inner', left_on='source_concept_id', right_on='target_concept_id')
        return result

    #
    # following standard omop query for source to source mapping is implemented in python pandas
    # passing in a vocabulary dictionary, return the source to standard dataframe
    #
    #   SELECT c.concept_code AS SOURCE_CODE, c.concept_id AS SOURCE_CONCEPT_ID, 
    #         c.CONCEPT_NAME AS SOURCE_CODE_DESCRIPTION,  c.vocabulary_id AS SOURCE_VOCABULARY_ID,
    #         c.domain_id AS SOURCE_DOMAIN_ID, c.concept_class_id AS SOURCE_CONCEPT_CLASS_ID, 
    #         c.VALID_START_DATE AS SOURCE_VALID_START_DATE, c.VALID_END_DATE AS SOURCE_VALID_END_DATE, 
    #         c.invalid_reason AS SOURCE_INVALID_REASON, c.concept_ID as TARGET_CONCEPT_ID,
    #         c.concept_name AS TARGET_CONCEPT_NAME, c.vocabulary_id AS TARGET_VOCABULARY_ID, c.domain_id AS TARGET_DOMAIN_ID, 
    #         c.concept_class_id AS TARGET_CONCEPT_CLASS_ID, c.INVALID_REASON AS TARGET_INVALID_REASON, 
    #         c.STANDARD_CONCEPT AS TARGET_STANDARD_CONCEPT
    #   FROM CONCEPT c
    def sourceToSourceVocabMap(self, vobab, model_omop):
        concept = vocab['concept']
        source = concept[model_schema['source_to_standard_source'].keys()]  # get rid of columns we don't need
        source = source.rename(columns=model_schema['source_to_standard_source'])
        target = concept[model_schema['source_to_standard_target'].keys()]  # get rid of columns we don't need
        target = target.rename(columns=model_schema['source_to_standard_target'])
        result = pd.merge(source_result, target_result, how='inner', left_on='source_concept_id', right_on='target_concept_id')
        return result
