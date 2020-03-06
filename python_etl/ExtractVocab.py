import pandas as pd
import datetime
import dateutil.parser

#
# given a synthea input set, find all concept and concept_relationships needed
#
class ExtractVocab:
    #
    # Check the model matches
    #
    def __init__(self, model_schema):
       self.model_schema = model_schema

    def conditionsExtract(self, df, vocab):
        concept = vocab['concept']
        syntheaconcept = pd.DataFrame(columns=['code'])
        syntheaconcept['code'] = df['CODE']
        syntheaconcept['code'] = syntheaconcept['code'].drop_duplicates()
        domainconcept = concept[(concept["domain_id"]=='Conditions') & (concept["domain_id"]=='Drug')]
        result = pd.merge(domainconcept, syntheaconcept, left_on='concept_code', right_on='code', how='inner').drop(columns=['code'])
        return result

    def observationsExtract(self, df, vocab):
        concept = vocab['concept']
        syntheaconcept = pd.DataFrame(columns=['code'])
        syntheaconcept['code'] = df['CODE']
        syntheaconcept['code'] = syntheaconcept['code'].drop_duplicates() 
        domainconcept = concept[(concept["domain_id"]=='Measurement')]
        result = pd.merge(domainconcept, syntheaconcept, left_on='concept_code', right_on='code', how='inner').drop(columns=['code'])
        return result

    def proceduresExtract(self, df, vocab):
        concept = vocab['concept']
        syntheaconcept = pd.DataFrame(columns=['code'])
        syntheaconcept['code'] = df['CODE']
        syntheaconcept['code'] = syntheaconcept['code'].drop_duplicates()
        domainconcept = concept[(concept["domain_id"]=='Procedure')]
        result = pd.merge(domainconcept, syntheaconcept, left_on='concept_code', right_on='code', how='inner').drop(columns=['code'])
        return result

    def immunizationsExtract(self, df, vocab):
        concept = vocab['concept']
        syntheaconcept = pd.DataFrame(columns=['code'])
        syntheaconcept['code'] = df['CODE']
        syntheaconcept['code'] = syntheaconcept['code'].drop_duplicates()
        domainconcept = concept[(concept["domain_id"]=='Drug')]
        result = pd.merge(domainconcept, syntheaconcept, left_on='concept_code', right_on='code', how='inner').drop(columns=['code'])
        return result

    def encountersExtract(self, df, vocab):
        pass

    def allergiesExtract(self, df, vocab):
        pass

    def medicationsExtract(self, df, vocab):
        concept = vocab['concept']
        syntheaconcept = pd.DataFrame(columns=['code'])
        syntheaconcept['code'] = df['CODE']
        syntheaconcept['code'] = syntheaconcept['code'].drop_duplicates()
        domainconcept = concept[(concept["domain_id"]=='Drug')]
        result = pd.merge(domainconcept, syntheaconcept, left_on='concept_code', right_on='code', how='inner').drop(columns=['code'])
        return result
