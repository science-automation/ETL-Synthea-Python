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

    def getExtract(self, df, concept, domain):
        syntheaconcept = pd.DataFrame(columns=['code'])
        syntheaconcept['code'] = df['CODE'].drop_duplicates()
        domainconcept = concept[concept["domain_id"]==domain]
        return pd.merge(domainconcept, syntheaconcept, left_on='concept_code', right_on='code', how='inner').drop(columns=['code'])

    def conditionsExtract(self, df, vocab):
        concept = vocab['concept']
        df1 = self.getExtract(df, concept, 'Conditions')
        df2 = self.getExtract(df, concept, 'Drug')
        return df1.append(df2)

    def observationsExtract(self, df, vocab):
        concept = vocab['concept']
        return self.getExtract(df, concept, 'Measurement')

    def proceduresExtract(self, df, vocab):
        concept = vocab['concept']
        return self.getExtract(df, concept, 'Procedure')

    def immunizationsExtract(self, df, vocab):
        concept = vocab['concept']
        return self.getExtract(df, concept, 'Drug')

    def encountersExtract(self, df, vocab):
        pass

    def allergiesExtract(self, df, vocab):
        pass

    def medicationsExtract(self, df, vocab):
        concept = vocab['concept']
        return self.getExtract(df, concept, 'Drug')
