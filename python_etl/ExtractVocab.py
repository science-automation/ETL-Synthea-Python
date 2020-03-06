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

    def getConceptExtract(self, df, concept, domain):
        syntheaconcept = pd.DataFrame(columns=['code'])
        syntheaconcept['code'] = df['CODE'].drop_duplicates()
        domainconcept = concept[concept["domain_id"]==domain]
        return pd.merge(domainconcept, syntheaconcept, left_on='concept_code', right_on='code', how='inner').drop(columns=['code'])

    def conditionsExtract(self, df, concept):
        df1 = self.getConceptExtract(df, concept, 'Conditions')
        df2 = self.getConceptExtract(df, concept, 'Drug')
        return df1.append(df2)

    def observationsExtract(self, df, concept):
        return self.getConceptExtract(df, concept, 'Measurement')

    def proceduresExtract(self, df, concept):
        return self.getConceptExtract(df, concept, 'Procedure')

    def immunizationsExtract(self, df, concept):
        return self.getConceptExtract(df, concept, 'Drug')

    def encountersExtract(self, df, vocab):
        pass

    def allergiesExtract(self, df, vocab):
        pass

    def medicationsExtract(self, df, concept):
        return self.getConceptExtract(df, concept, 'Drug')
