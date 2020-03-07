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

    # take concepts and find all related concept relationships that "map to"
    def getConceptRelationshipExtract(self, concept_relationship, concept):
        df1 = pd.merge(concept_relationship, concept[['concept_id']], left_on='concept_id_1', right_on='concept_id', how='inner').drop(columns=['concept_id'])
        df2 = pd.merge(concept_relationship, concept[['concept_id']], left_on='concept_id_2', right_on='concept_id', how='inner').drop(columns=['concept_id'])
        conrel = df1.append(df2)
        return conrel.loc[conrel['relationship_id'] == 'Maps to']

    def conditionsExtract(self, df, concept):
        df1 = self.getConceptExtract(df, concept, 'Condition')
        df2 = self.getConceptExtract(df, concept, 'Drug')
        df3 = self.getConceptExtract(df, concept, 'Observation')
        return df1.append(df2).append(df3)

    def observationsExtract(self, df, concept):
        return self.getConceptExtract(df, concept, 'Measurement')

    def proceduresExtract(self, df, concept):
        return self.getConceptExtract(df, concept, 'Procedure')

    def immunizationsExtract(self, df, concept):
        return self.getConceptExtract(df, concept, 'Drug')

    def encountersExtract(self, df, vocab):
        pass

    def allergiesExtract(self, df, vocab):
        return self.getConceptExtract(df, concept, 'Observation')

    def medicationsExtract(self, df, concept):
        return self.getConceptExtract(df, concept, 'Drug')
