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

    def conditionsExtract(self, df):
        pass

    def observationsExtract(self, df):
        pass

    def proceduresExtract(self, df):
        pass

    def immunizationsExtract(self, df):
        pass

    def encountersExtract(self, df):
        pass

    def allergiesExtract(self, df):
        pass

    def medicationsExtract(self, df):
        pass
