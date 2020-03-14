from collections import OrderedDict

#
# define types for omop schema
#
class ModelData:
    #
    # Check the model matches
    #
    def __init__(self):
       self.model_schema = self.dataSchema()

    #
    # define schema for various data loads being used
    #
    def dataSchema(self):
        model_schema = OrderedDict([])
        #
        # Standardized vocabulary
        #
        model_schema['openaddress'] = OrderedDict([
            ('LAT', 'object'),
            ('LON', 'object'),
            ('NUMBER', 'category'),
            ('STREET', 'category'),
            ('UNIT', 'category'),
            ('CITY', 'category'),
            ('DISTRICT', 'category'),
            ('REGION', 'category'),
            ('POSTCODE', 'category'),
            ('ID', 'object'),
            ('HASH','object')
        ])

        return model_schema
