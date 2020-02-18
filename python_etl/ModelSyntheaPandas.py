#
# define types for omop schema
#
class ModelSyntheaPandas:
    #
    # Check the model matches
    #
    def __init__(self):
       self.model_schema = self.omopSchema()

    #
    # define omop schema here
    # The date fields are read in as string.  They can be converted to date
    # objects if necessary
    # OMOP V5.3.1
    #
    def syntheaSchema(self):
        model_schema = {}
        #
        # Standardized vocabulary
        #
        model_schema[concept] = {
            'concept_id': 'integer',
            'concept_name': 'category',
            'domain_id': 'category',
            'vocabulary_id': 'category',
            'concept_class_id': 'category',
            'standard_concept': 'category',
            'concept_code': 'category',
            'valid_start_date': 'object',
            'valid_end_date': 'object',
            'invalid_reason': 'category'
        }

