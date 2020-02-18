#
# define types for synthea schema
#
class ModelSyntheaPandas:
    #
    # Check the model matches
    #
    def __init__(self):
       self.model_schema = self.syntheaSchema()

    #
    # define omop schema here
    # The DATE fields are read in as string.  They can be converted to DATE
    # objects if necessary
    # OMOP V5.3.1
    #
    def syntheaSchema(self):
        model_schema = {}
        #
        # Standardized vocabulary
        #
        model_schema['allergies'] = {
            'START': 'object',
            'STOP': 'object',
            'PATIENT': 'object',
            'ENCOUNTER': 'object',
            'CODE': 'category',
            'DESCRIPTION': 'category'
        }

        model_schema['careplans'] = {
            'Id': 'object',
            'START': 'object',
            'STOP': 'object',
            'PATIENT': 'object',
            'ENCOUNTER': 'object',
            'CODE': 'category',
            'DESCRIPTION': 'category',
            'REASONCODE': 'category',
            'REASONDESCRIPTION': 'category'
        }

        model_schema['conditions'] = {
            'START': 'object',
            'STOP': 'object',
            'PATIENT': 'object',
            'ENCOUNTER': 'object',
            'CODE': 'category',
            'DESCRIPTION': 'category'
        }

        model_schema['encounters'] = {
            'Id': 'object',
            'START': 'object',
            'STOP': 'object',
            'PATIENT': 'category',
            'PROVIdER': 'category',
            'PAYER': 'category',
            'ENCOUNTERCLASS': 'category',
            'CODE': 'category',
            'DESCRIPTION': 'category',
            'BASE_ENCOUNTER_COST': 'float16',
            'TOTAL_CLAIM_COST': 'float16',
            'PAYER_COVERAGE': 'float16',
            'REASONCODE': 'category',
            'REASONDESCRIPTION': 'category'
        }

        model_schema['imaging_studies'] = {
            'Id': 'object',
            'DATE': 'object',
            'PATIENT': 'object',
            'ENCOUNTER': 'object',
            'BODY_SITE_CODE': 'category',
            'BODY_SITE_DESCRIPTION': 'category',
            'MODALITY_CODE': 'category',
            'MODALITY_DESCRIPTION': 'category',
            'SOP_CODE': 'category',
            'SOP_DESCRIPTION': 'category'
        }

        model_schema['immunizations'] = {
            'DATE': 'object',
            'PATIENT': 'object',
            'ENCOUNTER': 'object',
            'CODE': 'category',
            'DESCRIPTION': 'category',
            'COST': 'float32'
        }

        model_schema['medications'] = {
            'START': 'object',
            'STOP': 'object',
            'PATIENT': 'category',
            'PAYER': 'category',
            'ENCOUNTER': 'object',
            'CODE': 'category',
            'DESCRIPTION': 'category',
            'BASE_COST': 'float32',
            'PAYER_coverage': 'float16',
            'DISPENSES': 'int8',
            'TOTALCOST': 'float16',
            'REASONCODE': 'category',
            'REASONDESCRIPTION': 'category'
        }

        model_schema['observations'] = {
            'DATE': 'category',
            'PATIENT': 'category',
            'ENCOUNTER': 'category',
            'CODE': 'category',
            'DESCRIPTION': 'category',
            'VALUE': 'category',
            'UNITS': 'category',
            'TYPE': 'category'
        }

        model_schema['organizations'] = {
            'Id': 'object',
            'NAME': 'object',
            'ADDRESS': 'object',
            'CITY': 'category',
            'STATE': 'category',
            'ZIP': 'category',
            'LAT': 'float32',
            'LON': 'float32',
            'PHONE': 'object',
            'REVENUE': 'float32',
            'utilization': 'float32'
        }

        model_schema['patients'] = {
            'Id': 'object',
            'BIRTHDATE': 'object',
            'DEATHDATE': 'object',
            'SSN': 'object',
            'DRIVERS': 'object',
            'PASSPORT': 'object',
            'PREFIX': 'category',
            'FIRST': 'category',
            'LAST': 'category',
            'SUFFIX': 'category',
            'MAIDEN': 'category',
            'MARITAL': 'category',
            'RACE': 'category',
            'ETHNICITY': 'category',
            'GENDER': 'category',
            'BIRTHPLACE': 'category',
            'ADDRESS': 'object',
            'CITY': 'category',
            'COUNTY': 'category',
            'STATE': 'category',
            'ZIP': 'category',
            'LAT': 'float32',
            'LON': 'float32',
            'HEALTHCARE_EXPENSE': 'float32',
            'HEALTHCARE_COVERAGE': 'float32'
        }

        model_schema['payer_transitions'] = {
            'PATIENT': 'object',
            'START_YEAR': 'object',
            'STOP_YEAR': 'object',
            'PATIENT': 'object',
            'PAYER': 'category',
            'OWNERSHIP': 'category'
        }

        model_schema['payers'] = {
            'Id': 'object',
            'NAME': 'object',
            'ADDRESS': 'object',
            'CITY': 'object',
            'STATE_HEADQUARTERED': 'category',
            'ZIP': 'category',
            'PHONE': 'cateogry',
            'AMOUNT_COVERED': 'float32',
            'REVENUE': 'float32',
            'COVERED_ENCOUNTERs': 'float32',
            'UNCOVERED_ENCOUNTERs': 'float32',
            'COVERED_MEDICATIONS': 'float32',
            'UNCOVERED_MEDICATIONS': 'float32',
            'COVERED_PROCESURES': 'float32',
            'UNCOVERED_PROCESURES': 'float32',
            'COVERED_IMMUNIZATIONS': 'float32',
            'UNCOVERED_IMMUNIZATIONS': 'float32',
            'UNIQUE_CUSTOMERS': 'float32',
            'QOLS_AVG': 'float32',
            'MEMBER_MONTHS': 'float32'
        }

        model_schema['procedures'] = {
            'DATE': 'object',
            'PATIENT': 'category',
            'ENCOUNTER': 'object',
            'CODE': 'category',
            'DESCRIPTION': 'category',
            'BASE_COST': 'category',
            'REASONCODE': 'category',
            'REASONDESCRIPTION': 'category'
        }

        model_schema['providers'] = {
            'Id': 'object',
            'ORGANIZATION': 'object',
            'NAME': 'object',
            'GENDER': 'category',
            'SPECIALITY': 'category',
            'ADDRESS': 'object',
            'CITY': 'category',
            'STATE': 'category',
            'ZIP': 'float32',
            'LAT': 'float32',
            'LON': 'float32',
            'UTILIZATION': 'float32'
        }
        return model_schema
