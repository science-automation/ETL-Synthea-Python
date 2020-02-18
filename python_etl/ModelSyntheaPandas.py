#
# define types for omop schema
#
class ModelSyntheaPandas:
    #
    # Check the model matches
    #
    def __init__(self):
       self.model_schema = self.syntheaSchema()

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
        model_schema['allergies'] = {
            'start': 'object',
            'stop': 'object',
            'patient': 'object',
            'encounter': 'object',
            'code': 'category',
            'description': 'category'
        }

        model_schema['careplans'] = {
            'id': 'object',
            'start': 'object',
            'stop': 'object',
            'patient': 'object',
            'encounter': 'object',
            'code': 'category',
            'description': 'category',
            'reasoncode': 'category',
            'reasondescription': 'category'
        }

        model_schema['conditions'] = {
            'start': 'object',
            'stop': 'object',
            'patient': 'object',
            'encounter': 'object',
            'code': 'category',
            'description': 'category'
        }

        model_schema['encounters'] = {
            'id': 'object',
            'start': 'object',
            'stop': 'object',
            'patient': 'object',
            'provider': 'category',
            'payer': 'object',
            'encounterclass': 'category',
            'code': 'category',
            'description': 'category',
            'base_encounter_cost': 'float',
            'total_claim_cost': 'float',
            'payer_coverage': 'float',
            'reasoncode': 'category',
            'reasondescription': 'category'

        }

        model_schema['imaging_studies'] = {
            'id': 'object',
            'date': 'object',
            'patient': 'object',
            'encounter': 'object',
            'body_site_code': 'category',
            'body_site_description': 'category',
            'modality_code': 'category',
            'modality_description': 'category',
            'sop_code': 'category',
            'sop_description': 'category'
        }

        model_schema['immunizations'] = {
            'date': 'object',
            'patient': 'object',
            'encounter': 'object',
            'code': 'category',
            'description': 'category',
            'cost': 'float'
        }

        model_schema['medications'] = {
            'start': 'object',
            'stop': 'object',
            'patient': 'object',
            'payer': 'category',
            'encounter': 'object',
            'code': 'category',
            'description': 'category',
            'base_cost': 'float',
            'payer_coverage': 'float',
            'dispenses': 'integer',  # small int
            'totalcost': 'float',
            'reasoncode': 'category',
            'reasondescription': 'category'
        }

        model_schema['observations'] = {
            'date': 'object',
            'patient': 'object',
            'encounter': 'object',
            'code': 'category',
            'description': 'category',
            'value': 'category',
            'units': 'category',
            'type': 'category'
        }

        model_schema['organizations'] = {
            'id': 'object',
            'name': 'object',
            'address': 'object',
            'city': 'category',
            'state': 'category',
            'zip': 'category',
            'lat': 'float',
            'lon': 'float',
            'phone': 'object',
            'revenue': 'float',
            'utilization': 'float'
        }

        model_schema['patients'] = {
            'id': 'object',
            'birthdate': 'object',
            'deathdate': 'object',
            'ssn': 'object',
            'drivers': 'object',
            'passport': 'object',
            'prefix': 'category',
            'first': 'category',
            'last': 'category',
            'suffix': 'category',
            'maiden': 'category',
            'marital': 'category',
            'race': 'category',
            'ethnicity': 'category',
            'gender': 'category',
            'birthplace': 'category',
            'address': 'object',
            'city': 'category',
            'county': 'category',
            'state': 'category',
            'zip': 'category',
            'lat': 'float',
            'lon': 'float',
            'healthcare_expense': 'float',
            'healthcare_coverage': 'float'
        }

        model_schema['payer_transitions'] = {
            'patient': 'object',
            'start_year': 'object',
            'stop_year': 'object',
            'patient': 'object',
            'payer': 'category',
            'ownership': 'category'
        }

        model_schema['payers'] = {
            'id': 'object',
            'name': 'object',
            'address': 'object',
            'city': 'object',
            'state_headquartered': 'category',
            'zip': 'category',
            'phone': 'cateogry',
            'amount_covered': 'float',
            'revenue': 'float',
            'covered_encounters': 'float',
            'uncovered_encounters': 'float',
            'covered_medications': 'float',
            'uncovered_medications': 'float',
            'covered_procedures': 'float',
            'uncovered_procedures': 'float',
            'covered_immunizations': 'float',
            'uncovered_immunizations': 'float',
            'unique_customers': 'float',
            'qols_avg': 'float',
            'member_months': 'float'
        }

        model_schema['procedures'] = {
            'date': 'object',
            'patient': 'object',
            'encounter': 'object',
            'code': 'category',
            'description': 'category',
            'base_cost': 'category',
            'reasoncode': 'category',
            'reasondescription': 'category'
        }

        model_schema['providers'] = {
            'id': 'object',
            'organization': 'object',
            'name': 'object',
            'gender': 'category',
            'speciality': 'category',
            'address': 'object',
            'city': 'category',
            'state': 'state',
            'zip': 'float',
            'lat': 'float',
            'lon': 'float',
            'utilization': 'float'
        }
