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
            'code': 'object',
            'description': 'object'
        }

        model_schema['careplans'] = {
            'id': 'object',
            'start': 'object',
            'stop': 'object',
            'patient': 'object',
            'encounter': 'object',
            'code': 'object',
            'description': 'object',
            'reasoncode': 'object',
            'reasondescription': 'object'
        }

        model_schema['conditions'] = {
            'start': 'object',
            'stop': 'object',
            'patient': 'object',
            'encounter': 'object',
            'code': 'object',
            'description': 'object'
        }

        model_schema['encounters'] = {
            'id': 'object',
            'start': 'object',
            'stop': 'object',
            'patient': 'object',
            'provider': 'object',
            'payer': 'object',
            'encounterclass': 'object',
            'code': 'object',
            'description': 'object',
            'base_encounter_cost': 'float32',
            'total_claim_cost': 'float32',
            'payer_coverage': 'float32',
            'reasoncode': 'object',
            'reasondescription': 'object'

        }

        model_schema['imaging_studies'] = {
            'id': 'object',
            'date': 'object',
            'patient': 'object',
            'encounter': 'object',
            'body_site_code': 'object',
            'body_site_description': 'object',
            'modality_code': 'object',
            'modality_description': 'object',
            'sop_code': 'object',
            'sop_description': 'object'
        }

        model_schema['immunizations'] = {
            'date': 'object',
            'patient': 'object',
            'encounter': 'object',
            'code': 'object',
            'description': 'object',
            'cost': 'float32'
        }

        model_schema['medications'] = {
            'start': 'object',
            'stop': 'object',
            'patient': 'object',
            'payer': 'object',
            'encounter': 'object',
            'code': 'object',
            'description': 'object',
            'base_cost': 'float32',
            'payer_coverage': 'float32',
            'dispenses': 'int8',  # small int
            'totalcost': 'float32',
            'reasoncode': 'object',
            'reasondescription': 'object'
        }

        model_schema['observations'] = {
            'date': 'object',
            'patient': 'object',
            'encounter': 'object',
            'code': 'object',
            'description': 'object',
            'value': 'object',
            'units': 'object',
            'type': 'object'
        }

        model_schema['organizations'] = {
            'id': 'object',
            'name': 'object',
            'address': 'object',
            'city': 'object',
            'state': 'object',
            'zip': 'object',
            'lat': 'float32',
            'lon': 'float32',
            'phone': 'object',
            'revenue': 'float32',
            'utilization': 'float32'
        }

        model_schema['patients'] = {
            'id': 'object',
            'birthdate': 'object',
            'deathdate': 'object',
            'ssn': 'object',
            'drivers': 'object',
            'passport': 'object',
            'prefix': 'object',
            'first': 'object',
            'last': 'object',
            'suffix': 'object',
            'maiden': 'object',
            'marital': 'object',
            'race': 'object',
            'ethnicity': 'object',
            'gender': 'object',
            'birthplace': 'object',
            'address': 'object',
            'city': 'object',
            'county': 'object',
            'state': 'object',
            'zip': 'object',
            'lat': 'float32',
            'lon': 'float32',
            'healthcare_expense': 'float32',
            'healthcare_coverage': 'float32'
        }

        model_schema['payer_transitions'] = {
            'patient': 'object',
            'start_year': 'object',
            'stop_year': 'object',
            'patient': 'object',
            'payer': 'object',
            'ownership': 'object'
        }

        model_schema['payers'] = {
            'id': 'object',
            'name': 'object',
            'address': 'object',
            'city': 'object',
            'state_headquartered': 'object',
            'zip': 'object',
            'phone': 'cateogry',
            'amount_covered': 'float32',
            'revenue': 'float32',
            'covered_encounters': 'float32',
            'uncovered_encounters': 'float32',
            'covered_medications': 'float32',
            'uncovered_medications': 'float32',
            'covered_procedures': 'float32',
            'uncovered_procedures': 'float32',
            'covered_immunizations': 'float32',
            'uncovered_immunizations': 'float32',
            'unique_customers': 'float32',
            'qols_avg': 'float32',
            'member_months': 'float32'
        }

        model_schema['procedures'] = {
            'date': 'object',
            'patient': 'object',
            'encounter': 'object',
            'code': 'object',
            'description': 'object',
            'base_cost': 'object',
            'reasoncode': 'object',
            'reasondescription': 'object'
        }

        model_schema['providers'] = {
            'id': 'object',
            'organization': 'object',
            'name': 'object',
            'gender': 'object',
            'speciality': 'object',
            'address': 'object',
            'city': 'object',
            'state': 'object',
            'zip': 'float32',
            'lat': 'float32',
            'lon': 'float32',
            'utilization': 'float32'
        }
        return model_schema
