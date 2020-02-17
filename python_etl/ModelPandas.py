#
# define types for omop schema
#
class Model:
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
    def omopSchema(self):
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

        model_schema[vocabulary] = {
            'vocabulary_id': 'object',
            'vocabulary_name': 'category',
            'vocabulary_reference': 'category',
            'vocabulary_version': 'category',
            'vocabulary_concept_id': 'integer'
        }

        model_schema[domain] = {
            'domain_id': 'category',
            'domain_name': 'category',
            'domain_concept_id': 'integer'
        }

        model_schema[concept_class] = {
            'concept_class_id': 'category',
            'concept_class_name': 'category',
            'concept_class_concept_id': 'integer'
        }

        model_schema[concept_relationship] = {
            'concept_id_1': 'integer',
            'concept_id_2': 'integer',
            'relationship_id': 'category',
            'valid_start_date': 'object',
            'valid_end_date': 'object',
            'invalid_reason': 'category'
        }

        model_schema[relationship] = {
            'relationship_id': 'category',
            'relationship_name': 'category',
            'is_hierarchical': 'category',
            'defines_ancestry': 'category',
            'reverse_relationship_id': 'category',
            'relationship_concept_id': 'integer'  
        }

        model_schema[concept_synonym]
            'concept_id': 'integer',
            'concept_synonym_name': 'category',
            'language_concept_id': 'integer'
        }

        model_schema[concept_ancestor]
            'ancestor_concept_id': 'integer',
            'descendant_concept_id': 'integer',
            'min_levels_of_separation':'integer',
            'max_levels_of_separation':'integer'
        }

        model_schema[source_to_concept_map]
            'source_code': 'category',
            'source_concept_id': 'integer',
            'source_vocabulary_id': 'category',
            'source_code_description': 'category',
            'target_concept_id': 'integer',
            'target_vocabulary_id': 'category',
            'valid_start_date':'object',
            'valid_end_date': 'object',
            'invalid_reason': 'category'
        }

        model_schema[drug_strength] = {
            'drug_concept_id': 'integer',
            'ingredient_concept_id': 'integer',
            'amount_value': 'float',
            'amount_unit_concept_id': 'integer',
            'numerator_value': 'float' ,
            'numerator_unit_concept_id': 'integer',
            'denominator_value': 'float' ,
            'denominator_unit_concept_id': 'integer',
            'box_size': 'integer',
            'valid_start_date': 'object',
            'valid_end_date': 'object',
            'invalid_reason: 'category'
        }

        model_schema[cohort_definition]
            'cohort_definition_id': 'integer',
            'cohort_definition_name': 'category',
            'cohort_definition_description': 'object',
            'definition_type_concept_id: 'integer',
            'cohort_definition_syntax': 'object',
            'subject_concept_id': 'integer',
            'cohort_initiation_date':'object'    

        model_schema[attribute_definition]
  attribute_definition_id  'integer',
  attribute_name      'category',
  attribute_description  'object',
  attribute_type_concept_id'integer',
  attribute_syntax    'object'

# Standardized meta-data
        model_schema[cdm_source
  cdm_source_name        'category',
  cdm_source_abbreviation    'category'25),
  cdm_holder        'category',
  source_description    'object',
  source_documentation_reference'category',
  cdm_etl_reference      'category',
  source_release_date      'object'  ,
  cdm_release_date      'object'  ,
  cdm_version          'category'10),
  vocabulary_version    'category'

        model_schema[metadata
  metadata_concept_id       'integer'     ,
  metadata_type_concept_id  'integer'     ,
  name                      'category'250,
  value_as_string           'object',
  value_as_concept_id       'integer'     ,
  metadata_date             'object'        ,
  metadata_datetime         'object'TIME2      

# Standardized clinical data
        model_schema[person
(
  person_id        'integer',
  gender_concept_id    'integer',
  year_of_birth      'integer',
  month_of_birth      'integer',
  day_of_birth      'integer',
  birth_datetime      'object'TIME2,
  race_concept_id      'integer',
  ethnicity_concept_id  'integer',
  location_id        'integer',
  provider_id        'integer',
  care_site_id      'integer',
  person_source_value    'category'50,
  gender_source_value    'category'50,
  gender_source_concept_id  'integer',
  race_source_value    'category'50,
  race_source_concept_id  'integer',
  ethnicity_source_value  'category'50,
  ethnicity_source_concept_id'integer'  

        model_schema[observation_period
  observation_period_id      'integer',
  person_id            'integer',
  observation_period_start_date  'object',
  observation_period_end_date    'object',
  period_type_concept_id      'integer'

        model_schema[specimen
  specimen_id      'integer',
  person_id      'integer',
  specimen_concept_id  'integer',
  specimen_type_concept_id'integer',
  specimen_date    'object',
  specimen_datetime  'object'TIME2,
  quantity      'float',
  unit_concept_id    'integer',
  anatomic_site_concept_id'integer',
  disease_status_concept_id'integer',
  specimen_source_id  'category',
  specimen_source_value'category',
  unit_source_value  'category',
  anatomic_site_source_value'category',
  disease_status_source_value 'category'

        model_schema[death
  person_id  'integer',
  death_date'object',
  death_datetime'object'TIME2,
  death_type_concept_id   'integer',
  cause_concept_id  'integer',
  cause_source_value'category',
  cause_source_concept_id 'integer'

        model_schema[visit_occurrence
  visit_occurrence_id      'integer',
  person_id          'integer',
  visit_concept_id      'integer',
  visit_start_date      'object',
  visit_start_datetime  'object'TIME2,
  visit_end_date      'object',
  visit_end_datetime  'object'TIME2,
  visit_type_concept_id    'integer',
  provider_id          'integer',
  care_site_id        'integer',
  visit_source_value    'category',
  visit_source_concept_id    'integer',
  admitting_source_concept_id  'integer',
  admitting_source_value    'category',
  discharge_to_concept_id    'integer' ,
  discharge_to_source_value  'category',
  preceding_visit_occurrence_id'integer'

        model_schema[visit_detail
  visit_detail_id                    'integer'   ,
  person_id                          'integer'   ,
  visit_detail_concept_id            'integer'   ,
  visit_detail_start_date            'object'      ,
  visit_detail_start_datetime        'object'TIME2  ,
  visit_detail_end_date              'object'      ,
  visit_detail_end_datetime          'object'TIME2  ,
  visit_detail_type_concept_id       'integer'   ,
  provider_id                        'integer'   ,
  care_site_id                       'integer'   ,
  admitting_source_concept_id        'integer'   ,
  discharge_to_concept_id            'integer'   ,
  preceding_visit_detail_id          'integer'   ,
  visit_detail_source_value          'category'50,
  visit_detail_source_concept_id     'integer'   ,
  admitting_source_value             'category'50,
  discharge_to_source_value          'category'50,
  visit_detail_parent_id             'integer'   ,
  visit_occurrence_id                'integer'     

        model_schema[procedure_occurrence
  procedure_occurrence_id  'integer',
  person_id        'integer',
  procedure_concept_id  'integer',
  procedure_date      'object',
  procedure_datetime    'object'TIME2,
  procedure_type_concept_id'integer',
  modifier_concept_id    'integer',
  quantity        'integer',
  provider_id        'integer',
  visit_occurrence_id    'integer',
  visit_detail_id             'integer'   ,
  procedure_source_value  'category',
  procedure_source_concept_id'integer',
  modifier_source_value   'category'

        model_schema[drug_exposure
  drug_exposure_id      'integer',
  person_id          'integer',
  drug_concept_id        'integer',
  drug_exposure_start_date  'object'  ,
  drug_exposure_start_datetime  'object'TIME2,
  drug_exposure_end_date    'object'  ,
  drug_exposure_end_datetime  'object'TIME2,
  verbatim_end_date      'object'  ,
  drug_type_concept_id    'integer',
  stop_reason          'category',
  refills            'integer',
  quantity          'float'  ,
  days_supply          'integer',
  sig              'object',
  route_concept_id      'integer',
  lot_number          'category',
  provider_id          'integer',
  visit_occurrence_id      'integer',
  visit_detail_id               'integer'     ,
  drug_source_value      'category',
  drug_source_concept_id    'integer',
  route_source_value      'category',
  dose_unit_source_value    'category'  

        model_schema[device_exposure
  device_exposure_id        'integer',
  person_id            'integer',
  device_concept_id        'integer',
  device_exposure_start_date    'object'  ,
  device_exposure_start_datetime  'object'TIME2,
  device_exposure_end_date    'object'  ,
  device_exposure_end_datetime    'object'TIME2,
  device_type_concept_id      'integer',
  unique_device_id        'category',
  quantity            'integer',
  provider_id            'integer',
  visit_occurrence_id        'integer',
  visit_detail_id                 'integer'     ,
  device_source_value        'category'100),
  device_source_concept_id    'integer'  

        model_schema[condition_occurrence
  condition_occurrence_id    'integer',
  person_id          'integer',
  condition_concept_id    'integer',
  condition_start_date    'object',
  condition_start_datetime  'object'TIME2,
  condition_end_date      'object',
  condition_end_datetime    'object'TIME2,
  condition_type_concept_id  'integer',
  stop_reason          'category',
  provider_id          'integer',
  visit_occurrence_id      'integer',
  visit_detail_id               'integer'   ,
  condition_source_value    'category',
  condition_source_concept_id  'integer',
  condition_status_source_value'category',
  condition_status_concept_id  'integer'

        model_schema[measurement
  measurement_id        'integer',
  person_id          'integer',
  measurement_concept_id    'integer',
  measurement_date      'object',
  measurement_datetime    'object'TIME2,
  measurement_time              'category'10,
  measurement_type_concept_id  'integer',
  operator_concept_id      'integer',
  value_as_number        'float',
  value_as_concept_id      'integer',
  unit_concept_id        'integer',
  range_low          'float',
  range_high          'float',
  provider_id          'integer',
  visit_occurrence_id      'integer',
  visit_detail_id               'integer'   ,
  measurement_source_value  'category',
  measurement_source_concept_id'integer',
  unit_source_value      'category',
  value_source_value      'category'

        model_schema[note
  note_id    'integer',
  person_id  'integer',
  note_date  'object'  ,
  note_datetime'object'TIME2,
  note_type_concept_id'integer',
  note_class_concept_id 'integer',
  note_title  'category'2,
  note_text  'object',
  encoding_concept_id'integer',
  language_concept_id'integer',
  provider_id  'integer',
  visit_occurrence_id'integer',
  visit_detail_id       'integer'     ,
  note_source_value'category'

        model_schema[note_nlp
  note_nlp_id        'integer',
  note_id          'integer',
  section_concept_id    'integer',
  snippet          'category'2,
  "offset"          'category'2,
  lexical_variant      'category'2,
  note_nlp_concept_id    'integer',
  note_nlp_source_concept_id  'integer',
  nlp_system        'category'2,
  nlp_date        'object'  ,
  nlp_datetime      'object'TIME2,
  term_exists        'category'1),
  term_temporal      'category',
  term_modifiers      'category'2000)

        model_schema[observation
  observation_id      'integer',
  person_id          'integer',
  observation_concept_id  'integer',
  observation_date      'object',
  observation_datetime  'object'TIME2,
  observation_type_concept_id  'integer',
  value_as_number        'float',
  value_as_string        'category'60),
  value_as_concept_id      'integer',
  qualifier_concept_id    'integer',
  unit_concept_id        'integer',
  provider_id          'integer',
  visit_occurrence_id      'integer',
  visit_detail_id               'integer'   ,
  observation_source_value  'category',
  observation_source_concept_id'integer',
  unit_source_value      'category',
  qualifier_source_value  'category'

        model_schema[fact_relationship
  domain_concept_id_1'integer',
  fact_id_1    'integer',
  domain_concept_id_2'integer',
  fact_id_2    'integer',
  relationship_concept_id'integer'

# Standardized health system data
        model_schema[location
  location_id  'integer',
  address_1  'category',
  address_2  'category',
  city    'category',
  state    'category'2),
  zip      'category'9),
  county  'category',
  location_source_value 'category'

        model_schema[care_site
  care_site_id      'integer',
  care_site_name    'category'255,
  place_of_service_concept_id  'integer',
  location_id        'integer',
  care_site_source_value  'category',
  place_of_service_source_value 'category'

        model_schema[provider
  provider_id        'integer',
  provider_name      'category',
  NPI            'category',
  DEA            'category',
  specialty_concept_id  'integer',
  care_site_id      'integer',
  year_of_birth      'integer',
  gender_concept_id    'integer',
  provider_source_value  'category',
  specialty_source_value'category',
  specialty_source_concept_id'integer',
  gender_source_value    'category',
  gender_source_concept_id'integer'  

# Standardized health economics

        model_schema[payer_plan_period
            'payer_plan_period_id': 'integer',
            'person_id': 'integer',
            'payer_plan_period_start_date':  'object',
            'payer_plan_period_end_date': 'object',
            'payer_concept_id': 'integer',
            'payer_source_value': 'category',
            'payer_source_concept_id': 'integer',
            'plan_concept_id: 'integer',
            'plan_source_value': 'category',
            'plan_source_concept_id': 'integer',
            'sponsor_concept_id': 'integer',
            'sponsor_source_value': 'category',
            'sponsor_source_concept_id': 'integer',
            'family_source_value': 'category',
            'stop_reason_concept_id': 'integer',
            'stop_reason_source_value': 'category',
            'stop_reason_source_concept_id': 'integer'       

        model_schema[cost
  cost_id          'integer'  ,
  cost_event_id             'integer'   ,
  cost_domain_id            'category'20,
  cost_type_concept_id      'integer'   ,
  currency_concept_id  'integer',
  total_charge  'float',
  total_cost    'float',
  total_paid    'float',
  paid_by_payer    'float',
  paid_by_patient'float',
  paid_patient_copay'float',
  paid_patient_coinsurance  'float',
  paid_patient_deductible'float',
  paid_by_primary'float',
  paid_ingredient_cost'float',
  paid_dispensing_fee'float',
  payer_plan_period_id'integer',
  amount_allowed        'float',
  revenue_code_concept_id'integer',
  reveue_code_source_value  'category'50,
  drg_concept_id      'integer',
  drg_source_value    'category'3)

# Standardized derived elements
        model_schema[cohort
            cohort_definition_id:'integer',
            subject_id:'integer',
            cohort_start_date:'object',
            cohort_end_date:'object'

        model_schema[cohort_attribute
            cohort_definition_id: 'integer',
            subject_id: 'integer',
            cohort_start_date: 'object',
            cohort_end_date: 'object',
            attribute_definition_id: 'integer',
            value_as_number: 'float',
            value_as_concept_id: 'integer'

        model_schema[drug_era
            drug_era_id: 'integer',
            person_id: 'integer',
            drug_concept_id: 'integer',
            drug_era_start_date: 'object',
            drug_era_end_date: 'object',
            drug_exposure_count: 'integer',
            gap_days: 'integer'

        model_schema[dose_era
            dose_era_id  'integer',
            person_id  'integer',
            drug_concept_id'integer',
            unit_concept_id'integer',
            dose_value'float',
            dose_era_start_date'object',
            dose_era_end_date    'object'  

        model_schema[condition_era
            condition_era_id    'integer',
            person_id        'integer',
            condition_concept_id  'integer',
            condition_era_start_date'object',
            condition_era_end_date'object',
            condition_occurrence_count'integer'
