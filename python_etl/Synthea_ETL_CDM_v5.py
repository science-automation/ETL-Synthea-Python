import csv,os,os.path,sys
from time import strftime
from collections import OrderedDict
import argparse
import dotenv
import math
from constants import OMOP_CONSTANTS, OMOP_MAPPING_RECORD, BENEFICIARY_SUMMARY_RECORD, OMOP_CONCEPT_RECORD, OMOP_CONCEPT_RELATIONSHIP_RECORD
from utility_classes import Table_ID_Values
from datetime import date
import calendar
# ------------------------
# This python script creates the OMOP CDM v5 tables from Synthea csv files.
# ------------------------
#
#  Input Required:
#       OMOP Vocabulary v5 Concept  file. Remember to run: java -jar cpt4.jar (appends CPT4 concepts from concept_cpt4.csv to CONCEPT.csv)
#           BASE_OMOP_INPUT_DIRECTORY    /  CONCEPT.csv
#                                        /  CONCEPT_RELATIONSHIP.csv
#
#  Output Produced:
#     Last-used concept_IDs for CDM v5 tables
#           BASE_OUTPUT_DIRECTORY       /  etl_synpuf_last_table_ids.txt
#                                       /  npi_provider_id.txt
#                                       /  provider_id_care_site.txt
#                                       /  location_dictionary.csv
#
#     OMOP CDM v5 Tables
#           BASE_OUTPUT_DIRECTORY       /  care_site_<sample_number>.csv
#                                       /  condition_occurrence_<sample_number>.csv
#                                       /  death_<sample_number>.csv
#                                       /  device_cost_<sample_number>.csv
#                                       /  device_exposure_<sample_number>.csv
#                                       /  drug_cost_<sample_number>.csv
#                                       /  drug_exposure_<sample_number>.csv
#                                       /  location_<sample_number>.csv
#                                       /  measurement_occurrence_<sample_number>.csv
#                                       /  observation_<sample_number>.csv
#                                       /  observation_period_<sample_number>.csv
#                                       /  payer_plan_period_<sample_number>.csv
#                                       /  person_<sample_number>.csv
#                                       /  procedure_cost_<sample_number>.csv
#                                       /  procedure_occurrence_<sample_number>.csv
#                                       /  provider_<sample_number>.csv
#                                       /  specimen_<sample_number>.csv
#                                       /  visit_cost_<sample_number>.csv
#                                       /  visit_occurrence_<sample_number>.csv
#
#
#                                       ** Various debug and log files
#
# ------------------------

dotenv.load_dotenv(".env")

# -----------------------------------
# - Configuration
# -----------------------------------

# Edit your .env file to change which directories to use in the ETL process
# Path to the directory where control files should be saved (input/output
BASE_ETL_CONTROL_DIRECTORY      = os.environ['BASE_ETL_CONTROL_DIRECTORY']
# Path to the directory containing the downloaded SynPUF files
BASE_SYNTHEA_INPUT_DIRECTORY     = os.environ['BASE_SYNTHEA_INPUT_DIRECTORY']
# Path to the directory containing the OMOP Vocabulary v5 files (can be downloaded from http://www.ohdsi.org/web/athena/)
BASE_OMOP_INPUT_DIRECTORY       = os.environ['BASE_OMOP_INPUT_DIRECTORY']
# Path to the directory where CDM-compatible CSV files should be saved
BASE_OUTPUT_DIRECTORY           = os.environ['BASE_OUTPUT_DIRECTORY']
SYNTHEA_DIR_FORMAT               = os.environ['SYNTHEA_DIR_FORMAT']

# -----------------------------------
# Globals
# -----------------------------------
source_code_concept_dict = {}   # stores source and target concept ids + destination file
concept_relationship_dict = {}  # stores the source concept id and its mapped target concept id
DESTINATION_FILE_DRUG               = 'drug'
DESTINATION_FILE_CONDITION          = 'condition'
DESTINATION_FILE_PROCEDURE          = 'procedure'
DESTINATION_FILE_OBSERVATION        = 'observation'
DESTINATION_FILE_MEASUREMENT        = 'measurement'
DESTINATION_FILE_DEVICE             = 'device'
DESTINATION_FILE_VISIT              = 'visit'
class SourceCodeConcept(object):
    def __init__(self, source_concept_code, source_concept_id, target_concept_id, destination_file):
        self.source_concept_code = source_concept_code
        self.source_concept_id = source_concept_id
        self.target_concept_id = target_concept_id
        self.destination_file = destination_file

domain_destination_file_list = {
    'Condition'             : DESTINATION_FILE_CONDITION,
    'Condition/Meas'        : DESTINATION_FILE_MEASUREMENT,
    'Condition/Obs'         : DESTINATION_FILE_OBSERVATION,
    'Condition/Procedure'   : DESTINATION_FILE_PROCEDURE,
    'Device'                : DESTINATION_FILE_DEVICE,
    'Device/Obs'            : DESTINATION_FILE_OBSERVATION,
    'Device/Procedure'      : DESTINATION_FILE_PROCEDURE,
    'Drug'                  : DESTINATION_FILE_DRUG,
    'Measurement'           : DESTINATION_FILE_MEASUREMENT,
    'Meas/Procedure'        : DESTINATION_FILE_PROCEDURE,
    'Obs/Procedure'         : DESTINATION_FILE_PROCEDURE,
    'Observation'           : DESTINATION_FILE_OBSERVATION,
    'Procedure'             : DESTINATION_FILE_PROCEDURE,
    'Visit'                 : DESTINATION_FILE_VISIT
    }

# -----------------------------------
# get timestamp
# -----------------------------------
def get_timestamp():
    return strftime("%Y-%m-%d %H:%M:%S")

# -----------------------------------
# format date in YYYYMMDD
# -----------------------------------
def get_date_YYYY_MM_DD(date_YYYYMMDD):
    if len(date_YYYYMMDD) == 0:
        return ''
    return '{0}-{1}-{2}'.format(date_YYYYMMDD[0:4], date_YYYYMMDD[4:6], date_YYYYMMDD[6:8])

# -----------------------------------
# This function produces dictionaries that give mappings between SynPUF codes and OMOP concept_ids
# -----------------------------------
def build_maps():
    print('build_maps starting...')

    #----------------
    # Load the OMOP v5 Concept file to build the source code to conceptID xref.
    # NOTE: This version of the flat file had embedded newlines. This code handles merging the split
    #       records. This may not be needed when the final OMOP v5 Concept file is produced.
    #----------------
    omop_concept_relationship_debug_file = os.path.join(BASE_OUTPUT_DIRECTORY,'concept_relationship_debug_log.txt')
    omop_concept_relationship_file = os.path.join(BASE_OMOP_INPUT_DIRECTORY,'CONCEPT_RELATIONSHIP.csv')
    omop_concept_debug_file = os.path.join(BASE_OUTPUT_DIRECTORY,'concept_debug_log.txt')
    omop_concept_file = os.path.join(BASE_OMOP_INPUT_DIRECTORY,'CONCEPT.csv')

    recs_in = 0
    recs_skipped = 0


    with open(omop_concept_relationship_file,'r') as fin, \
         open(omop_concept_relationship_debug_file, 'w') as fout_log:
        fin.readline() #skip header
        for rec in fin:
            recs_in += 1
            if recs_in % 100000 == 0: print 'omop concept relationship recs=',recs_in
            flds = (rec[:-1]).split('\t')
            if len(flds) == OMOP_CONCEPT_RELATIONSHIP_RECORD.fieldCount:
                concept_id1 = flds[OMOP_CONCEPT_RELATIONSHIP_RECORD.CONCEPT_ID_1]
                concept_id2 = flds[OMOP_CONCEPT_RELATIONSHIP_RECORD.CONCEPT_ID_2]
                relationship_id = flds[OMOP_CONCEPT_RELATIONSHIP_RECORD.RELATIONSHIP_ID]
                invalid_reason = flds[OMOP_CONCEPT_RELATIONSHIP_RECORD.INVALID_REASON]

                if concept_id1 != '' and concept_id2 != '' and relationship_id == "Maps to" and invalid_reason == '':
                    if concept_relationship_dict.has_key(concept_id1):         # one concept id might have several mapping, so values are stored as list
                        concept_relationship_dict[concept_id1].append(concept_id2)
                    else:
                        concept_relationship_dict[concept_id1] = [concept_id2]
                else:
                    recs_skipped = recs_skipped + 1

    recs_in = 0
    recs_skipped = 0
    merged_recs=0
    recs_checked=0

    print('Reading omop_concept_file        -> ' + omop_concept_file)
    print('Writing to log file              -> ' + omop_concept_debug_file)

    #First pass to obtain domain ids of concepts
    domain_dict = {}
    with open(omop_concept_file,'r') as fin:
        fin.readline()
        for rec in fin:
            flds = (rec[:-1]).split('\t')
            if len(flds) == OMOP_CONCEPT_RECORD.fieldCount:
                concept_id = flds[OMOP_CONCEPT_RECORD.CONCEPT_ID]
                domain_id = flds[OMOP_CONCEPT_RECORD.DOMAIN_ID]
                domain_dict[concept_id] = domain_id
    print "loaded domain dict with this many records: ", len(domain_dict)

    with open(omop_concept_file,'r') as fin, \
         open(omop_concept_debug_file, 'w') as fout_log:
        fin.readline() #skip header
        for rec in fin:
            recs_in += 1
            if recs_in % 100000 == 0: print 'omop concept recs=',recs_in
            flds = (rec[:-1]).split('\t')

            if len(flds) == OMOP_CONCEPT_RECORD.fieldCount:
                concept_id = flds[OMOP_CONCEPT_RECORD.CONCEPT_ID]
                concept_code = original_concept_code = flds[OMOP_CONCEPT_RECORD.CONCEPT_CODE].replace(".","")
                vocabulary_id = flds[OMOP_CONCEPT_RECORD.VOCABULARY_ID]
                if vocabulary_id ==  OMOP_CONSTANTS.CPT4_VOCABULARY_ID:
                    vocabulary_id = OMOP_CONSTANTS.HCPCS_VOCABULARY_ID
                if(vocabulary_id in [OMOP_CONSTANTS.ICD_9_DIAGNOSIS_VOCAB_ID,OMOP_CONSTANTS.ICD_9_PROCEDURES_VOCAB_ID]):
                    vocabulary_id = OMOP_CONSTANTS.ICD_9_VOCAB_ID

                domain_id = flds[OMOP_CONCEPT_RECORD.DOMAIN_ID]
                invalid_reason = flds[OMOP_CONCEPT_RECORD.INVALID_REASON]

                status = ''
                if concept_id != '':
                    if vocabulary_id in [OMOP_CONSTANTS.ICD_9_VOCAB_ID,
                                         OMOP_CONSTANTS.HCPCS_VOCABULARY_ID,
                                         OMOP_CONSTANTS.NDC_VOCABULARY_ID]:
                        recs_checked += 1

                        if  not concept_relationship_dict.has_key(concept_id):
                            destination_file = domain_destination_file_list[domain_id]
                            if( vocabulary_id == OMOP_CONSTANTS.ICD_9_VOCAB_ID):
                                status = "No map from ICD9 code, or code invalid for " + concept_id
                                recs_skipped += 1
                            if( vocabulary_id == OMOP_CONSTANTS.HCPCS_VOCABULARY_ID):
                                status = "No self map from OMOP (HCPCS/CPT4) to OMOP (HCPCS/CPT4) or code invalid for " + concept_id
                                recs_skipped += 1
                            if( vocabulary_id == OMOP_CONSTANTS.NDC_VOCABULARY_ID):
                                status = "No map from OMOP (NCD) to OMOP (RxNorm) or code invalid for " + concept_id
                                recs_skipped += 1
                            source_code_concept_dict[vocabulary_id,concept_code] = [SourceCodeConcept(concept_code, concept_id, "0", destination_file)]
                        else:
                            source_code_concept_dict[vocabulary_id,concept_code] = []
                            for concept in concept_relationship_dict[concept_id]:
                                destination_file = domain_destination_file_list[domain_dict[concept]]
                                source_code_concept_dict[vocabulary_id,concept_code].append(SourceCodeConcept(concept_code, concept_id, concept, destination_file))

                if status != '':
                    fout_log.write(status + ': \t')
                    # for fld in line: fout_log.write(fld + '\t')
                    fout_log.write(rec + '\n')


# -----------------------------------
# CDM v5 Person - Write person records
# -----------------------------------
def write_person_record(beneficiary):
    person_fd = file_control.get_Descriptor('person')
    yd = beneficiary.LatestYearData()
    if yd is None: return

    #write person records to the person file
    state_county = str(beneficiary.SP_STATE_CODE) + '-' + str(beneficiary.BENE_COUNTY_CD)
    current_location_id = get_location_id(state_county)     # get the location id for the given pair of state & county
    person_fd.write('{0},'.format(current_location_id))                              # location_id
    person_fd.write(',')                                                                    # provider_id
    person_fd.write(',')                                                                    # care_site_id
    person_fd.write('{0},'.format(beneficiary.DESYNTHEA_ID))                                # person_source_value
    person_fd.write('{0},'.format(yd.BENE_SEX_IDENT_CD))                                    # gender_source_value
    person_fd.write(',')                 					            # gender_source_concept_id
    person_fd.write('{0},'.format(yd.BENE_RACE_CD))                                         # race_source_value
    person_fd.write(',')                                                                    # race_source_concept_id
    person_fd.write('{0},'.format(yd.BENE_RACE_CD))                                         # ethnicity_source_value
    person_fd.write('\n')
    person_fd.increment_recs_written(1)

# -----------------------------------
# Death Record
# -----------------------------------
def write_death_records(death_fd, beneficiary, death_type_concept_id, cause_source_concept_id):
    yd = beneficiary.LatestYearData()
    if yd is not None and yd.BENE_DEATH_DT != '':       # if year data for BENE_DEATH_DT is not available, don't write to death file.
        death_fd.write('{0},'.format(beneficiary.person_id))
        death_fd.write('{0},'.format(get_date_YYYY_MM_DD(yd.BENE_DEATH_DT)))
        death_fd.write('{0},'.format(death_type_concept_id))
        death_fd.write(',')                                                    # cause_concept_id
        death_fd.write(',')                                                     # cause_source_value
        death_fd.write('{0}'.format(cause_source_concept_id))
        death_fd.write('\n')
        death_fd.increment_recs_written(1)

# -----------------------------------
# Drug Exposure
# -----------------------------------
def write_drug_exposure(drug_exp_fd, person_id, drug_concept_id, start_date, drug_type_concept_id,
                        quantity, days_supply, drug_source_concept_id, drug_source_value, provider_id, visit_occurrence_id):
        drug_exp_fd.write('{0},'.format(table_ids.last_drug_exposure_id))
        drug_exp_fd.write('{0},'.format(person_id))
        drug_exp_fd.write('{0},'.format(drug_concept_id))
        drug_exp_fd.write('{0},'.format(get_date_YYYY_MM_DD(start_date)))              # drug_exposure_start_date
        drug_exp_fd.write(',')                                                          # drug_exposure_end_date
        drug_exp_fd.write('{0},'.format(drug_type_concept_id))
        drug_exp_fd.write(',')                                                          # stop_reason
        drug_exp_fd.write(',')                                                          # refills
        if quantity is None:
            drug_exp_fd.write(',')
        else:
            drug_exp_fd.write('{0},'.format(float(quantity)))
        if days_supply is None:
            drug_exp_fd.write(',')
        else:
            drug_exp_fd.write('{0},'.format(days_supply))
        drug_exp_fd.write(',')                                                          # sig
        drug_exp_fd.write(',')                                                          # route_concept_id
        drug_exp_fd.write(',')                                                          # effective_drug_dose
        drug_exp_fd.write(',')                                                          # dose_unit_concept_ id
        drug_exp_fd.write(',')                                                          # lot_number
        drug_exp_fd.write('{0},'.format(provider_id))                                   # provider_id
        drug_exp_fd.write('{0},'.format(visit_occurrence_id))
        drug_exp_fd.write('{0},'.format(drug_source_value))
        drug_exp_fd.write('{0},'.format(drug_source_concept_id))
        drug_exp_fd.write(',')                                                          # route_source_value
        #drug_exp_fd.write('')                                                          # dose_unit_source_value
        drug_exp_fd.write('\n')
        drug_exp_fd.increment_recs_written(1)
        table_ids.last_drug_exposure_id += 1

# -----------------------------------
# Device Exposure
# -----------------------------------
def write_device_exposure(device_fd, person_id, device_concept_id, start_date, end_date, device_type_concept_id,
                          device_source_value, device_source_concept_id, provider_id, visit_occurrence_id):
    device_fd.write('{0},'.format(table_ids.last_device_exposure_id))
    device_fd.write('{0},'.format(person_id))
    device_fd.write('{0},'.format(device_concept_id))
    device_fd.write('{0},'.format(get_date_YYYY_MM_DD(start_date)))
    device_fd.write('{0},'.format(get_date_YYYY_MM_DD(end_date)))
    device_fd.write('{0},'.format(device_type_concept_id))
    device_fd.write(',')        # unique_device_id
    device_fd.write(',')        # quantity
    device_fd.write('{0},'.format(provider_id))        # provider_id
    device_fd.write('{0},'.format(visit_occurrence_id))
    device_fd.write('{0},'.format(device_source_value))
    device_fd.write('{0}'.format(device_source_concept_id))
    device_fd.write('\n')
    device_fd.increment_recs_written(1)
    table_ids.last_device_exposure_id += 1

# -----------------------------------
# Prescription Drug File -> Drug Exposure; Drug Cost
# -----------------------------------
def write_drug_records(beneficiary):
    drug_exp_fd = file_control.get_Descriptor('drug_exposure')
    drug_cost_fd = file_control.get_Descriptor('drug_cost')

    for raw_rec in beneficiary.prescription_records:
        rec = PrescriptionDrug(raw_rec)
        if rec.SRVC_DT == '':
            continue

        ndc_code = rec.PROD_SRVC_ID

        if (OMOP_CONSTANTS.NDC_VOCABULARY_ID,ndc_code) in source_code_concept_dict:
            #In practice we do not see multiple mappings of drugs, but in principle it could happen
            for sccd in source_code_concept_dict[OMOP_CONSTANTS.NDC_VOCABULARY_ID,ndc_code]:
                drug_source_concept_id = sccd.source_concept_id
                drug_concept_id = sccd.target_concept_id
                write_drug_exposure(drug_exp_fd, beneficiary.person_id,
                                    drug_concept_id=drug_concept_id,
                                    start_date=rec.SRVC_DT,
                                    drug_type_concept_id=OMOP_CONSTANTS.DRUG_TYPE_PRESCRIPTION,
                                    quantity=rec.QTY_DSPNSD_NUM,
                                    days_supply=rec.DAYS_SUPLY_NUM,
                                    drug_source_concept_id=drug_source_concept_id,
                                    drug_source_value=ndc_code,
                                    provider_id="",
                                    visit_occurrence_id="")
        else:
            #These are for any NDC codes not in CONCEPT.csv
            dline = 'DrugRecords--- ' + 'Unmapped NDC code: ' + str(ndc_code) + ' DESYNTHEA_ID: ' + rec.DESYNTHEA_ID + '\n'
            unmapped_log.write(dline)
            write_drug_exposure(drug_exp_fd, beneficiary.person_id,
                                drug_concept_id="0",
                                start_date=rec.SRVC_DT,
                                drug_type_concept_id=OMOP_CONSTANTS.DRUG_TYPE_PRESCRIPTION,
                                quantity=rec.QTY_DSPNSD_NUM,
                                days_supply=rec.DAYS_SUPLY_NUM,
                                drug_source_concept_id="0",
                                drug_source_value=ndc_code,
                                provider_id="",
                                visit_occurrence_id="")
        #----------------------
        # drug cost -- only written once, even if (doesn't happen now) NDC code maps to multiple RxNorm drugs
        #----------------------
        current_drug_exposure_id = table_ids.last_drug_exposure_id - 1  #subtracted 1 as drug_exposure function added 1 to last_drug_exposure_id
        drug_cost_fd.write('{0},'.format(table_ids.last_drug_cost_id))
        drug_cost_fd.write('{0},'.format(current_drug_exposure_id))
        drug_cost_fd.write('{0},'.format(OMOP_CONSTANTS.CURRENCY_US_DOLLAR))
        drug_cost_fd.write(',')                                          # paid_copay
        drug_cost_fd.write('{0},'.format(rec.PTNT_PAY_AMT))              # paid_coinsurance
        drug_cost_fd.write(',')                                          # paid_toward_deductible
        drug_cost_fd.write(',')                                          # paid_by_payer
        drug_cost_fd.write(',')                                          # paid_by_coordination_of_benefits
        drug_cost_fd.write('{0},'.format(rec.PTNT_PAY_AMT))              # total_out_of_pocket      #
        drug_cost_fd.write('{0},'.format(rec.TOT_RX_CST_AMT))            # total_paid  #
        drug_cost_fd.write(',')                                          # ingredient_cost
        drug_cost_fd.write(',')                                          # dispensing_fee
        drug_cost_fd.write(',')                                          # average_wholesale_price
        #drug_cost_fd.write('')                                          # payer_plan_period_id                  ##### At moment we do not have payer_plan_period implemented, as we have no payer plan information.
        drug_cost_fd.write('\n')
        drug_cost_fd.increment_recs_written(1)
        table_ids.last_drug_cost_id += 1

# -----------------------------------
# Provider file
# -----------------------------------
def write_provider_record(provider_fd, npi, provider_id, care_site_id, provider_source_value):
    if not provider_id:
        return
    idx = npi_provider_id[npi][1]
    if idx == 0:
        provider_fd.write('{0},'.format(provider_id))
        provider_fd.write(',')                                                            # provider_name
        provider_fd.write('{0},'.format(npi))
        provider_fd.write(',')                                                            # dea
        provider_fd.write(',')
        provider_fd.write('{0},'.format(care_site_id))
        provider_fd.write(',')                                                            # year_of_birth
        provider_fd.write(',')                                                            # gender_concept_id
        provider_fd.write('{0},'.format(provider_source_value))                           # provider_source_value
        provider_fd.write(',')                                                            # specialty_source_value
        provider_fd.write(',')                                                            # specialty_source_concept_id
        provider_fd.write(',')                                                            # gender_source_value
        #provider_fd.write('')                                                            # gender_source_concept_id
        provider_fd.write('\n')
        provider_fd.increment_recs_written(1)
        npi_provider_id[npi] = [npi_provider_id[npi][0],1]  #set index to 1 to mark provider_id written


# -----------------------------------
# Condition Occurence file
#  - Added provider_id
# -----------------------------------
def write_condition_occurrence(cond_occur_fd, person_id, condition_concept_id,
                              from_date, thru_date, condition_type_concept_id, provider_id,
                              condition_source_value, condition_source_concept_id, visit_occurrence_id):
    cond_occur_fd.write('{0},'.format(table_ids.last_condition_occurrence_id))
    cond_occur_fd.write('{0},'.format(person_id))
    cond_occur_fd.write('{0},'.format(condition_concept_id))
    cond_occur_fd.write('{0},'.format(get_date_YYYY_MM_DD(from_date)))
    cond_occur_fd.write('{0},'.format(get_date_YYYY_MM_DD(thru_date)))
    cond_occur_fd.write('{0},'.format(condition_type_concept_id))
    cond_occur_fd.write(',')                                          # stop_reason
    cond_occur_fd.write('{0},'.format(provider_id))                   # provider_id
    cond_occur_fd.write('{0},'.format(visit_occurrence_id))
    cond_occur_fd.write('{0},'.format(condition_source_value))
    cond_occur_fd.write('{0}'.format(condition_source_concept_id))
    cond_occur_fd.write('\n')
    cond_occur_fd.increment_recs_written(1)
    table_ids.last_condition_occurrence_id += 1

# -----------------------------------
#  - Added this new function to
# create Visit Occurence file
# -----------------------------------
def write_visit_occurrence(visit_occur_fd, person_id, visit_concept_id, visit_occurrence_id, care_site_id, visit_source_concept_id,
                              from_date, thru_date, visit_type_concept_id, provider_id, visit_source_value):
    visit_occur_fd.write('{0},'.format(visit_occurrence_id))
    visit_occur_fd.write('{0},'.format(person_id))
    visit_occur_fd.write('{0},'.format(visit_concept_id))
    visit_occur_fd.write('{0},'.format(get_date_YYYY_MM_DD(from_date)))
    visit_occur_fd.write(',')                                          # visit_start_time
    visit_occur_fd.write('{0},'.format(get_date_YYYY_MM_DD(thru_date)))
    visit_occur_fd.write(',')                                          # visit_end_time
    visit_occur_fd.write('{0},'.format(visit_type_concept_id))
    visit_occur_fd.write('{0},'.format(provider_id))                   # provider_id
    visit_occur_fd.write('{0},'.format(care_site_id))                  # care_site_id
    visit_occur_fd.write('{0},'.format(visit_source_value))
    visit_occur_fd.write('\n')
    visit_occur_fd.increment_recs_written(1)

# -----------------------------------
# Procedure Occurence file
# -----------------------------------
def write_procedure_occurrence(proc_occur_fd, person_id, procedure_concept_id,
                              from_date, procedure_type_concept_id,provider_id,modifier_concept_id,
                              procedure_source_value, procedure_source_concept_id, visit_occurrence_id):
    proc_occur_fd.write('{0},'.format(table_ids.last_procedure_occurrence_id))
    proc_occur_fd.write('{0},'.format(person_id))
    proc_occur_fd.write('{0},'.format(procedure_concept_id))
    proc_occur_fd.write('{0},'.format(get_date_YYYY_MM_DD(from_date))) # procedure_date
    proc_occur_fd.write('{0},'.format(procedure_type_concept_id))
    proc_occur_fd.write(',')                                           # modifier_concept_id
    proc_occur_fd.write(',')                                           # quantity
    proc_occur_fd.write('{0},'.format(provider_id))                    # provider_id
    proc_occur_fd.write('{0},'.format(visit_occurrence_id))
    proc_occur_fd.write('{0},'.format(procedure_source_value))
    proc_occur_fd.write('{0},'.format(procedure_source_concept_id))
    proc_occur_fd.write('\n')
    proc_occur_fd.increment_recs_written(1)
    table_ids.last_procedure_occurrence_id += 1

# -----------------------------------
# Measurement file
# -----------------------------------
def write_measurement(measurement_fd, person_id, measurement_concept_id,
                      measurement_date, measurement_type_concept_id,
                      measurement_source_value, measurement_source_concept_id, provider_id, visit_occurrence_id):
    measurement_fd.write('{0},'.format(table_ids.last_measurement_id))
    measurement_fd.write('{0},'.format(person_id))
    measurement_fd.write('{0},'.format(measurement_concept_id))
    measurement_fd.write('{0},'.format(get_date_YYYY_MM_DD(measurement_date)))
    measurement_fd.write(',')        # measurement_time
    measurement_fd.write('{0},'.format(measurement_type_concept_id))
    measurement_fd.write(',')        # operator_concept_id
    measurement_fd.write(',')        # value_as_number
    measurement_fd.write('0,')       # value_as_concept_id
    measurement_fd.write(',')        # unit_concept_id
    measurement_fd.write(',')        # range_low
    measurement_fd.write(',')        # range_high
    measurement_fd.write('{0},'.format(provider_id))        # provider_id
    measurement_fd.write('{0},'.format(visit_occurrence_id))
    measurement_fd.write('{0},'.format(measurement_source_value))
    measurement_fd.write('{0},'.format(measurement_source_concept_id))
    measurement_fd.write(',')        # unit_source_value
    measurement_fd.write('\n')
    measurement_fd.increment_recs_written(1)
    table_ids.last_measurement_id += 1

# -----------------------------------
# Observation file
# -----------------------------------
def write_observation(observation_fd, person_id, observation_concept_id,provider_id,
                      observation_date, observation_type_concept_id,
                      observation_source_value, observation_source_concept_id, visit_occurrence_id):
    observation_fd.write('{0},'.format(table_ids.last_observation_id))
    observation_fd.write('{0},'.format(person_id))
    observation_fd.write('{0},'.format(observation_concept_id))
    observation_fd.write('{0},'.format(get_date_YYYY_MM_DD(observation_date)))
    observation_fd.write(',')        # observation_time
    observation_fd.write('{0},'.format(observation_type_concept_id))
    observation_fd.write(',')        # value_as_number
    observation_fd.write(',')        # value_as_string
    observation_fd.write('0,')       # value_as_concept_id
    observation_fd.write(',')        # qualifier_concept_id
    observation_fd.write(',')        # unit_concept_id
    observation_fd.write('{0},'.format(provider_id))   # provider_id
    observation_fd.write('{0},'.format(visit_occurrence_id))
    observation_fd.write('{0},'.format(observation_source_value))
    observation_fd.write('{0},'.format(observation_source_concept_id))
    observation_fd.write(',')        # unit_source_value
    observation_fd.write('\n')
    observation_fd.increment_recs_written(1)
    table_ids.last_observation_id += 1


# -----------------------------------
# Write to Care Site file
# -----------------------------------
def write_care_site(care_site_fd, care_site_id, place_of_service_concept_id, care_site_source_value, place_of_service_source_value):
    if not care_site_id:
        return
    idx = provider_id_care_site_id[care_site_source_value][1]
    if idx == 0:
        care_site_fd.write('{0},'.format(care_site_id))
        care_site_fd.write(',')                                      # care_site_name
        care_site_fd.write('{0},'.format(place_of_service_concept_id))
        care_site_fd.write(',')                                      # location_id
        care_site_fd.write('{0},'.format(care_site_source_value))
        care_site_fd.write('{0}'.format(place_of_service_source_value))
        care_site_fd.write('\n')
        care_site_fd.increment_recs_written(1)
        provider_id_care_site_id[care_site_source_value] = [provider_id_care_site_id[care_site_source_value][0],1]   # change index to 1 to mark it written


#---------------------------------
# Write header records
#---------------------------------
def write_header_records():
    headers = {
        'person' :
            'person_id,gender_concept_id,year_of_birth,month_of_birth,day_of_birth,time_of_birth,race_concept_id,ethnicity_concept_id,'
            'location_id,provider_id,care_site_id,person_source_value,gender_source_value,gender_source_concept_id,race_source_value,'
            'race_source_concept_id,ethnicity_source_value,ethnicity_source_concept_id',

        'observation':
            'observation_id,person_id,observation_concept_id,observation_date,observation_time,observation_type_concept_id,value_as_number,'
            'value_as_string,value_as_concept_id,qualifier_concept_id,unit_concept_id,provider_id,visit_occurrence_id,observation_source_value,'
            'observation_source_concept_id,unit_source_value,qualifier_source_value',

        'observation_period':
            'observation_period_id,person_id,observation_period_start_date,observation_period_end_date,period_type_concept_id',

        'specimen':
            'specimen_id,person_id,specimen_concept_id,specimen_type_concept_id,specimen_date,specimen_time,quantity,'
            'unit_concept_id,anatomic_site_concept_id,disease_status_concept_id,specimen_source_id,specimen_source_value,unit_source_value,'
            'anatomic_site_source_value,disease_status_source_value',

        'death':
            'person_id,death_date,death_type_concept_id,cause_concept_id,cause_source_value,cause_source_concept_id',

        'visit_occurrence':
            'visit_occurrence_id,person_id,visit_concept_id,visit_start_date,visit_start_time,visit_end_date,visit_end_time,'
            'visit_type_concept_id,provider_id,care_site_id,visit_source_value,visit_source_concept_id',

        'visit_cost':
            'visit_cost_id,visit_occurrence_id,currency_concept_id,paid_copay,paid_coinsurance,paid_toward_deductible,'
            'paid_by_payer,paid_by_coordination_benefits,total_out_of_pocket,total_paid,payer_plan_period_id',

        'condition_occurrence':
            'condition_occurrence_id,person_id,condition_concept_id,condition_start_date,condition_end_date,condition_type_concept_id,'
            'stop_reason,provider_id,visit_occurrence_id,condition_source_value,condition_source_concept_id',

        'procedure_occurrence':
            'procedure_occurrence_id,person_id,procedure_concept_id,procedure_date,procedure_type_concept_id,modifier_concept_id,'
            'quantity,provider_id,visit_occurrence_id,procedure_source_value,procedure_source_concept_id,qualifier_source_value',

        'procedure_cost':
            'procedure_cost_id,procedure_occurrence_id,currency_concept_id,paid_copay,paid_coinsurance,paid_toward_deductible,'
            'paid_by_payer,paid_by_coordination_benefits,total_out_of_pocket,total_paid,revenue_code_concept_id,payer_plan_period_id,revenue_code_source_value',

        'drug_exposure':
            'drug_exposure_id,person_id,drug_concept_id,drug_exposure_start_date,drug_exposure_end_date,drug_type_concept_id,'
            'stop_reason,refills,quantity,days_supply,sig,route_concept_id,effective_drug_dose,dose_unit_concept_id,'
            'lot_number,provider_id,visit_occurrence_id,drug_source_value,drug_source_concept_id,route_source_value,dose_unit_source_value',

        'drug_cost':
            'drug_cost_id,drug_exposure_id,currency_concept_id,paid_copay,paid_coinsurance,paid_toward_deductible,paid_by_payer,paid_by_coordination_of_benefits,'
            'total_out_of_pocket,total_paid,ingredient_cost,dispensing_fee,average_wholesale_price,payer_plan_period_id',

        'device_exposure':
            'device_exposure_id,person_id,device_concept_id,device_exposure_start_date,device_exposure_end_date,device_type_concept_id,'
            'unique_device_id,quantity,provider_id,visit_occurrence_id,device_source_value,device_source_concept_id',

        'device_cost':
            'device_cost_id,device_exposure_id,currency_concept_id,paid_copay,paid_coinsurance,paid_toward_deductible,'
            'paid_by_payer,paid_by_coordination_benefits,total_out_of_pocket,total_paid,payer_plan_period_id',

        'measurement_occurrence':
            'measurement_id,person_id,measurement_concept_id,measurement_date,measurement_time,measurement_type_concept_id,operator_concept_id,'
            'value_as_number,value_as_concept_id,unit_concept_id,range_low,range_high,provider_id,visit_occurrence_id,measurement_source_value,'
            'measurement_source_concept_id,unit_source_value,value_source_value',

        'location':
            'location_id,address_1,address_2,city,state,zip,county,location_source_value',

        'care_site':
            'care_site_id,care_site_name,place_of_service_concept_id,location_id,care_site_source_value,place_of_service_source_value',

        'provider':
            'provider_id,provider_name,NPI,DEA,specialty_concept_id,care_site_id,year_of_birth,gender_concept_id,provider_source_value,'
            'specialty_source_value,specialty_source_concept_id,gender_source_value,gender_source_concept_id',

        'payer_plan_period':
            'payer_plan_period_id,person_id,payer_plan_period_start_date,payer_plan_period_end_date,payer_source_value,'
            'plan_source_value,family_source_value',
    }

    #for token in sorted(file_control.descriptor_list(which='output')):
    #    fd = file_control.get_Descriptor(token)
    #    fd.write(headers[token] + '\n')
    #    fd.increment_recs_written(1)


#---------------------------------
# start of the program
#---------------------------------
if __name__ == '__main__':
    if not os.path.exists(BASE_OUTPUT_DIRECTORY): os.makedirs(BASE_OUTPUT_DIRECTORY)
    if not os.path.exists(BASE_ETL_CONTROL_DIRECTORY): os.makedirs(BASE_ETL_CONTROL_DIRECTORY)

    current_stats_filename = os.path.join(BASE_OUTPUT_DIRECTORY,'etl_stats.txt')
    print('SYNTHEA_ETL starting')
    print('BASE_SYNTHEA_INPUT_DIRECTORY     =' + BASE_SYNTHEA_INPUT_DIRECTORY)
    print('BASE_OUTPUT_DIRECTORY           =' + BASE_OUTPUT_DIRECTORY)
    print('BASE_ETL_CONTROL_DIRECTORY      =' + BASE_ETL_CONTROL_DIRECTORY)

    # check files look ok

    print '-'*80
    print '-- all files present....'
    print '-'*80

    # Build the object to manage access to all the files
    write_header_records()

    # load the vocab flies
    #build_maps()
   
    # we only need to consider one synthea input file at a time to make the mapping
    synthea_file_list =  ['conditions.csv','careplans.csv','observations.csv','procedures.csv','immunizations.csv','imaging_studies.csv','imaging_studies.csv','encounters.csv','organizations.csv','providers.csv','providers.csv','payer_transitions.csv','allergies.csv','patients.csv','medications.csv']
    synthea_file_list = ['patients.csv']
    for inputfile in synthea_file_list:
         recs_in = 0
         rec=''
         inputdata = os.path.join(BASE_SYNTHEA_INPUT_DIRECTORY,inputfile)
         print('-'*80)
         print('reading file -> '+ inputfile)

         try:
            with open(inputdata) as fin:
                # Skip header record
                rec = fin.readline()

                for rec in fin:
                    recs_in += 1
                    if recs_in % 10000 == 0: print inputfile + ': ', recs_in
                    # read each record of the file
                    print(rec)

         except BaseException:
            print '** ERROR reading file, record number ', recs_in, '\n record-> ', rec
            raise

    #- save look up tables & last-used-ids
#    persist_lookup_tables()
#    table_ids.Save(table_ids_filename)

    print '** done **'
