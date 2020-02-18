import os
import configparser
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *

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
    # Pre-defining the schema will have better performance then use the cvs importer
    # to try to infer the schema which causes an extra scan of the data.
    # The date fields are read in as string.  They can be converted to date
    # objects if necessary
    # OMOP V5.3.1
    #
    def omopSchema(self):
        model_schema = {}
        model_schema['care_site'] = StructType([ \
            StructField("CARE_SITE_ID", IntegerType(), False), \
            StructField("CARE_SITE_NAME", StringType(), True), \
            StructField("LOCATION_ID", IntegerType(), True), \
            StructField("PLACE_OF_SERVICE_CONCEPT_ID", IntegerType(), True), \
            StructField("CARE_SITE_SOURCE_VALUE", StringType(), True), \
            StructField("PLACE_OF_SERVICE_SOURCE_VALUE", StringType(), True)])

        model_schema['cohort'] = StructType([ \
            StructField("COHORT_DEFINITION_ID", IntegerType(), False), \
            StructField("COHORT_START_DATE", StringType(), False), \
            StructField("COHORT_END_DATE", StringType(), True), \
            StructField("SUBJECT_ID", IntegerType(), False])

        model_schema['condition_era'] = StructType([ \
            StructField("CONDITION_ERA_ID", IntegerType(), False), \
            StructField("PERSON_ID", IntegerType(), False), \
            StructField("CONDITION_CONCEPT_ID", IntegerType(), False), \
            StructField("CONDITION_ERA_START_DATE", StringType(), False), \
            StructField("CONDITION_ERA_END_DATE", StringType(), True), \
            StructField("CONDITION_OCCURRENCE_COUNT", IntegerType(), True)])

        model_schema['condition_occurrence'] = StructType([ \
            StructField("CONDITION_OCCURRENCE_ID", IntegerType(), False), \
            StructField("PERSON_ID", IntegerType(), False), \
            StructField("CONDITION_CONCEPT_ID", IntegerType(), False), \
            StructField("CONDITION_START_DATE", StringType(), False), \
            StructField("CONDITION_END_DATE", StringType(), True), \
            StructField("CONDITION_TYPE_CONCEPT_ID", IntegerType(), False), \
            StructField("STOP_REASON", StringType(), True), \
            StructField("PROVIDER_ID", IntegerType(), True), \
            StructField("VISIT_OCCURRENCE_ID", IntegerType(), True), \
            StructField("VISIT_DETAIL_ID", IntegerType(), True), \
            StructField("CONDITION_SOURCE_VALUE", StringType(), True), \
            StructField("CONDITION_SOURCE_CONCEPT_ID", IntegerType(), True)), \
            StructField("CONDITION_STATUS_SOURCE_VALUE", StringType(), True), \
            StructField("CONDITION_STATUS_CONCEPT_ID", IntegerType(), True)])

        model_schema['death'] = StructType([ \
            StructField("PERSON_ID", IntegerType(), False), \
            StructField("DEATH_DATE", StringType(), False), \
            StructField("DEATH_TYPE_CONCEPT_ID", IntegerType(), False), \
            StructField("CAUSE_CONCEPT_ID", IntegerType(), True), \
            StructField("CAUSE_SOURCE_VALUE", StringType(), True), \
            StructField("CAUSE_SOURCE_CONCEPT_ID", IntegerType(), True)])

        model_schema['device_exposure'] = StructType([ \
            StructField("DEVICE_EXPOSURE_ID", IntegerType(), False), \
            StructField("PERSON_ID", IntegerType(), False), \
            StructField("DEVICE_CONCEPT_ID", IntegerType(), False), \
            StructField("DEVICE_EXPOSURE_START_DATE", StringType(), False), \
            StructField("DEVICE_EXPOSURE_END_DATE", StringType(), True), \
            StructField("DEVICE_TYPE_CONCEPT_ID", IntegerType(), True), \
            StructField("UNIQUE_DEVICE_ID", IntegerType(), True), \
            StructField("QUANTITY", IntegerType(), True), \
            StructField("PROVIDER_ID", IntegerType(), True), \
            StructField("VISIT_OCCURRENCE_ID", IntegerType(), True), \
            StructField("DEVICE_SOURCE_VALUE", StringType(), True), \
            StructField("DEVICE_SOURCE_CONCEPT_ID", IntegerType(), True)])

        model_schema['drug_cost'] = StructType([ \
            StructField("DRUG_COST_ID", IntegerType(), False), \
            StructField("DRUG_EXPOSURE_ID", IntegerType(), False), \
            StructField("PAID_COPAY", FloatType(), True), \
            StructField("PAID_COINSURANCE", FloatType(), True), \
            StructField("PAID_TOWARD_DEDUCTIBLE", FloatType(), True), \
            StructField("PAID_BY_PAYER", FloatType(), True), \
            StructField("PAID_BY_COORDINATION_BENEFITS", FloatType(), True), \
            StructField("TOTAL_OUT_OF_POCKET", FloatType(), True), \
            StructField("TOTAL_PAID", FloatType(), True), \
            StructField("INGREDIENT_COST", FloatType(), True), \
            StructField("DISPENSING_FEE", FloatType(), True), \
            StructField("AVERAGE_WHOLESALE_PRICE", FloatType(), True), \
            StructField("PAYER_PLAN_PERIOD_ID", IntegerType(), True)])

        model_schema['drug_era'] = StructType([ \
            StructField("DRUG_ERA_ID", IntegerType(), False), \
            StructField("PERSON_ID", IntegerType(), False), \
            StructField("DRUG_CONCEPT_ID", IntegerType(), False), \
            StructField("DRUG_ERA_START_DATE", StringType(), False), \
            StructField("DRUG_ERA_END_DATE", StringType(), True), \
            StructField("DRUG_TYPE_CONCEPT_ID", IntegerType(), False), \
            StructField("DRUG_EXPOSURE_COUNT", IntegerType(), True)])

        model_schema['drug_exposure'] = StructType([ \
            StructField("DRUG_EXPOSURE_ID", IntegerType(), False), \
            StructField("PERSON_ID", IntegerType(), False), \
            StructField("DRUG_CONCEPT_ID", IntegerType(), False), \
            StructField("DRUG_EXPOSURE_START_DATE", StringType(), False), \
            StructField("DRUG_EXPOSURE_END_DATE", StringType(), True), \
            StructField("DRUG_TYPE_CONCEPT_ID", IntegerType(), False), \
            StructField("STOP_REASON", StringType(), True), \
            StructField("REFILLS", IntegerType(), True), \
            StructField("QUANTITY", FloatType(), True), \
            StructField("DAYS_SUPPLY", IntegerType(), True), \
            StructField("SIG", StringType(), True), \
            StructField("ROUTE_CONCEPT_ID", IntegerType(), True), \
            StructField("EFFECTIVE_DRUG_DOSE", FloatType(), True), \
            StructField("DOSE_UNIT_CONCEPT_ID", IntegerType(), True), \
            StructField("LOT_NUMBER", StringType(), True), \
            StructField("PROVIDER_ID", IntegerType(), True), \
            StructField("VISIT_OCCURRENCE_ID", IntegerType(), True), \
            StructField("DRUG_SOURCE_VALUE", StringType(), True), \
            StructField("DRUG_SOURCE_CONCEPT_ID", IntegerType(), True), \
            StructField("ROUTE_SOURCE_VALUE", StringType(), True), \
            StructField("DOSE_UNIT_SOURCE_VALUE", StringType(), True)])

        model_schema['location'] = StructType([ \
            StructField("LOCATION_ID", IntegerType(), False), \
            StructField("ADDRESS_1", StringType(), True), \
            StructField("ADDRESS_2", StringType(), True), \
            StructField("CITY", StringType(), True), \
            StructField("STATE", StringType(), True), \
            StructField("ZIP", StringType(), True), \
            StructField("COUNTY", StringType(), True), \
            StructField("LOCATION_SOURCE_VALUE", StringType(), True)])

        model_schema['measurement'] = StructType([ \
            StructField("MEASUREMENT_ID", IntegerType(), False), \
            StructField("PERSON_ID", IntegerType(), False), \
            StructField("MEASUREMENT_CONCEPT_ID", IntegerType(), False), \
            StructField("MEASUREMENT_DATE", StringType(), False), \
            StructField("MEASUREMENT_TIME", StringType(), True), \
            StructField("MEASUREMENT_TYPE_CONCEPT_ID", IntegerType(), False), \
            StructField("OPERATOR_CONCEPT_ID", IntegerType(), True), \
            StructField("VALUE_AS_NUMBER", FloatType(), True), \
            StructField("VALUE_AS_CONCEPT_ID", IntegerType(), True), \
            StructField("UNIT_CONCEPT_ID", IntegerType(), True), \
            StructField("RANGE_LOW", FloatType(), True), \
            StructField("RANGE_HIGH", FloatType(), True), \
            StructField("PROVIDER_ID", IntegerType(), True), \
            StructField("VISIT_OCCURRENCE_ID", IntegerType(), True), \
            StructField("MEASUREMENT_SOURCE_VALUE", StringType(), True), \
            StructField("MEASUREMENT_SOURCE_CONCEPT_ID", IntegerType(), True), \
            StructField("UNIT_SOURCE_VALUE", StringType(), True), \
            StructField("VALUE_SOURCE_VALUE", StringType(), True)])

        model_schema['observation'] = StructType([ \
            StructField("OBSERVATION_ID", IntegerType(), False), \
            StructField("PERSON_ID", IntegerType(), False), \
            StructField("OBSERVATION_CONCEPT_ID", IntegerType(), False), \
            StructField("OBSERVATION_DATE", StringType(), False), \
            StructField("OBSERVATION_TIME", StringType(), True), \
            StructField("OBSERVATION_TYPE_CONCEPT_ID", IntegerType(), False), \
            StructField("VALUE_AS_NUMBER", FloatType(), True), \
            StructField("VALUE_AS_STRING", StringType(), True), \
            StructField("VALUE_AS_CONCEPT_ID", IntegerType(), True), \
            StructField("QUALIFIER_CONCEPT_ID", IntegerType(), True), \
            StructField("UNIT_CONCEPT_ID", IntegerType(), True), \
            StructField("PROVIDER_ID", IntegerType(), True), \
            StructField("VISIT_OCCURRENCE_ID", IntegerType(), True), \
            StructField("OBSERVATION_SOURCE_VALUE", StringType(), True), \
            StructField("OBSERVATION_SOURCE_CONCEPT_ID", IntegerType(), True), \
            StructField("UNIT_SOURCE_VALUE", StringType(), True), \
            StructField("QUALIFIER_SOURCE_VALUE", StringType(), True)])

        model_schema['observation_period'] = StructType([ \
            StructField("OBSERVATION_PERIOD_ID", IntegerType(), False), \
            StructField("PERSON_ID", IntegerType(), False), \
            StructField("OBSERVATION_PERIOD_START_DATE", StringType(), False), \
            StructField("OBSERVATION_PERIOD_END_DATE", StringType(), False)])

        model_schema['observation_period'] = StructType([ \
            StructField("ORGANIZATION_ID", IntegerType(), False), \
            StructField("PLACE_OF_SERVICE_CONCEPT_ID", IntegerType(), True), \
            StructField("LOCATION_ID", IntegerType(), True), \
            StructField("ORGANIZATION_SOURCE_VALUE", StringType(), True), \
            StructField("PLACE_OF_SERVICE_SOURCE_VALUE", StringType(), True)])

        model_schema['payer_plan_period'] = StructType([ \
            StructField("PAYER_PLAN_PERIOD_ID", IntegerType(), False), \
            StructField("PERSON_ID", IntegerType(), False), \
            StructField("PAYER_PLAN_PERIOD_START_DATE", StringType(), False), \
            StructField("PAYER_PLAN_PERIOD_END_DATE", StringType(), False), \
            StructField("PAYER_SOURCE_VALUE", StringType(), True), \
            StructField("PLAN_SOURCE_VALUE", StringType(), True), \
            StructField("FAMILY_SOURCE_VALUE", StringType(), True)])

        model_schema['person'] = StructType([ \
            StructField("PERSON_ID", IntegerType(), False), \
            StructField("GENDER_CONCEPT_ID", IntegerType(), False), \
            StructField("YEAR_OF_BIRTH", IntegerType(), False), \
            StructField("MONTH_OF_BIRTH", IntegerType(), True), \
            StructField("DAY_OF_BIRTH", IntegerType(), True), \
            StructField("TIME_OF_BIRTH", StringType(), True), \
            StructField("RACE_CONCEPT_ID", IntegerType(), True), \
            StructField("ETHNICITY_CONCEPT_ID", IntegerType(), True), \
            StructField("LOCATION_ID", IntegerType(), True), \
            StructField("PROVIDER_ID", IntegerType(), True), \
            StructField("CARE_SITE_ID", IntegerType(), True), \
            StructField("PERSON_SOURCE_VALUE", StringType(), True), \
            StructField("GENDER_SOURCE_VALUE", StringType(), True), \
            StructField("GENDER_SOURCE_CONCEPT_ID", IntegerType(), True), \
            StructField("RACE_SOURCE_VALUE", StringType(), True), \
            StructField("RACE_SOURCE_CONCEPT_ID", IntegerType(), True), \
            StructField("ETHNICITY_SOURCE_VALUE", StringType(), True), \
            StructField("ETHNICITY_SOURCE_CONCEPT_ID", IntegerType(), True)])

        model_schema['procedure_cost'] = StructType([ \
            StructField("PROCEDURE_COST_ID", IntegerType(), False), \
            StructField("PROCEDURE_OCCURRENCE", IntegerType(), False), \
            StructField("PAID_COPAY", FloatType(), True), \
            StructField("PAID_COINSURANCE", FloatType(), True), \
            StructField("PAID_TOWARD_DEDUCTIBLE", FloatType(), True), \
            StructField("PAID_BY_PAYER", FloatType(), True), \
            StructField("PAID_BY_COORDINATION_BENEFITS", FloatType(), True), \
            StructField("TOTAL_OUT_OF_POCKET", FloatType(), True), \
            StructField("TOTAL_PAID", FloatType(), True), \
            StructField("DISEASE_CLASS_CONCEPT_ID", IntegerType(), True), \
            StructField("REVENUE_CODE_CONCEPT_ID", IntegerType(), True), \
            StructField("PAYER_PLAN_PERIOD_ID", IntegerType(), True), \
            StructField("DISEASE_CLASS_SOURCE_VALUE", StringType(), True), \
            StructField("REVENUE_CODE_SOURCE_VALUE", StringType(), True)])

        model_schema['procedure_occurrence'] = StructType([ \
            StructField("PROCEDURE_OCCURRENCE_ID", IntegerType(), False), \
            StructField("PERSON_ID", IntegerType(), False), \
            StructField("PROCEDURE_CONCEPT_ID", IntegerType(), False), \
            StructField("PROCEDURE_DATE", StringType(), False), \
            StructField("PROCEDURE_TYPE_CONCEPT_ID", IntegerType(), False), \
            StructField("MODIFIER_CONCEPT_ID", IntegerType(), True), \
            StructField("QUANTITY", IntegerType(), False), \
            StructField("PROVIDER_ID", IntegerType(), True), \
            StructField("VISIT_OCCURRENCE_ID", IntegerType(), True), \
            StructField("PROCEDURE_SOURCE_VALUE", StringType(), True), \
            StructField("PROCEDURE_SOURCE_CONCEPT_ID", IntegerType(), True), \
            StructField("QUALIFIER_SOURCE_VALUE", StringType(), True)])

        model_schema['provider'] = StructType([ \
            StructField("PROVIDER_ID", IntegerType(), False), \
            StructField("PROVIDER_NAME", StringType(), True), \
            StructField("NPI", StringType(), True), \
            StructField("DEA", StringType(), True), \
            StructField("SPECIALTY_CONCEPT_ID", IntegerType(), True), \
            StructField("CARE_SITE_ID", IntegerType(), True), \
            StructField("YEAR_OF_BIRTH", IntegerType(), True), \
            StructField("GENDER_CONCEPT_ID", IntegerType(), True), \
            StructField("PROVIDER_SOURCE_VALUE", StringType(), False), \
            StructField("SPECIALTY_SOURCE_VALUE", StringType(), True), \
            StructField("SPECIALTY_SOURCE_CONCEPT_ID", IntegerType(), True), \
            StructField("GENDER_SOURCE_VALUE", StringType(), False), \
            StructField("GENDER_SOURCE_CONCEPT_ID", IntegerType(), True)])

        model_schema['visit_occurrence'] = StructType([ \
            StructField("VISIT_OCCURRENCE_ID", IntegerType(), False), \
            StructField("PERSON_ID", IntegerType(), False), \
            StructField("VISIT_CONCEPT_ID", IntegerType(), False), \
            StructField("VISIT_START_DATE", StringType(), False), \
            StructField("VISIT_START_TIME", StringType(), True), \
            StructField("VISIT_END_DATE", StringType(), False), \
            StructField("VISIT_END_TIME", StringType(), True), \
            StructField("VISIT_TYPE_CONCEPT_ID", IntegerType(), False), \
            StructField("PROVIDER_ID", IntegerType(), True), \
            StructField("CARE_SITE_ID", IntegerType(), True), \
            StructField("VISIT_SOURCE_VALUE", StringType(), True), \
            StructField("VISIT_SOURCE_CONCEPT_ID", IntegerType(), True)])
        return model_schema


