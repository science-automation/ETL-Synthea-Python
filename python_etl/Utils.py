import os
import pandas as pd
import datetime
import dateutil.parser


class Utils:
    def __init__(self):
        pass

    # given date in synthea format return the year
    def getYearFromSyntheaDate(self, date):
        return datetime.datetime.strptime(date, "%Y-%m-%d").year

    # given date in synthea format return the month
    def getMonthFromSyntheaDate(self, date):
        return datetime.datetime.strptime(date, "%Y-%m-%d").month

    # given date in synthea format return the day
    def getDayFromSyntheaDate(self, date):
        return datetime.datetime.strptime(date, "%Y-%m-%d").day

    # given gender as M or F return the OMOP concept code
    def getGenderConceptCode(self, gender):
        gendre = gender.upper()
        if gender=='M':
            return '8507'
        elif gender=='F':
            return '8532'
        else:
            return 0

    # given synthea race code return omop code
    def getRaceConceptCode(self, race):
        race = race.upper()
        if race=='WHITE':
            return '8527'
        elif race=='BLACK':
            return '8516'
        elif race=='ASIAN':
            return 8515
        else:
            return '0'

    def getEthnicityConceptCode(self, eth):
        eth = eth.upper()
        #if race=='HISPANIC' or eth=='CENTRAL_AMERICAN' or eth=='DOMINICAN' or eth=='MEXICAN' or eth=='PUERTO_RICAN' or eth=='SOUTH_AMERICAN':
        if eth=='CENTRAL_AMERICAN' or eth=='DOMINICAN' or eth=='MEXICAN' or eth=='PUERTO_RICAN' or eth=='SOUTH_AMERICAN':
            return '38003563'
        else:
            return '0'

    # convert a synthea timestamp like 2020-02-16T05:05:49Z to omop datestamp like 2020-02-16
    def isoTimestampToDate(self, timestamp):
        date = dateutil.parser.parse(timestamp)
        return datetime.date.strftime(date, '%Y-%m-%d')

    # given a datestamp, return on timestamp with default 0 hour
    def getDefaultTimestamp(self, datestamp):
        return str(datestamp) + " 00:00:00"

    #
    def getVisitConcept(self, encounterclass):
        if encounterclass == 'emergency' or encounterclass == 'urgentcare':
            return '9203'
        elif encounterclass == 'ambulatory' or encounterclass == 'wellness' or encounterclass == 'outpatient':
            return '9202'
        else:
            return '0'

    #
    # check memory usage of a pandas dataframe
    #
    def mem_usage(self, pandas_obj):
        if isinstance(pandas_obj,pd.DataFrame):
            usage_b = pandas_obj.memory_usage(deep=True).sum()
        else: # we assume if not a df it's a series
            usage_b = pandas_obj.memory_usage(deep=True)
        usage_mb = usage_b / 1024 ** 2 # convert bytes to megabytes
        return "{:03.2f} MB".format(usage_mb)
 
    #
    # load the concept vocabulary into dataframes
    # can be gz compressed or plain text
    #
    def loadConceptVocabulary(self, BASE_OMOP_INPUT_DIRECTORY, model_omop):
        vocab = {}
        vocabfiledict = {}
        vocablist = ['CONCEPT', 'CONCEPT_RELATIONSHIP']
        # determine if vocabulary files exists and whether they are compressed
        for vocabfile in vocablist:
            if (os.path.exists(os.path.join(BASE_OMOP_INPUT_DIRECTORY,vocabfile + '.csv'))):
                vocabfiledict[vocabfile] = os.path.join(BASE_OMOP_INPUT_DIRECTORY,vocabfile + '.csv')
                compression=None
            elif (os.path.exists(os.path.join(BASE_OMOP_INPUT_DIRECTORY,vocabfile + '.csv.gz'))):
                vocabfiledict[vocabfile] = os.path.join(BASE_OMOP_INPUT_DIRECTORY,vocabfile + '.csv.gz')
                compression='gzip'
            else:
                print("Error:  Could not find " + vocabfile + " vocabulary file")
                exit(1)
            vocab[vocabfile.lower()] = pd.read_csv(vocabfiledict[vocabfile], sep='\t', dtype=model_omop.model_schema[vocabfile.lower()], compression=compression)
        return vocab

    #
    # load the full vocabulary into dataframes
    # can be gz compressed or plain text
    #
    def loadVocabulary(self, BASE_OMOP_INPUT_DIRECTORY, model_omop):
        vocab = {}
        vocabfiledict = {}
        vocablist = ['CONCEPT', 'CONCEPT_RELATIONSHIP', 'CONCEPT_SYNONYM', 'CONCEPT_ANCESTOR', 'CONCEPT_CLASS', 'DRUG_STRENGTH']
        # determine if vocabulary files exists and whether they are compressed
        for vocabfile in vocablist:
            if (os.path.exists(os.path.join(BASE_OMOP_INPUT_DIRECTORY,vocabfile + '.csv'))):
                vocabfiledict[vocabfile] = os.path.join(BASE_OMOP_INPUT_DIRECTORY,vocabfile + '.csv')
                compression=None
            elif (os.path.exists(os.path.join(BASE_OMOP_INPUT_DIRECTORY,vocabfile + '.csv.gz'))):
                vocabfiledict[vocabfile] = os.path.join(BASE_OMOP_INPUT_DIRECTORY,vocabfile + '.csv.gz')
                compression='gzip'
            else:
                print("Error:  Could not find " + vocabfile + " vocabulary file")
                exit(1)
            vocab[vocabfile.lower()] = pd.read_csv(vocabfiledict[vocabfile], sep='\t', dtype=model_omop.model_schema[vocabfile.lower()], compression=compression)
        return vocab

    #
    # following standard omop query for source to standard mapping is implemented in python pandas
    # passing in a vocabulary dictionary, return the source to standard dataframe
    #
    #   SELECT c.concept_code AS SOURCE_CODE, c.concept_id AS SOURCE_CONCEPT_ID,
    #         c.concept_name AS SOURCE_CODE_DESCRIPTION,
    #         c.vocabulary_id AS SOURCE_VOCABULARY_ID, c.domain_id AS SOURCE_DOMAIN_ID,
    #         c.CONCEPT_CLASS_ID AS SOURCE_CONCEPT_CLASS_ID, c.VALID_START_DATE AS SOURCE_VALID_START_DATE,
    #         c.VALID_END_DATE AS SOURCE_VALID_END_DATE, c.INVALID_REASON AS SOURCE_INVALID_REASON,
    #         c1.concept_id AS TARGET_CONCEPT_ID, c1.concept_name AS TARGET_CONCEPT_NAME,
    #         c1.VOCABULARY_ID AS TARGET_VOCABULARY_ID, c1.domain_id AS TARGET_DOMAIN_ID,
    #         c1.concept_class_id AS TARGET_CONCEPT_CLASS_ID,
    #         c1.INVALID_REASON AS TARGET_INVALID_REASON,
    #         c1.standard_concept AS TARGET_STANDARD_CONCEPT
    #   FROM CONCEPT C
    #   JOIN CONCEPT_RELATIONSHIP CR
    #         ON C.CONCEPT_ID = CR.CONCEPT_ID_1
    #         AND CR.invalid_reason IS NULL
    #         AND lower(cr.relationship_id) = 'maps to'
    #   JOIN CONCEPT C1
    #         ON CR.CONCEPT_ID_2 = C1.CONCEPT_ID
    #         AND C1.INVALID_REASON IN (NULL,'')
    def sourceToStandardVocabMap(self, vocab, model_omop):
        concept = vocab['concept']
        concept_relationship = vocab['concept_relationship']
        source = concept[model_omop.model_schema['source_to_standard_source'].keys()]  # get rid of columns we don't need
        source = source.rename(columns=model_omop.model_schema['source_to_standard_source'])
        target = concept[model_omop.model_schema['source_to_standard_target'].keys()]  # get rid of columns we don't need
        target = target.rename(columns=model_omop.model_schema['source_to_standard_target'])
        source_result = pd.merge(source,concept_relationship[(concept_relationship["invalid_reason"].isnull()) & (concept_relationship["relationship_id"].str.contains('Maps to'))], \
            how='inner', left_on='source_concept_id', right_on='concept_id_1')
        source_result = source_result[model_omop.model_schema['source_to_standard_source'].values()].drop_duplicates()
        target_result = pd.merge(target,concept_relationship[concept_relationship["invalid_reason"].isnull()], \
            how='inner', left_on='target_concept_id', right_on='concept_id_2')
        target_result = target_result[ model_omop.model_schema['source_to_standard_target'].values()].drop_duplicates()
        result = pd.merge(source_result, target_result, how='inner', left_on='source_concept_id', right_on='target_concept_id')
        return result

    #
    # following standard omop query for source to source mapping is implemented in python pandas
    # passing in a vocabulary dictionary, return the source to standard dataframe
    #
    #   SELECT c.concept_code AS SOURCE_CODE, c.concept_id AS SOURCE_CONCEPT_ID, 
    #         c.CONCEPT_NAME AS SOURCE_CODE_DESCRIPTION,  c.vocabulary_id AS SOURCE_VOCABULARY_ID,
    #         c.domain_id AS SOURCE_DOMAIN_ID, c.concept_class_id AS SOURCE_CONCEPT_CLASS_ID, 
    #         c.VALID_START_DATE AS SOURCE_VALID_START_DATE, c.VALID_END_DATE AS SOURCE_VALID_END_DATE, 
    #         c.invalid_reason AS SOURCE_INVALID_REASON, c.concept_ID as TARGET_CONCEPT_ID,
    #         c.concept_name AS TARGET_CONCEPT_NAME, c.vocabulary_id AS TARGET_VOCABULARY_ID, c.domain_id AS TARGET_DOMAIN_ID, 
    #         c.concept_class_id AS TARGET_CONCEPT_CLASS_ID, c.INVALID_REASON AS TARGET_INVALID_REASON, 
    #         c.STANDARD_CONCEPT AS TARGET_STANDARD_CONCEPT
    #   FROM CONCEPT c
    def sourceToSourceVocabMap(self, vocab, model_omop):
        concept = vocab['concept']
        source = concept[model_omop.model_schema['source_to_standard_source'].keys()]  # get rid of columns we don't need
        source = source.rename(columns=model_omop.model_schema['source_to_standard_source'])
        target = concept[model_omop.model_schema['source_to_standard_target'].keys()]  # get rid of columns we don't need
        target = target.rename(columns=model_omop.model_schema['source_to_standard_target'])
        result = pd.merge(source, target, how='inner', left_on='source_concept_id', right_on='target_concept_id')
        result = result.drop_duplicates()
        return result

    # create a counter file to read/save id starting numbers
    def initCounterFile(self, omoplist, initnum, file):
        f= open(file,"w")
        for datatype in omoplist:
            f.write(datatype + "_id=" + str(initnum) + "\n") 
        f.close()

    # read counter into a dictionary
    def getCounter(self, counterfile):
        counter = {}
        with open(counterfile) as file:
            for line in file:
                name, var = line.partition("=")[::2]
                counter[name.strip()] = int(var)
        return counter

    # take dictionary and write counter
    def writeCounter(self, counter, counterfile):
        f= open(counterfile,"w")
        for key, value in counter.iteritems():
            f.write(key + "=" + str(value) + "\n")
        f.close()

    # given an openaddresses dataframe, return a random real address within the zip code
    def getRealOpenAddress(self, code, addresses):
        temp = addresses.loc[addresses['POSTCODE'] == code]
        if len(temp) > 0:
            sample = temp.sample(n=1)
            return str(sample.iloc[0]['LAT']) + "," + str(sample.iloc[0]['LON']) + "," + str(sample.iloc[0]['STREET']) + " " + str(sample.iloc[0]['NUMBER'])
        else:
            return ",,"

    # given a finland address dataframe, return a random real address within the zip code
    def getRealFIAddress(self, code, addresses):
        temp = addresses.loc[addresses['postal_code'] == code]
        if len(temp) > 0:
            sample = temp.sample(n=1)
            return str(sample.iloc[0]['latitude_wgs84']) + "," + str(sample.iloc[0]['longitude_wgs84']) + "," + str(sample.iloc[0]['street']) + " " + str(sample.iloc[0]['house_number'])
        else:
            return ",,"
