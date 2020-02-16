/*********************************************************************************
# Copyright 2016 Observational Health Data Sciences and Informatics
#
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
********************************************************************************/

\COPY synthea.concept FROM '/home/lambert/Dropbox/Research/vocab_download_v5/CONCEPT.csv' WITH DELIMITER E'	' CSV HEADER QUOTE E'\b';
\COPY synthea.concept_ancestor FROM '/home/lambert/Dropbox/Research/vocab_download_v5/CONCEPT_ANCESTOR.csv' WITH DELIMITER E'	' CSV HEADER QUOTE E'\b';
\COPY synthea.concept_class FROM '/home/lambert/Dropbox/Research/vocab_download_v5/CONCEPT_CLASS.csv' WITH DELIMITER E'	' CSV HEADER QUOTE E'\b';
\COPY synthea.concept_relationship FROM '/home/lambert/Dropbox/Research/vocab_download_v5/CONCEPT_RELATIONSHIP.csv' WITH DELIMITER E'	' CSV HEADER QUOTE E'\b';
DELETE FROM synthea.concept_relationship WHERE invalid_reason is not null;
\COPY synthea.concept_synonym FROM '/home/lambert/Dropbox/Research/vocab_download_v5/CONCEPT_SYNONYM.csv' WITH DELIMITER E'	' CSV HEADER QUOTE E'\b';
\COPY synthea.domain FROM '/home/lambert/Dropbox/Research/vocab_download_v5/DOMAIN.csv' WITH DELIMITER E'	' CSV HEADER QUOTE E'\b';
\COPY synthea.relationship FROM '/home/lambert/Dropbox/Research/vocab_download_v5/RELATIONSHIP.csv' WITH DELIMITER E'	' CSV HEADER QUOTE E'\b';
\COPY synthea.vocabulary FROM '/home/lambert/Dropbox/Research/vocab_download_v5/VOCABULARY.csv' WITH DELIMITER E'	' CSV HEADER QUOTE E'\b';
\COPY synthea.drug_strength FROM '/home/lambert/Dropbox/Research/vocab_download_v5/DRUG_STRENGTH.csv' WITH DELIMITER E'	' CSV HEADER QUOTE E'\b';
