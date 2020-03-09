# Python-based ETL of Synthea data to OMOP CSV files

This is an implementation of an ETL designed to take a Synthea generated csv dataset and convert it to either CDMv5 or CDMv6 compatible CSV files that can then be bulk-loaded into your RDBMS of choice.

It is designed to be massively scalable but still run on a user's workstation or laptop

ETL-Synthea is used as a reference implementation and output should be identical

Drug era and condition era sql scripts that are typically run as post process on the OMOP database are implemented in python.

## Overview of Steps

0) Shortcut: download ready-to-go data

1) Install required software

2) Download Synthea and generate csv files

3) Download CDMv5 Vocabulary files

4) Setup the .env file to specify file locations

## 0. Shortcut: download ready-to-go data

Ready to go datasets for Synthea, FHIR, CDMv5.3.1 and CDMv6 are available.

### Healthcare Sample

Datasets of 100 patients for every submodule of synthea.

### Synthetic Finland

OMOP Dataset - 100K patient subset.  Full populuation of about 5 million patients in planning

### Synthetic Norway

OMOP Dataset - 100K patients.  Full populuation of about 5 million patients in planning

### Sample Postgres Upload Script
 
Here is an example PostgreSQL psql client copy command to load the concept file into the concept table in a schema called 'cdm'. 
 
\copy cdm.concept from '/download_directory_path/concept' null as '\\000' delimiter ','

You are completed after this step.


## 1. Install required software

The ETL process requires Python 2.7 with the python-dotenv package.

### Linux

Python 2.7 and python-pip must be installed if they are not already
present. If you are using a RedHat distribution the following commands
will work (you must have system administrator privileges):

``sudo yum install python``  
``sudo yum install python-pip``

The python-pip package enables a non-administrative user to install
python packages for their personal use. From the python_etl directory
run the following command to install the python-dotenv package:

``pip install -r requirements.txt``

### Windows + Cygwin

We have been able to run the ETL under Windows using cygwin, available at
<https://www.cygwin.com>. Be sure to install the following packages
with the cygwin installer:

python  
python-setuptools  

After that run the following in order to install pip:  
``easy_install-2.7 pip``

Then to install python-dotenv, run the following command within the python\_etl folder:  
``pip install -r requirements.txt``


## 2. Download Synthea and run for your country

## 3. Download CDMv5 Vocabulary files
Download vocabulary files from <http://www.ohdsi.org/web/athena/>, ensuring that you select at minimum, the following vocabularies:
SNOMED, ICD9CM, ICD9Proc, CPT4, HCPCS, LOINC, RxNorm, and NDC.

- Unzip the resulting .zip file to a directory.

## 4. Setup the .env file to specify file locations
Edit the variables in the .env file which specify various directories used during the ETL process.
Example .env files are provided for Windows (.env.example.windows) and unix (.env.example.unix) runs,
differing only in path name constructs.
