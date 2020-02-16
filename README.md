# ETL-Synthea-Python
Release date: Feb 16, 2020

This project contains the source code to convert the public
Centers for Medicare & Medicaid Services (CMS) Data Entrepreneurs'
[Synthetic Public Use File](<https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/DE_Syn_PUF.html>) (DE-SynPUF) to .csv files suitable for loading into an OMOP Common Data Model v5.2 database.

Synthea is able to generate an unlimited amount of patient records in multiple countries

This tools is capable of creating massive v5 and v6 patient records using the OMOP CDM for multiple countries

## What's in Here?


### python_etl
A complete Python-based ETL of the Synthea data into CDMv5 and CDMv6 compatible CSV
files. See the [README.md](https://github.com/OHDSI/ETL-CMS/blob/master/python_etl/README.md) file therein for detailed instructions for
running the ETL, as well as creating and loading the data into a CDMv5 database.


### scripts
The scripts folder holds handy scripts for downloading and munging some of the raw
data used in the ETL process. Instructions for their use can be found
in the [python_etl/README.md](https://github.com/OHDSI/ETL-CMS/blob/master/python_etl/README.md) file.


### hand_conversion
@claire-oi hand-converted a couple patients worth of SynPUF data into CDMv5.  Along the way she found several inconsistencies and ambiguities with the ETL specification which have hopefully been addressed.  Her internal notes, along with the sample patients and her hand-converted CDM outputs are available in the [hand_conversion](https://github.com/OHDSI/ETL-CMS/tree/master/hand_conversion) directory.

### Additional Resources
- [Partial ETL of SEER Medicare to OMOP CDMv4](https://github.com/outcomesinsights/seer_to_omop_cdmv4) - [Outcomes Insights](http://outins.com) has released their partial implementation of a SEER Medicare ETL, along with their specification document to serve as a reference for the ETLs created by this workgroup
 - The [OHDSI Medicare ETL SynPUF.pdf](https://github.com/OHDSI/ETL-CMS/blob/master/OHDSI%20Medicare%20ETL%20SynPuf.pdf) provides a light overview of the differences between SynPUF and other Medicare datasets, such as SEER Medicare and Medicare LDS.  This presentation was presented to the OHDSI CMS ETL workgroup on February 2015 by Jennifer Duryea at [Outcomes Insights](http://outins.com).

#History of contributions

Based on CMS-ETL written by:

- Ryan Duryea [@aguynamedryan](https://github.com/aguynamedryan), Outcomes Insights, Inc.
- Erica Voss [@ericaVoss](http://forums.ohdsi.org/users/ericaVoss), Janssen Research and Development.
- Jennifer Duryea [@jenniferduryea](https://github.com/jenniferduryea), Outcomes Insights, Inc.
- Don O'Hara [@donohara](https://github.com/donohara), Evidera.
- Claire Cangialose [@claire-oi](https://github.com/claire-oi), Outcomes Insights, Inc.
- Patrick Ryan [@Patrick_Ryan](http://forums.ohdsi.org/users/Patrick_Ryan), Janssen Research and Development.
- Christophe Lambert [@Christophe_Lambert](http://forums.ohdsi.org/users/Christophe_Lambert), University of New Mexico, Center for Global Health, Division of Translational Informatics, Department of Internal Medicine
- Praveen Kumar [@Praveen_Kumar](http://forums.ohdsi.org/users/Praveen_Kumar), University of New Mexico, Department of Computer Science
- Amritansh [@Amritansh](http://forums.ohdsi.org/users/Amritansh), University of New Mexico, Department of Computer Science
