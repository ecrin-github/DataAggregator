# DataAggregator
Combines data from different sources, each in their own database, into the central mdr database.

The program takes the new or revised data within the ad tables and transfers it to the central tables within the mdr database, also updating the link information between studies and data objects. The aggregated data is held within tables in the st (study), ob (object) and nk (links) schemas. A fourth schema, 'core' is then populated as a direct import from the others, to provide a single simplified dataset that can be exported to other systems. 

The aggregation process is a mixture of calls to SQL code (for the simpler transfer operations, e.g. addition of new data) and code based processing (when relatively complex comparison of entities is required, e.g. for revised data).

### Parameters
There are no parameters - the aggregation process acts on the whole MDR system, i.e. it takes the data from each source database in turn and brings new or revised data into the core database tables.

There is an order, however, to the aggregation process. Trial registry data is agregated first, in the order of the trial registry's 'preference', as listed within the database (so ClinicalTrials.gov is the first). The purpose of this ordering is to allow the more informative registries to create the base study record, which simplifies the aggregation process. After that data repository data can be aggregated. 

### Provenance
* Author: Steve Canham
* Organisation: ECRIN (https://ecrin.org)
* System: Clinical Research Metadata Repository (MDR)
* Project: EOSC Life
* Funding: EU H2020 programme, grant 824087

