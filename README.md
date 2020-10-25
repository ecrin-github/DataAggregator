# DataAggregator
Combines data from different sources, each in a separate database, into the central mdr database.

The program takes all the data within the ad tables in the various source databases and loads it to central tables within the mdr database, dealing with multiple entries for studies and creating the link data information between studies and data objects. The aggregated data is held within tables in the st (study), ob (object) and nk (links) schemas. A fourth schema, 'core' is then populated as a direct import from the others, to provide a single simplified mdr dataset that can be exported to other systems. <br/>
The program represents the fourth stage in the 4 stage MDR extraction process:<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Download => Harvest => Import => **Aggregation**<br/><br/>
For a much more detailed explanation of the extraction process,and the MDR system as a whole, please see the project wiki (landing page at https://ecrin-mdr.online/index.php/Project_Overview).<br/>

### Parameters
There are no parameters - the aggregation process acts on the whole MDR system, i.e. it takes the data from each source database in turn and brings new or revised data into the core database tables.

There is an order, however, to the aggregation process. Trial registry data is agregated first, in the order of the trial registry's 'preference', as listed within the database (so ClinicalTrials.gov is the first). The purpose of this ordering is to allow the more informative registries to create the base study record, which simplifies the aggregation process. After that data repository data can be aggregated. 

### Provenance
* Author: Steve Canham
* Organisation: ECRIN (https://ecrin.org)
* System: Clinical Research Metadata Repository (MDR)
* Project: EOSC Life
* Funding: EU H2020 programme, grant 824087

