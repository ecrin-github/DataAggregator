# DataAggregator
Combines data from different sources, each in a separate database, into the central mdr database.

The program takes all the data within the ad tables in the various source databases and loads it to central tables within the mdr database, dealing with multiple entries for studies and creating the link data information between studies and data objects. The aggregated data is held within tables in the st (study), ob (object) and nk (links) schemas. A fourth schema, 'core' is then populated as a direct import from the others, to provide a single simplified mdr dataset that can be exported to other systems. <br/>
Note that the aggregation process starts from scratch each time - there is no attempt to edit existing data. All tables are re-created during the main aggregation processes (triggered by -D and -C, as described below).This is to simplify the process and make the system easier to maintain.<br/><br/>
The program represents the fourth stage in the 4 stage MDR extraction process:<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Download => Harvest => Import => **Aggregation**<br/><br/>
For a much more detailed explanation of the extraction process,and the MDR system as a whole, please see the project wiki (landing page at https://ecrin-mdr.online/index.php/Project_Overview).<br/>

### Parameters
The program is a concxole app. There are a variety of flag type parameters, that can be used alone or in combination (though only some combinations make sense).
These include:<br/>
-D: which indicates that the aggregating data transfer should take place, from the source ad tables to the tables in the st (studies), ob (objects) and nk (links) schemas. This is the necessary first step of the aggregation process.<br/>
-C: indicates that the core tables should be created and filled from the aggregate tables, i.e. data is combined from the st, ob and nk schemas in to a single, simpler core schema.<br/>
-J: indicates that the core data be used to create JSON versions of the data withiknn the core database.<br/>
-F: indicates that the core data should be used to create JSON files, with the study and data object data. It has no effect unless the -J parameter is also supplied.<br/>
-S: collects statistics about the existing data, in both the ad tables and in the central aggregated tables.<br/>  
-Z: zips the json files created by the -F parameter into a series of zip files, with up to 100,000 files in each. This is for ease of transfer to other systems.<br/>        
<br/>   
The -S parameter can be provided at any time or on its own. It makes little sense to trigger the other processes without an initial call using -D. A -C call then follows, and then -J -F, and finally -Z. The system can cope with multipolple parameters, and applies them in the order given: -D -C -J -F -S -Z. It is easier to see what is happening, however, to make multiple calls to the program working through the parametrer list as described.<br/>  

### Overview

### Provenance
* Author: Steve Canham
* Organisation: ECRIN (https://ecrin.org)
* System: Clinical Research Metadata Repository (MDR)
* Project: EOSC Life
* Funding: EU H2020 programme, grant 824087

