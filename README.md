# DataAggregator
Combines data from different sources, each in a separate database, into the central mdr database.

The program takes all the data within the ad tables in the various source databases and loads it to central tables within the mdr database, dealing with multiple entries for studies and creating the link data information between studies and data objects. The aggregated data is held within tables in the st (study), ob (object) and nk (links) schemas. A fourth schema, 'core' is then populated as a direct import from the others, to provide a single simplified mdr dataset that can be exported to other systems. <br/>
Note that the aggregation process starts from scratch each time - there is no attempt to edit existing data. All tables are re-created during the main aggregation processes (triggered by -D and -C, as described below). This is to simplify the process and make the system easier to maintain.<br/><br/>
The program represents the fourth stage in the 4 stage MDR extraction process:<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Download => Harvest => Import => **Aggregation**<br/><br/>
For a much more detailed explanation of the extraction process,and the MDR system as a whole, please see the project wiki (landing page at https://ecrin-mdr.online/index.php/Project_Overview).<br/>

### Parameters
The program is a console app. There are a variety of flag type parameters, that can be used alone or in combination (though only some combinations make sense).
These include:<br/>
**-D**: which indicates that the aggregating data transfer should take place, from the source ad tables to the tables in the st (studies), ob (objects) and nk (links) schemas. This is the necessary first step of the aggregation process.<br/>
**-C**: indicates that the core tables should be created and filled from the aggregate tables, i.e. data is combined from the st, ob and nk schemas in to a single, simpler core schema.<br/>
**-J**: indicates that the core data be used to create JSON versions of the data within the core database.<br/>
**-F**: indicates that the core data should be used to create JSON files of two types, one for each study and another for each data object. It has no effect unless the -J parameter is also supplied.<br/>
**-S**: collects statistics about the existing data, from both the ad tables and the central aggregated tables.<br/>
The -S parameter can be provided at any time or on its own. It makes little sense to trigger the other processes without an initial call using -D. A -C call would then normally follows, and then -J (-F), and finally -Z. The system can cope with multiple parameters, and applies them in the order given: -D -C -J -F -S -Z. It is easier to see what is happening, however, to make multiple calls to the program working through the parameter list as described.<br/>  

### Overview
The main aggregation process, as triggered by -D, begins with the creation of a new set of central tables in the st, ob and nk schemas. After that:
* The program interrogates the monitor database to obtain a list of the sources and the names of the corresponding databases.
* it runs through those sources to obtain a list - from each - of 'other registry ids'. In other words it builds up a list of all studies that are registered in more than one registry, which includes the ids of that study in each registry. About 25,000 studies are registered in 2 registries and about another 1,000 are registered in 3 or more registries.
* It uses the concept of a 'preferred' source (the more information a source normally provides, the more 'preferred' it is) to order the study ids of those studies with more than one registry entry. The result is a table that relates the id of any multiple-registered study in any registry to the id of that same study in its most 'preferred' source.
* Some studies (several hundred) have more complex relationships - for example are registered in two registries but in one are entered as a group of related studies rather than having a simple 1-to-1 relationship with the other registry study. These are removed and the link data are instead added to the study-study relationship data.
* The study data is then added to the aggregate tables, in the order of most preferred source, working through the list to the least preferred. Apart from the first (ClinicalTriuals.gov) the study id of any imported study is checked against the table of multiple-registered studies. If it exists in that table it is not added as a separate record but instead is given the same id as that of the most preferred version of that study.  
* Study data, including all attribute data, of studies that are genuinely new to the system are simply added to the aggregate data. Also immediately added are all associated data objects and their attribute data. 
* Study data for a study that already exists in the system is checked first to see if it represents new data. In this case the main study data record is *not* added - that can only come from the 'preferred' source. Study attributes are only added if they do not already exist, so far as that can be readily checked by the program. Data objects in the 'non-preferred' versions of the study may already exist but the nature of the data is that genuine duplication of data objects from different sources is extemely rare. Almost all data objects are therefore added. Studies with multiple entries in different registries therefore have their data built up from a single 'preferred' source for the main study record, from potentially multiple registries for study attributes, and from all source registries for the associated data objects.
* The link between data objects and studies - found within the source data object data - is transfered to link tables. The 'parent study' id is transformed into its most 'preferred' form if and when necessary, to ensure that the links work properly in the aggregated system. Also transferred to link tables is the provenance data that indicates when each study and data object was last retrieved from the source. 
* For sources where there are no studies - just data objects - the process is necessarily different. It must also follow after the aggregation of study data, to ensure that all  studies are in the central system.
* This only applies to PubMed data at the moment. For PubMed, the links between the PubMed data and the studies are first identified. Two sources are used - the 'bank id' data within the PubMed data itself, referring to trial registry ids, and the 'source id' data in the study based sources, where references are provided to relevant papers. These two sets of data are combined and de-duplicated, and two final sets of data are created: the distinct list of PubMed data objects, and the list of links between those objects and studies. Unlike most data objects in the study based resources, PubMed data objects can be linked to multiple studies, and of course studies may have multiple article references. The linkage is therefore complex and requires considerable additional processing.

Most of the other options provided by the progrram are relatively simple and self contained. The -C option copies the data from the aggregating schema (st, ob, and nk) to the core schema without any processing, other than creating the provenance strings for both studies and data objects. The latter may be composite if more than one source was involved.<br/>

### Logging
There is an aggregation event record stored after each aggregation process that summarises the numbers of studies, data onbjects and links involved, as well as recording the time of the process, but the main statistics are provided by the statistics module, which records the state of each source before, and the central tables after, the aggregation process. In addition each source record has a datetime exported value added by the aggregation process, and each central record includes a datetime added field.

### Provenance
* Author: Steve Canham
* Organisation: ECRIN (https://ecrin.org)
* System: Clinical Research Metadata Repository (MDR)
* Project: EOSC Life
* Funding: EU H2020 programme, grant 824087

