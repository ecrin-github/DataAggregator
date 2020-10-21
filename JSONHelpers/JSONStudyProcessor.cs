using System;
using System.Collections.Generic;
using System.Text;

namespace DataAggregator
{
    public class JSONStudyProcessor
    {
        JSONDataLayer repo;

        private DBStudy st;
        private lookup study_type;
        private lookup study_status;
        private lookup study_gender_elig;
        private age_param min_age;
        private age_param max_age;

        private List<study_identifier> study_identifiers;
        private List<study_title> study_titles;
        private List<study_topic> study_topics;
        private List<study_feature> study_features;
        private List<study_relationship> study_relationships;
        private List<int> related_objects;

        public JSONStudyProcessor(JSONDataLayer _repo)
        {
            repo = _repo;
        }

        public JSONStudy CreateStudyObject(int id)
        {
            // Re-initialise these compound properties.
            study_type = null;
            study_status = null;
            study_gender_elig = null;
            min_age = null;
            max_age = null;

            study_identifiers = null;
            study_titles = null;
            study_topics = null;
            study_features = null;
            study_relationships = null;
            related_objects = null;

            // Get the singleton study properties from DB

            st = repo.FetchDbStudy(id);

            // Instantiate the top level lookup types
            study_type = new lookup(st.study_type_id, st.study_type);
            study_status = new lookup(st.study_status_id, st.study_status);
            study_gender_elig = new lookup(st.study_gender_elig_id, st.study_gender_elig);
            min_age = new age_param(st.min_age, st.min_age_units_id, st.min_age_units);
            max_age = new age_param(st.max_age, st.max_age_units_id, st.max_age_units);

            JSONStudy jst = new JSONStudy();

            return jst;
        }

    }
}
