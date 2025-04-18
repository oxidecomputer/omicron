// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use nexus_db_lookup::LookupPath;
use nexus_db_lookup::lookup::Instance;
use nexus_db_lookup::lookup::Project;
use nexus_db_model::Name;
use nexus_db_queries::db::pub_test_utils::TestDatabase;
use omicron_test_utils::dev;

/* This is a smoke test that things basically appear to work. */
#[tokio::test]
async fn test_lookup() {
    let logctx = dev::test_setup_log("test_lookup");
    let db = TestDatabase::new_with_datastore(&logctx.log).await;
    let (opctx, datastore) = (db.opctx(), db.datastore());
    let project_name: Name = Name("my-project".parse().unwrap());
    let instance_name: Name = Name("my-instance".parse().unwrap());

    let leaf = LookupPath::new(&opctx, datastore)
        .project_name(&project_name)
        .instance_name(&instance_name);
    assert!(matches!(&leaf,
            Instance::Name(Project::Name(_, p), i)
            if **p == project_name && **i == instance_name));

    let leaf = LookupPath::new(&opctx, datastore).project_name(&project_name);
    assert!(matches!(&leaf,
            Project::Name(_, p)
            if **p == project_name));

    let project_id = "006f29d9-0ff0-e2d2-a022-87e152440122".parse().unwrap();
    let leaf = LookupPath::new(&opctx, datastore).project_id(project_id);
    assert!(matches!(&leaf,
            Project::PrimaryKey(_, p)
            if *p == project_id));

    db.terminate().await;
    logctx.cleanup_successful();
}
