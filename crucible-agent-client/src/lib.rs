use anyhow::Result;

mod progenitor_support {
    use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};

    const PATH_SET: &AsciiSet = &CONTROLS
        .add(b' ')
        .add(b'"')
        .add(b'#')
        .add(b'<')
        .add(b'>')
        .add(b'?')
        .add(b'`')
        .add(b'{')
        .add(b'}');

    pub(crate) fn encode_path(pc: &str) -> String {
        utf8_percent_encode(pc, PATH_SET).to_string()
    }
}

pub mod types {
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Debug)]
    pub struct CreateRegion {
        pub block_size: i64,
        pub extent_count: i64,
        pub extent_size: i64,
        pub id: RegionId,
        pub volume_id: String,
    }

    #[derive(Serialize, Deserialize, Debug)]
    pub struct Region {
        pub block_size: i64,
        pub extent_count: i64,
        pub extent_size: i64,
        pub id: RegionId,
        pub port_number: i64,
        pub state: State,
        pub volume_id: String,
    }

    #[derive(Serialize, Deserialize, Debug)]
    pub struct RegionId(pub String);

    impl std::fmt::Display for RegionId {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    #[derive(Serialize, Deserialize, Debug)]
    #[serde(rename_all = "lowercase")]
    pub enum State {
        Requested,
        Created,
        Tombstoned,
        Destroyed,
        Failed,
    }
}

pub struct Client {
    pub id: uuid::Uuid,
    pub addr: std::net::SocketAddr,
    baseurl: String,
    client: reqwest::Client,
}

impl Client {
    pub fn new(id: &uuid::Uuid, addr: &std::net::SocketAddr) -> Client {
        let id = id.clone();
        let addr = addr.clone();
        let baseurl = format!("http://{}:{}", addr.ip(), addr.port());
        let dur = std::time::Duration::from_secs(15);
        let client = reqwest::ClientBuilder::new()
            .connect_timeout(dur)
            .timeout(dur)
            .build()
            .unwrap();

        Client { id, addr, baseurl, client }
    }

    /**
     * region_list: GET /crucible/0/regions
     */
    pub async fn region_list(&self) -> Result<Vec<types::Region>> {
        let url = format!("{}/crucible/0/regions", self.baseurl,);

        let res = self.client.get(url).send().await?.error_for_status()?;

        Ok(res.json().await?)
    }

    /**
     * region_create: POST /crucible/0/regions
     */
    pub async fn region_create(
        &self,
        body: &types::CreateRegion,
    ) -> Result<types::Region> {
        let url = format!("{}/crucible/0/regions", self.baseurl,);

        let res = self
            .client
            .post(url)
            .json(body)
            .send()
            .await?
            .error_for_status()?;

        Ok(res.json().await?)
    }

    /**
     * region_get: GET /crucible/0/regions/{id}
     */
    pub async fn region_get(
        &self,
        id: types::RegionId,
    ) -> Result<types::Region> {
        let url = format!(
            "{}/crucible/0/regions/{}",
            self.baseurl,
            progenitor_support::encode_path(&id.to_string()),
        );

        let res = self.client.get(url).send().await?.error_for_status()?;

        Ok(res.json().await?)
    }

    /**
     * region_delete: DELETE /crucible/0/regions/{id}
     */
    pub async fn region_delete(&self, id: types::RegionId) -> Result<()> {
        let url = format!(
            "{}/crucible/0/regions/{}",
            self.baseurl,
            progenitor_support::encode_path(&id.to_string()),
        );

        let res = self.client.delete(url).send().await?.error_for_status()?;

        Ok(res.json().await?)
    }
}
