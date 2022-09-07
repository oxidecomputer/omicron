CREATE TABLE IF NOT EXISTS key_shares (
    epoch            INTEGER   NOT NULL,
    share            BLOB      NOT NULL,
    share_digest     BLOB      NOT NULL,
    committed        INTEGER   NOT NULL,
    
    PRIMARY KEY (epoch)
);

CREATE TABLE IF NOT EXISTS rack (
    uuid             TEXT     NOT NULL,
    
    PRIMARY KEY (uuid)
);

CREATE TABLE IF NOT EXISTS encrypted_root_secrets (
    epoch            INTEGER    NOT NULL,
    salt             BLOB       NOT NULL,
    secret           BLOB       NOT NULL,
    tag              BLOB       NOT NULL,
    
    PRIMARY KEY (epoch)
    FOREIGN KEY (epoch) REFERENCES key_share_prepares (epoch)
);
