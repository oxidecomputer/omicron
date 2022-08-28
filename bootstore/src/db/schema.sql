CREATE TABLE IF NOT EXISTS key_share_prepare (
    epoch    INTEGER   NOT NULL,
    share    TEXT      NOT NULL,
    
    PRIMARY KEY(epoch)
)

CREATE TABLE IF NOT EXISTS key_share_commit(
    epoch    INTEGER   NOT NULL,
    
    PRIMARY KEY(epoch)
)

CREATE TABLE IF NOT EXISTS encrypted_root_secret(
    epoch    INTEGER    NOT NULL,
    salt     BLOB       NOT NULL,
    secret   BLOB       NOT NULL,
    tag      BLOB       NOT NULL,
)
