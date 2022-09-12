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

-- Ensure there is no more than one rack row
-- We rollback the whole transaction in the case of a constraint
-- violation, since we have broken an invariant and should not proceed.
CREATE TRIGGER ensure_rack_contains_at_most_one_row 
BEFORE INSERT ON rack
WHEN (SELECT COUNT(*) FROM rack) >= 1
BEGIN
    SELECT RAISE(ROLLBACK, 'maximum one rack');
END;
