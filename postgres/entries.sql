
CREATE TABLE entries (
    entryIndex  int8  PRIMARY KEY,
    entryTerm   int8  NOT NULL,
    entryValueHash bytea NOT NULL UNIQUE,
    entryValue  bytea NOT NULL, 
    entryIssuer varchar NOT NULL,
    entryPrevHash bytea NOT NULL UNIQUE
);
