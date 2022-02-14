PRAGMA foreign_keys = ON;
create TABLE IF NOT EXISTS "apple pie" (alice INTEGER, bob TEXT);
create TABLE IF NOT EXISTS "bananas foster" (claire REAL, dave BLOB, erin TEXT PRIMARY KEY);
create TABLE IF NOT EXISTS "banana details" (
    fred TEXT NOT NULL,
    gina REAL,
    herb INTEGER,
    FOREIGN KEY (fred) REFERENCES "bananas foster" (erin)
);