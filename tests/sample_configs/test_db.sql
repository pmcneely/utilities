PRAGMA foreign_keys = ON;
create TABLE IF NOT EXISTS "apple pie" (alice INTEGER, bob TEXT);
create TABLE IF NOT EXISTS "bananas foster" (claire REAL, dave BLOB, erin TEXT PRIMARY KEY);
create TABLE IF NOT EXISTS "banana details" (
    "fred rumors" TEXT NOT NULL,
    gina REAL,
    herb INTEGER,
    FOREIGN KEY ("fred rumors") REFERENCES "bananas foster" (erin)
);