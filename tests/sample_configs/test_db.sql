PRAGMA foreign_keys = ON;
create TABLE IF NOT EXISTS "apple pie" (alice INTEGER, bob TEXT, bonnie TEXT NOT NULL);
create TABLE IF NOT EXISTS "bananas foster" (claire REAL NOT NULL, dave BLOB, erin TEXT PRIMARY KEY);
create TABLE IF NOT EXISTS "banana details" (
    "fred rumors" TEXT NOT NULL,
    frank INTEGER,
    gina REAL,
    herb INTEGER,
    PRIMARY KEY ("fred rumors", frank),
    FOREIGN KEY ("fred rumors") REFERENCES "bananas foster" (erin)
);