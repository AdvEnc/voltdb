-- DDL for subqueries table partitioned on varchar column

CREATE TABLE P1 (
  ID INTEGER NOT NULL,
  VCHAR VARCHAR(300) NOT NULL,
  NUM INTEGER,
  RATIO FLOAT,
  PRIMARY KEY (ID, VCHAR)
);
PARTITION TABLE P1 ON COLUMN VCHAR;

CREATE TABLE P2 (
  ID INTEGER NOT NULL,
  VCHAR VARCHAR(300) NOT NULL,
  NUM INTEGER,
  RATIO FLOAT,
  PRIMARY KEY (ID, VCHAR)
);
PARTITION TABLE P2 ON COLUMN VCHAR;

CREATE TABLE R1 (
  ID INTEGER NOT NULL,
  VCHAR VARCHAR(300),
  NUM INTEGER,
  RATIO FLOAT,
  PRIMARY KEY (ID)
);

CREATE TABLE R2 (
  ID INTEGER NOT NULL,
  VCHAR VARCHAR(300),
  NUM INTEGER,
  RATIO FLOAT,
  PRIMARY KEY (ID)
);