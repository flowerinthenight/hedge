-- for spindle
CREATE TABLE locktable (
    name STRING(MAX) NOT NULL,
    token TIMESTAMP OPTIONS (allow_commit_timestamp=true),
    owner STRING(MAX)
) PRIMARY KEY (name);

-- for hedge
CREATE TABLE logtable (
    id STRING(MAX),
    key STRING(MAX),
    value STRING(MAX),
    leader STRING(MAX),
    timestamp TIMESTAMP OPTIONS (allow_commit_timestamp=true)
) PRIMARY KEY (key, id)
