SET allow_experimental_analyzer = 1;

DROP TABLE IF EXISTS v1;
DROP TABLE IF EXISTS v2;

CREATE TABLE v1 ( id Int32 ) ENGINE = MergeTree() ORDER BY id;
CREATE TABLE v2 ( value Int32 ) ENGINE = MergeTree() ORDER BY value;

INSERT INTO v1 ( id ) VALUES (1);
INSERT INTO v2 ( value ) VALUES (1);

SELECT * FROM v1 AS t1
JOIN v1 AS t2 USING (id)
JOIN v2 AS n1 ON t1.id = n1.value
JOIN v2 AS n2 ON t1.id = n2.value;

SELECT * FROM v1 AS t1
JOIN v1 AS t2 USING (id)
CROSS JOIN v2 AS n1
CROSS JOIN v2 AS n2;

SELECT * FROM v1 AS t1 JOIN v1 AS t2 USING (id) JOIN v1 AS t3 USING (value); -- { serverError UNKNOWN_IDENTIFIER }

DROP TABLE IF EXISTS v1;
DROP TABLE IF EXISTS v2;
