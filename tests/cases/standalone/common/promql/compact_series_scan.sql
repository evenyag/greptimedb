-- Compact-mode PoC validation also runs this case against an explicitly enabled
-- local server. Results must be identical with the default v2 path.
CREATE TABLE compact_scan_physical (
  ts TIMESTAMP(3) TIME INDEX,
  greptime_value DOUBLE,
) ENGINE = metric WITH ("physical_metric_table" = "");

CREATE TABLE compact_scan_metric (
  host STRING NULL,
  job STRING NULL,
  ts TIMESTAMP(3) NOT NULL,
  greptime_value DOUBLE NULL,
  TIME INDEX (ts),
  PRIMARY KEY(host, job),
) ENGINE = metric WITH (on_physical_table = 'compact_scan_physical');

INSERT INTO compact_scan_metric (host, job, ts, greptime_value) VALUES
  ('a', 'api', 0, 1), ('a', 'api', 1000, 2), ('b', 'db', 0, 10);
ADMIN FLUSH_TABLE('compact_scan_physical');
INSERT INTO compact_scan_metric (host, job, ts, greptime_value) VALUES
  ('a', 'api', 2000, 3), ('b', 'db', 2000, 20);

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (2, 2, '1s') compact_scan_metric{job=~"api|db"};
TQL EVAL (2, 2, '1s') sum_over_time(compact_scan_metric{host="a"}[3s]);
TQL EVAL (2, 2, '1s') compact_scan_metric{host!="b"} > 2;

ADMIN FLUSH_TABLE('compact_scan_physical');
-- SQLNESS SORT_RESULT 3 1
TQL EVAL (2, 2, '1s') compact_scan_metric{job=~"api|db"};
TQL EVAL (2, 2, '1s') sum_over_time(compact_scan_metric{host="a"}[3s]);
SELECT host, job, ts, greptime_value FROM compact_scan_metric WHERE greptime_value > 1 ORDER BY host, ts;

ALTER TABLE compact_scan_metric ADD COLUMN zone STRING NULL PRIMARY KEY;
INSERT INTO compact_scan_metric (host, job, zone, ts, greptime_value) VALUES ('c', 'api', 'west', 2000, 30);
TQL EVAL (2, 2, '1s') compact_scan_metric{host="c"};
TQL EVAL (2, 2, '1s') compact_scan_metric{host="a"};

DROP TABLE compact_scan_metric;
DROP TABLE compact_scan_physical;
