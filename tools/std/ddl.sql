CREATE TABLE test (a int PRIMARY KEY, b int, c text);
COMMENT ON TABLE test IS 'Description';
CREATE INDEX IF NOT EXISTS idx_test_a_b ON test (a, b);
