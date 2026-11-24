-- Least privilege. pipeline writes, grafana_ro and mcp_ro only read what they need.
GRANT USAGE ON SCHEMA raw, staging, dw, gold, quality, governance TO pipeline;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA raw, staging, dw, gold, quality, governance TO pipeline;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA raw, staging, dw, gold, quality, governance TO pipeline;

GRANT USAGE ON SCHEMA dw, gold, quality TO grafana_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA dw, gold, quality TO grafana_ro;

GRANT USAGE ON SCHEMA quality TO mcp_ro;
GRANT SELECT ON quality.dq_results TO mcp_ro;
