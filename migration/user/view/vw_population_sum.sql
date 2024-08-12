DROP VIEW IF EXISTS ${schema:raw}.vw_population_sum CASCADE;

CREATE OR REPLACE VIEW ${schema:raw}.vw_population_sum AS
SELECT SUM(CAST((data->>'Population') AS BIGINT)) as total_population
FROM ${schema:raw}.api_data,
     jsonb_array_elements(doc_record->'data') as data
WHERE CAST((data->>'ID Year') AS INTEGER) IN (2018, 2019, 2020)
  AND is_active = true
  AND is_deleted = false;