-- Placeholder production-ready SQLMesh model for deployment
-- No input tables, business logic, calculations, or data quality rules provided
-- Includes SQLMesh best practices and syntax compliance

CREATE OR REPLACE MODEL empty_model_no_requirements
OWNER 'team_empty'
tags ('placeholder', 'empty_model')
SELECT 1 AS dummy_column
WHERE FALSE;

-- Due to absence of input data and business rules, unique_key and audits are omitted
-- Model serves as a stable stub for future extension
