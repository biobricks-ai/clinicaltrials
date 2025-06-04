-- Get table statistics for split tables

SELECT 'CORE TABLE' as table_name, COUNT(*) as total_records FROM local.ctg_studies_core;

-- @@

SELECT 'DERIVED TABLE' as table_name, COUNT(*) as total_records FROM local.ctg_studies_derived;
