SELECT 'DROP INDEX IF EXISTS "' || indexname || '";'
FROM pg_indexes
WHERE schemaname = 'public'
  AND indexname NOT IN (
    SELECT conname
    FROM pg_constraint
    WHERE contype = 'p'  -- primary key
  )
  AND indexname NOT LIKE 'idx_%'
  AND indexname NOT LIKE 'pg_%';
