SELECT MAX(etl_date)
FROM etl_log
WHERE 
    step = :step and
    table_name ilike :table_name and
    status = :status and
    component = :component