-- Восстановление таблицы raw_data на момент, когда была сделана вставка данных
CREATE OR REPLACE TABLE raw_data_clone AS
SELECT * FROM raw_data AT (OFFSET => -5 );
--Удаление и восстановление таблицы
DROP TABLE raw_data_clone
UNDROP TABLE raw_data_clone
