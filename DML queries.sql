--Событие,id которого указано - очистка таблицы
SELECT * FROM raw_data BEFORE(STATEMENT => '8e5d0ca9-005e-44e6-b858-a8f5b37c5726');
--
SELECT * FROM raw_data AT(TIMESTAMP => 'Mon, 15 Jul 2024 11:20:00 -0700'::timestamp_tz);
