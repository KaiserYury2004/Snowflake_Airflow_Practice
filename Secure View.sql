-- Создание таблицы пользователей с уровнем доступа
CREATE OR REPLACE TABLE user_access (
    user_name STRING,
    access_level STRING
);
INSERT INTO user_access VALUES ('user_a', 'FULL_ACCESS');
INSERT INTO user_access VALUES ('user_b', 'RESTRICTED_ACCESS');
-- Создание представления с учетом доступа пользователей
CREATE OR REPLACE SECURE VIEW secure_view AS
SELECT *
FROM datamart_data
WHERE
  (SELECT access_level FROM user_access WHERE user_name = CURRENT_USER()) = 'FULL_ACCESS'
  OR
  (SELECT access_level FROM user_access WHERE user_name = CURRENT_USER()) = 'RESTRICTED_ACCESS';
