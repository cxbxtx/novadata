--0 Создание схемы dev
CREATE SCHEMA dev;


--1 Создание основной таблицы dev.users
CREATE TABLE dev.users (
  id          INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY
, name        TEXT
, email       TEXT
, role        TEXT
, updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
;


--2 Создание таблицы для отслеживания изменений dev.users_audit
CREATE TABLE dev.users_audit (
  id              INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY
, user_id         INTEGER
, changed_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
, changed_by      TEXT DEFAULT CURRENT_USER
, field_changed   TEXT
, old_value       TEXT
, new_value       TEXT
)
;


--3 Вставка данных в dev.users
INSERT INTO dev.users (name, email, role)
VALUES 
('Ivan Ivanov', 'ivan@example.com', 'viewer'),
('Anna Petrova', 'anna@example.com', 'creator'),
('Fedor Makarov', 'fedor@example.com', 'explorer')
;

INSERT INTO dev.users (name, email, role)
VALUES 
('Anton Smirnov', 'anton@example.com', 'viewer')
;


--4 Установка pg_cron
CREATE EXTENSION pg_cron;


--5 Создание функции-триггера для вставки изменений в dev.users_audit
CREATE OR REPLACE FUNCTION dev.func_user_upd()
RETURNS TRIGGER AS $$
BEGIN
  --если меняется колонка name
  IF OLD.name IS DISTINCT FROM NEW.name THEN
    INSERT INTO dev.users_audit(user_id, field_changed, old_value, new_value)
    VALUES (OLD.id, 'name', OLD.name, NEW.name);
  END IF;
  --если меняется колонка email
  IF OLD.email IS DISTINCT FROM NEW.email THEN
    INSERT INTO dev.users_audit(user_id, field_changed, old_value, new_value)
    VALUES (OLD.id, 'email', OLD.email, NEW.email);
  END IF;
  --если меняется колонка role
  IF OLD.role IS DISTINCT FROM NEW.role THEN
    INSERT INTO dev.users_audit(user_id, field_changed, old_value, new_value)
    VALUES (OLD.id, 'role', OLD.role, NEW.role);
  END IF;

  RETURN NEW;	
END;
$$ LANGUAGE plpgsql
;


--6 Создание триггера для отследивания изменений в dev.users
CREATE TRIGGER trg_user_upd BEFORE
UPDATE ON dev.users
FOR EACH ROW EXECUTE FUNCTION func_user_upd()
;


--7 Апдейт данных в dev.users
UPDATE dev.users
SET name = 'Anna Ivanova'
WHERE name = 'Anna Petrova'
;

UPDATE dev.users
SET email = 'ivan@primer.com'
WHERE id = 1
;

UPDATE dev.users
SET email = 'fedor@ex.com'
WHERE id = 3
;

DELETE FROM dev.users
WHERE id = 5
;

UPDATE dev.users
SET role = 'admin'
WHERE id = 4
;


--8 Создание функции для экспорта апдейтов по dev.users_audit
CREATE OR REPLACE FUNCTION dev.func_users_audit_export_daily()
RETURNS TEXT AS $$
DECLARE
    export_path TEXT := '/tmp/users_audit_export_' || TO_CHAR(CURRENT_DATE, 'YYYYMMDD') || '.csv';
    export_count INTEGER;
BEGIN
    EXECUTE format('
        COPY (
            SELECT *
            FROM dev.users_audit
            WHERE 1=1
              AND changed_at >= DATE_TRUNC(''day'', CURRENT_TIMESTAMP) - INTERVAL''1 day''
              AND changed_at <  DATE_TRUNC(''day'', CURRENT_TIMESTAMP)
        ) TO %L WITH CSV HEADER',
        export_path
    );
    -- Выводим кол-во экспортированных строк
    GET DIAGNOSTICS export_count = ROW_COUNT;
    RETURN 'Экспортировано ' || export_count || ' записи(ей) в файл: ' || export_path;
    
EXCEPTION WHEN OTHERS THEN
    RETURN 'Ошибка экспорта: ' || SQLERRM;    
END;
$$ LANGUAGE plpgsql
;


--9 Ставим функцию экспорта на расписание
SELECT cron.schedule('0 3 * * *', 'SELECT dev.func_users_audit_export_daily()');

--10 Проверка расписания
SELECT * FROM cron.job;
