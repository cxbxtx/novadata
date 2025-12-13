--============================================--
--      4.3 Задание. Работа с Clickhouse
--============================================--

--1 Создаем таблицу для сырых данных
CREATE TABLE user_events (
  user_id       UInt32
, event_type    String
, points_spent  UInt32
, event_time    DateTime
) 
ENGINE = MergeTree()
ORDER BY (
  event_time
, user_id
)
TTL event_time + INTERVAL 30 DAY
SETTINGS index_granularity = 8192
;

--2 Создаем таблицу для агрегированных данных
CREATE TABLE user_events_agg (
  event_day         Date
, event_type        String
, user_uniq         AggregateFunction(uniq, UInt32)
, points_spent_sum  AggregateFunction(sum, UInt32)
, event_type_cnt    AggregateFunction(count, UInt32)
)
ENGINE = AggregatingMergeTree()
ORDER BY (
  event_day
, event_type
)
TTL event_day + INTERVAL 180 DAY
SETTINGS index_granularity = 8192
;

--3 Создаем материализ. представление
CREATE MATERIALIZED VIEW user_events_mv 
TO user_events_agg
AS 
SELECT
  toStartOfDay(event_time)  AS event_day
, event_type
, uniqState(user_id)        AS user_uniq
, sumState(points_spent)    AS points_spent_sum
, countState(event_type)    AS event_type_cnt
FROM user_events
GROUP BY 
  event_day
, event_type
;

--4 Наполняем user_events данными
INSERT INTO user_events VALUES
-- События 10 дней назад
(1, 'login', 0, now() - INTERVAL 10 DAY),
(2, 'signup', 0, now() - INTERVAL 10 DAY),
(3, 'login', 0, now() - INTERVAL 10 DAY),
;

INSERT INTO user_events VALUES
-- События 7 дней назад
(1, 'login', 0, now() - INTERVAL 7 DAY),
(2, 'login', 0, now() - INTERVAL 7 DAY),
(3, 'purchase', 30, now() - INTERVAL 7 DAY),
;

INSERT INTO user_events VALUES
-- События 5 дней назад
(1, 'purchase', 50, now() - INTERVAL 5 DAY),
(2, 'logout', 0, now() - INTERVAL 5 DAY),
(4, 'login', 0, now() - INTERVAL 5 DAY),
;

INSERT INTO user_events VALUES
-- События 3 дня назад
(1, 'login', 0, now() - INTERVAL 3 DAY),
(3, 'purchase', 70, now() - INTERVAL 3 DAY),
(5, 'signup', 0, now() - INTERVAL 3 DAY),
;

INSERT INTO user_events VALUES
-- События вчера
(2, 'purchase', 20, now() - INTERVAL 1 DAY),
(4, 'logout', 0, now() - INTERVAL 1 DAY),
(5, 'login', 0, now() - INTERVAL 1 DAY),
;

INSERT INTO user_events VALUES
-- События сегодня
(1, 'purchase', 25, now()),
(2, 'login', 0, now()),
(3, 'logout', 0, now()),
(6, 'signup', 0, now()),
(6, 'purchase', 100, now())
;

--5 Расчёт Retention
--retention_7d_percent = (returned_in_7_days / total_users_day_0) * 100%,
--Где:
--total_users_day_0 — общее количество пользователей, зарегистрировавшиеся (signup) в день 0 (7 дней назад)
--returned_in_7_days — количество пользователей из day_0, которые совершили хотя бы одно действие в течение последующих 7 дней
--retention_7d_percent — процент удержания

--когорта пользователей, зарегистрировавшихся 7 дней назад
WITH cohort AS (
SELECT DISTINCT user_id
FROM user_events
WHERE 1=1
  AND event_type = 'signup'
  AND toStartOfDay(event_time) = CURRENT_DATE() - INTERVAL 7 DAY
),
--активные пользователи из этой когорты в последующие 7 дней
returned_users AS (
SELECT DISTINCT user_id
FROM user_events
JOIN cohort AS coho USING(user_id)
WHERE toStartOfDay(event_time) > CURRENT_DATE() - INTERVAL 7 DAY
)
--расчёт метрик
SELECT
    (SELECT COUNT() FROM cohort)          AS total_users_day_0
  , (SELECT COUNT() FROM returned_users)  AS returned_in_7_days
  , format('{}%', 
      ROUND(
        (SELECT COUNT(*) FROM returned_users) * 100.0 / 
        (SELECT COUNT(*) FROM cohort),
      2
      )
    )                                     AS retention_7d_percent
;

--6 Запрос с группировками по быстрой аналитике по дням
SELECT
  event_day
, event_type
, uniqMerge(user_uniq)        AS unique_users
, sumMerge(points_spent_sum)  AS total_spent
, countMerge(event_type_cnt)  AS total_actions
FROM user_events_agg
GROUP BY 
  event_day
, event_type
ORDER BY 
  event_day
, event_type
;
