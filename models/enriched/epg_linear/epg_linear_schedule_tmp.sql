--table for EPG linear schedule in viewership
--Epic: https://agile.at.sky/browse/DPAEP-653
--DM: Simone Villarà
--PRIMARY KEYS:
------broadcasting_management_snapshot.catalogue_linear_daily: service_key
------skyita-da-composer-prod.ovp_elementary_data.transcode_genre: id_genre, id_subgenre
with

Linear_Schedule_get_last_partition AS (
SELECT service_key,
        date_event_display,
        event_duration,
        id_content_universal,
        id_series_universal,
        id_season_universal,
        marketing_message,
        id_event,
        cod_genre,
        cod_sub_genre,
        id_title_event,
        TITLE,
        DES_SHORT_CONTENT,
        file_name
--FROM `broadcasting_management_snapshot.catalogue_linear_daily`
  FROM {{ ref('catalogue_linear_daily_tmp') }}
  WHERE DATE(partition_date) = "{{ var("ingestion_date") }}"),

Linear_Schedule_TempView1 AS (
SELECT DISTINCT
       CAST (SERVICE_KEY AS INT64) AS SERVICE_KEY,
       DATETIME(CAST(date_event_display AS TIMESTAMP),"Europe/Rome") AS SLOT_START_TIME,
       DATETIME_ADD(DATETIME(CAST(date_event_display AS TIMESTAMP),"Europe/Rome"), 
       INTERVAL
            CAST(SUBSTR(event_duration,1,2) AS INT64)*3600+
            CAST(SUBSTR(event_duration,4,2) AS INT64)*60+
            CAST(SUBSTR(event_duration,7,2) AS INT64)
       SECOND) AS SLOT_END_TIME,
       IF(id_content_universal="#", "", id_content_universal) AS UUID,id_series_universal,id_season_universal,marketing_message,
       id_event AS ID_EVENT,cod_genre,cod_sub_genre,id_title_event,TITLE,DES_SHORT_CONTENT,file_name
FROM Linear_Schedule_get_last_partition
WHERE NOT STARTS_WITH(event_duration,"-")
),

Linear_Schedule_TempView2 AS (
SELECT A.SERVICE_KEY, A.SLOT_START_TIME, A.SLOT_END_TIME, A.FILE_NAME,
       MAX(UUID) AS UUID, MAX(ID_EVENT) AS ID_EVENT, MAX(id_series_universal) AS id_series_universal,MAX(id_season_universal) AS id_season_universal,MAX(marketing_message) AS marketing_message,
       MAX(cod_genre) AS cod_genre, MAX(cod_sub_genre) AS cod_sub_genre,MAX(id_title_event) AS id_title_event,MAX(TITLE) AS TITLE, MAX(DES_SHORT_CONTENT) AS DES_SHORT_CONTENT
FROM Linear_Schedule_TempView1 AS A
    INNER JOIN (
        SELECT SERVICE_KEY, SLOT_START_TIME, SLOT_END_TIME, MAX(file_name) AS MAX_FILE_NAME
        FROM Linear_Schedule_TempView1
        GROUP BY SERVICE_KEY, SLOT_START_TIME, SLOT_END_TIME
    ) AS B
    ON A.SERVICE_KEY=B.SERVICE_KEY AND A.SLOT_START_TIME=B.SLOT_START_TIME AND A.SLOT_END_TIME=B.SLOT_END_TIME AND A.file_name=B.MAX_FILE_NAME
GROUP BY SERVICE_KEY, SLOT_START_TIME, SLOT_END_TIME, FILE_NAME),

Linear_Schedule_TempView6 AS (
SELECT *,
       DATETIME_TRUNC(DATETIME_SUB(SLOT_START_TIME, INTERVAL 2 HOUR), DAY) AS VIEWING_START_DATE,
       DATETIME_TRUNC(DATETIME_SUB(SLOT_END_TIME, INTERVAL 7201 SECOND), DAY) AS VIEWING_END_DATE,
FROM (SELECT SERVICE_KEY, SLOT_START_TIME,
       IFNULL(
         LEAST(SLOT_END_TIME, LEAD(SLOT_START_TIME,1) OVER (PARTITION BY SERVICE_KEY ORDER BY SLOT_START_TIME, SLOT_END_TIME)),
         SLOT_END_TIME) AS SLOT_END_TIME,
       UUID, ID_EVENT,id_series_universal,id_season_universal,marketing_message,cod_genre,cod_sub_genre,id_title_event,TITLE,DES_SHORT_CONTENT,
FROM Linear_Schedule_TempView2)),

linear AS (
SELECT *
FROM(
    SELECT *, DATETIME_DIFF(SLOT_END_TIME, SLOT_START_TIME, SECOND) AS SLOT_DURATION_SEC,
    FROM (
        SELECT SERVICE_KEY,
            DATE(VIEWING_START_DATE) AS VIEWING_DATE,
            SLOT_START_TIME,
            LEAST(SLOT_END_TIME, DATETIME_ADD(VIEWING_START_DATE, INTERVAL 26 HOUR)) AS SLOT_END_TIME,
            UUID, ID_EVENT,id_series_universal,id_season_universal,marketing_message,cod_genre,cod_sub_genre,id_title_event,TITLE,DES_SHORT_CONTENT
        FROM Linear_Schedule_TempView6
        UNION ALL
        SELECT SERVICE_KEY,
                DATE(VIEWING_END_DATE) AS VIEWING_DATE,
                DATETIME_ADD(VIEWING_END_DATE, INTERVAL 2 HOUR) AS SLOT_START_TIME,
                SLOT_END_TIME, UUID, ID_EVENT,id_series_universal,id_season_universal,marketing_message,cod_genre,cod_sub_genre,id_title_event,TITLE,DES_SHORT_CONTENT
        FROM Linear_Schedule_TempView6
        WHERE VIEWING_END_DATE>VIEWING_START_DATE
   )
)
WHERE SLOT_DURATION_SEC>0 ),

genres as (SELECT id_genre, id_subgenre,genre, sub_genre
--FROM `skyita-da-composer-prod.ovp_elementary_data.transcode_genre`),
  FROM {{ source('skyita-da-composer-prod_ovp_elementary_data','transcode_genre') }} ),
final_schedule AS(
SELECT
id_event,
CAST(service_key as INTEGER) as service_key,
VIEWING_DATE AS date_schedule,
linear.SLOT_START_TIME AS date_time_event_start,
IFNULL(greatest(SLOT_END_TIME, LEAD(SLOT_START_TIME,1) OVER (PARTITION BY SERVICE_KEY ORDER BY SLOT_START_TIME, SLOT_END_TIME)),SLOT_END_TIME) AS date_time_event_end,
linear.SLOT_DURATION_SEC AS event_duration_sec,
IF(linear.cod_genre>="1" AND linear.cod_genre<="7", genre, "Undefined") AS event_genre,
IF(linear.cod_genre="0" AND linear.cod_sub_genre='31', "Intrattenimento",IF(genre IS NULL OR genre ="" OR sub_genre IS NULL OR sub_genre ="" OR (linear.cod_genre="0" AND linear.cod_sub_genre<>'31'), "Undefined", sub_genre)) AS event_sub_genre,
uuid,
ID_TITLE_EVENT AS id_title,
title,
des_short_content,
marketing_message
FROM linear LEFT JOIN genres AS G ON linear.cod_genre=G.id_genre AND linear.cod_sub_genre=G.id_subgenre
)

SELECT *, CURRENT_TIMESTAMP() AS snapshot_time,
       TIMESTAMP("{{ var("ingestion_date") }}") AS partition_date,
       EXTRACT(HOUR FROM TIMESTAMP("{{ var("ingestion_date") }}")) AS _clustering_hour,
       EXTRACT(MINUTE FROM TIMESTAMP("{{ var("ingestion_date") }}")) AS _clustering_minute
FROM final_schedule