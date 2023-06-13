/*
  Description: The structure is dedicated to the navigation of the IVR Sky "Quarto Referente di Fulfillment" service.
               The table has call level granularity and cardinality corresponding to the number of calls to the "Quarto Referente di Fulfillment" IVR service.
  PKs: ivr_navigation.id_conn
  DM: Paolo Basti
  Epics: https://agile.at.sky/browse/DPAEP-813
  Docs: https://wiki.at.sky/pages/viewpage.action?spaceKey=DPAB&title=Quarto+Referente+per+Fulfilment
*/
WITH
new_ivr_data AS (
    SELECT id_conn,
           date_created_ts,
           date_start_ts,
           tech_var_1,
    FROM 
        -- cti.ivr_navigation
        {{ source('skyita-da-daita-prod_cti','ivr_navigation') }}
    WHERE _process_mapping_start_ts >= TIMESTAMP('2023-06-09 22:00:00+00') AND
          _process_mapping_start_ts < TIMESTAMP_ADD(TIMESTAMP("{{ var("ingestion_date") }}",'Europe/Rome'), INTERVAL 1 DAY) AND
          IFNULL(id_conn,'')<>'' AND
          UPPER(TRIM(level_1)) = 'INSTALLER' AND
          UPPER(TRIM(level_2)) = 'MENU_INST' AND
          UPPER(TRIM(level_3)) = 'ORD' AND
          IFNULL(level_4, '') = '' AND
          IFNULL(level_5, '') = ''
),

order_access_to_sf AS (
    SELECT id_order_primary_access,
           id_order_sf,
           MIN(osf.date_created_ts) AS date_created_ts
    FROM
        -- order_management.order_salesforce osf
        {{ source('skyita-da-daita-prod_order_management','order_salesforce') }} osf
    WHERE IFNULL(id_order_primary_access, '') <> ''
    GROUP BY id_order_primary_access,id_order_sf
),

order_access_to_tibco AS (
    SELECT distinct id_order_access,
           id_order_t
    FROM 
        -- operational_support.oss_orders oss
        {{ source('skyita-da-daita-prod_operational_support','oss_orders') }} oss

    INNER JOIN bb_coverage.tracking_of trk ON oss.cod_order = trk.id_technical_order_tibco
    WHERE UPPER(TRIM(service_type)) = 'C_ACCESS'
          QUALIFY ROW_NUMBER() OVER (PARTITION BY id_order_access ORDER BY trk.date_creation DESC) = 1 -- Needed to get around abnormal duplicates
),

que_tracking_data AS (
    SELECT id_conn,
           MAX(id_case) AS id_case
    FROM 
    -- cti.cti_que_tracking que
        {{ source('skyita-da-daita-prod_cti','cti_que_tracking') }} que

    GROUP BY id_conn
),

last_snapshot_agenda_management AS (
    SELECT id,
           id_order,
           datetime_appointment_ts
    -- FROM agenda_snapshot.agenda_management_daily agm
    {{ ref('agenda_management_daily') }}
    WHERE date(partition_date) = date("{{ var("ingestion_date") }}")
),

new_data AS (
    SELECT ivr.id_conn,
           NULLIF(ivr.tech_var_1, '') AS id_order_access,
           tib.id_order_t AS id_order_tibco,
           que.id_case AS id_case_4th_contact_point,
           agm.id AS id_appointment
    FROM new_ivr_data ivr
    LEFT JOIN que_tracking_data que ON ivr.id_conn = que.id_conn
    LEFT JOIN order_access_to_sf osf ON ivr.tech_var_1 = osf.id_order_primary_access
    LEFT JOIN order_access_to_tibco tib ON ivr.tech_var_1 = tib.id_order_t
    LEFT JOIN last_snapshot_agenda_management agm ON osf.id_order_sf = agm.id_order
            QUALIFY ROW_NUMBER() OVER (PARTITION BY ivr.id_conn
                                       ORDER BY ABS(TIMESTAMP_DIFF(ivr.date_start_ts, agm.datetime_appointment_ts, SECOND)) ASC) = 1
),

last_query_result AS (
    SELECT id_conn,
           id_order_access,
           id_order_tibco,
           id_appointment,
           id_case_4th_contact_point
    FROM 
    -- `cti_enriched.ivr_4th_contact_point_kpis_daily`
    {{ source('skyita-da-daita-prod_cti_enriched','ivr_4th_contact_point_kpis_daily') }}

    WHERE DATE(partition_date) = DATE('2023-06-09 22:00:00+00')
)

SELECT if(n.id_conn is NULL, o.id_conn, IFNULL(n.id_conn, o.id_conn)) AS id_conn,
       if(n.id_conn is NULL, o.id_order_access, IFNULL(n.id_order_access, o.id_order_access)) AS id_order_access,
       if(n.id_conn is NULL, o.id_order_tibco, IFNULL(n.id_order_tibco, o.id_order_tibco)) AS id_order_tibco,
       if(n.id_conn is NULL, o.id_case_4th_contact_point, IFNULL(n.id_case_4th_contact_point, o.id_case_4th_contact_point)) AS id_case_4th_contact_point,
       if(n.id_conn is NULL, o.id_appointment, IFNULL(n.id_appointment, o.id_appointment)) AS id_appointment,

       CURRENT_TIMESTAMP()  AS snapshot_time,
       TIMESTAMP_TRUNC(TIMESTAMP("{{ var("ingestion_date") }}"), day) AS partition_date,
       EXTRACT(HOUR FROM TIMESTAMP("{{ var("ingestion_date") }}")) AS _clustering_hour,
       EXTRACT(MINUTE FROM TIMESTAMP("{{ var("ingestion_date") }}")) AS _clustering_minute,
FROM new_data n
FULL OUTER JOIN last_query_result o ON n.id_conn = o.id_conn