WITH snap_new_basic AS (
	SELECT * EXCEPT (rn) 
	FROM (
		SELECT 
			cod_istat,
      			date_slot_appointment,
      			time_slot_start,
      			time_slot_end,
      			slot_number,
      			_process_mapping_start_ts,
      			_process_transaction_id,
      			_process_mapping_version,
      			ROW_NUMBER() OVER (PARTITION BY cod_istat, date_slot_appointment, time_slot_start ORDER BY _process_mapping_start_ts DESC) AS rn
		FROM 
            -- agenda_management.agenda_management_forecast
            {{ source('skyita-da-daita-prod_agenda_management','agenda_management_forecast') }}
		WHERE _process_mapping_start_ts >= TIMESTAMP_SUB(TIMESTAMP('2023-06-09 22:00:00+00'), INTERVAL 60 MINUTE)
        and _process_mapping_start_ts < TIMESTAMP(date_add(DATE("{{ var("ingestion_date") }}"), INTERVAL 1 DAY), 'Europe/Rome')
        and date(partition_date) between DATE(TIMESTAMP_SUB(TIMESTAMP('2023-06-09 22:00:00+00'), INTERVAL 60 MINUTE)) and DATE("{{ var("ingestion_date") }}")
	) WHERE rn = 1
),

last_query_result as
	(select * from 
    -- `agenda_snapshot.agenda_management_forecast_daily`
    {{ source('skyita-da-daita-prod_agenda_snapshot','agenda_management_forecast_daily') }}

     where DATE(partition_date) = DATE('2023-06-09 22:00:00+00'))

select
	current_timestamp() as snapshot_time,
	TIMESTAMP("{{ var("ingestion_date") }}") as partition_date,
	extract(hour from TIMESTAMP("{{ var("ingestion_date") }}")) as _clustering_hour,
	extract(minute from TIMESTAMP("{{ var("ingestion_date") }}")) as _clustering_minute,

	if(n.cod_istat is null, lim.cod_istat, if(lim._process_mapping_start_ts is null or n._process_mapping_start_ts > lim._process_mapping_start_ts, n.cod_istat, lim.cod_istat)) as cod_istat,
	if(n.date_slot_appointment is null, lim.date_slot_appointment, if(lim._process_mapping_start_ts is null or n._process_mapping_start_ts > lim._process_mapping_start_ts, n.date_slot_appointment, lim.date_slot_appointment)) as date_slot_appointment,
	if(n.time_slot_start is null, lim.time_slot_start, if(lim._process_mapping_start_ts is null or n._process_mapping_start_ts > lim._process_mapping_start_ts, n.time_slot_start, lim.time_slot_start)) as time_slot_start,
	if(n.time_slot_end is null, lim.time_slot_end, if(lim._process_mapping_start_ts is null or n._process_mapping_start_ts > lim._process_mapping_start_ts, n.time_slot_end, lim.time_slot_end)) as time_slot_end,
	if(n.slot_number is null, lim.slot_number, if(lim._process_mapping_start_ts is null or n._process_mapping_start_ts > lim._process_mapping_start_ts, n.slot_number, lim.slot_number)) as slot_number,
	if(n._process_mapping_start_ts is null, lim._process_mapping_start_ts, if(lim._process_mapping_start_ts is null or n._process_mapping_start_ts > lim._process_mapping_start_ts, n._process_mapping_start_ts, lim._process_mapping_start_ts)) as _process_mapping_start_ts,
	if(n._process_transaction_id is null, lim._process_transaction_id, if(lim._process_mapping_start_ts is null or n._process_mapping_start_ts > lim._process_mapping_start_ts, n._process_transaction_id, lim._process_transaction_id)) as _process_transaction_id,
	if(n._process_mapping_version is null, lim._process_mapping_version, if(lim._process_mapping_start_ts is null or n._process_mapping_start_ts > lim._process_mapping_start_ts, n._process_mapping_version, lim._process_mapping_version)) as _process_mapping_version
	from
	snap_new_basic n full outer join last_query_result lim on (n.cod_istat = lim.cod_istat and
  n.date_slot_appointment = lim.date_slot_appointment and
  n.time_slot_start = lim.time_slot_start)