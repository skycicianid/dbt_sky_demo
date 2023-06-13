WITH
catalogue_linear AS (
  SELECT
    service_key,
    service_type,
    id_event,
    id_title_event,
    id_series,
    id_event_vod,
    title,
    des_short_content,
    des_long_content,
    marketing_message,
    id_parental_rating,
    event_duration,
    cod_genre,
    cod_sub_genre,
    audio_type,
    flg_subtitles_bool,
    flg_subtitles,
    time_start_event,
    date_event,
    date_event_display,
    event_duration_display,
    flg_widescreen_bool,
    flg_widescreen,
    flg_add_languages_bool,
    flg_add_languages,
    flg_new_event_bool,
    flg_new_event,
    id_event_vod_iptv,
    id_version,
    uid_svod_stb,
    uid_svod_iptv,
    title_key,
    seca_date,
    id_scrambling_type,
    flg_link_bool,
    flg_link,
    id_content_universal,
    id_season_universal,
    id_series_universal,
    episode_num,
    series_title,
    episode_title,
    flg_iptv_blackout_bool,
    flg_iptv_blackout,
    id_programme,
    _process_mapping_start_ts,
    _process_transaction_id ,
    _process_mapping_version ,
    _ingestion_file_name,
    COALESCE(_ingestion_file_name, file_name) as file_name, --needed because starting from DPAP-4767 file name is in technical field _ingestion_file_name
    partition_date
  FROM
    --`broadcasting_management.catalogue_linear`
    {{ source('skyita-da-daita-prod_broadcasting_management','catalogue_linear') }}
),
catalogue_linear_lastyear AS (
  select 
    service_key, 
    file_name, 
    min(date_event_display) as start_date, --get for each day the maximum file for non overlapping series
    max(TIMESTAMP_ADD(date_event_display, INTERVAL
    ( cast (split(event_duration, ':')[offset(0)] as INT64) * 3600) + ( cast (split(event_duration, ':')[offset(1)] as INT64) * 60) + ( cast (split(event_duration, ':')[offset(2)] as INT64))
    SECOND) )as end_date,
    substr(file_name, 5, 8) as file_date
  from 
    catalogue_linear
  where 
    partition_date >= TIMESTAMP_SUB(TIMESTAMP(DATE("{{ var("ingestion_date") }}"), 'Europe/Rome'), INTERVAL 365 DAY)
  group by 
    service_key, 
    file_name, 
    substr(file_name, 5, 8)
),
catalogue_versions as (  --each file name is composed by [1-4] channel_name, [5-12] file_date, [13] file_version
  select 
    max(file_name) as file_name, 
    service_key, 
    start_date, 
    end_date,
    file_date 
  FROM 
    catalogue_linear_lastyear
  group by 
    service_key, 
    start_date, 
    end_date, 
    file_date
),
catalogue_dates as (
   select distinct *
   from (
        select
            distinct service_key, start_date as date, a.file_date
        from catalogue_versions as a
     union all
        select
            distinct service_key, end_date as date, a.file_date
        from catalogue_versions as a
        )
   ),
ordered_dates as ( --compute file-series over day
    select date, service_key, file_date,
        row_number() over (partition by service_key, file_date order by date asc) as idx
    from catalogue_dates
   ),
timeseries as (
     select a.service_key, a.date as start_date, b.date as end_date, a.file_date as file_date
     from ordered_dates as a,
        ordered_dates as b
     where a.idx = (b.idx-1)
     and a.service_key = b.service_key
     and a.file_date = b.file_date
   ),
versions as ( --get for each time range, for each file that match, the maximum one
    select
      max(file_name) as version, service_key, file_date, start_date, end_date,
      row_number() over (PARTITION BY service_key order by start_date, file_date asc) as num_row
    from (
        select
          file_name, a.service_key, b.start_date, b.end_date, a.file_date
        from catalogue_versions as a,
          timeseries as b
        where  b.start_date >= a.start_date
        and b.end_date <= a.end_date
        and a.service_key = b.service_key
        and a.file_date = b.file_date
        )
    group by service_key, file_date, start_date, end_date
  ),
versioned_catalogue as ( --remove overlapping-day
  select distinct a.* from
    versions as a
    left join  versions as b
    on a.service_key = b.service_key
      and a.num_row = (b.num_row-1)
  where
  b.file_date is null
  or  b.file_date >= a.file_date
),
updated_catalogue as (
    select a.*
   	from 
      catalogue_linear as a,
   		versioned_catalogue as b, 
      versioned_catalogue as c
    where a.partition_date >= TIMESTAMP_SUB(TIMESTAMP(DATE("{{ var("ingestion_date") }}"), 'Europe/Rome'), INTERVAL 365 DAY)
   	and a.service_key  = b.service_key
   	and a.file_name  = b.version
    and a.file_name = c.version
    and a.service_key = c.service_key
   	and a.date_event_display  >= b.start_date
    and a.date_event_display <= b.end_date
   	and TIMESTAMP_ADD(a.date_event_display, INTERVAL
   	 ( cast (split(event_duration, ':')[offset(0)] as INT64) * 3600) + ( cast (split(event_duration, ':')[offset(1)] as INT64) * 60) + ( cast (split(event_duration, ':')[offset(2)] as INT64))
   	 SECOND) <= c.end_date
   	and TIMESTAMP_ADD(a.date_event_display, INTERVAL
   	 ( cast (split(event_duration, ':')[offset(0)] as INT64) * 3600) + ( cast (split(event_duration, ':')[offset(1)] as INT64) * 60) + ( cast (split(event_duration, ':')[offset(2)] as INT64))
   	 SECOND) >= c.start_date
)
select *,
     TIMESTAMP("{{ var("ingestion_date") }}") as partition_date,
     extract(hour from TIMESTAMP("{{ var("ingestion_date") }}")) as _clustering_hour,
     extract(minute from TIMESTAMP("{{ var("ingestion_date") }}")) as _clustering_minute,
     current_timestamp() as snapshot_time
from (
    select
      a.service_key,
      a.service_type,
      a.id_event,
      a.id_title_event,
      a.id_series,
      a.id_event_vod,
      a.title,
      a.des_short_content,
      a.des_long_content,
      a.marketing_message,
      a.id_parental_rating,
      a.event_duration,
      a.cod_genre,
      a.cod_sub_genre,
      a.audio_type,
      a.flg_subtitles_bool,
      a.flg_subtitles,
      a.time_start_event,
      a.date_event,
      a.date_event_display,
      a.event_duration_display,
      a.flg_widescreen_bool,
      a.flg_widescreen,
      a.flg_add_languages_bool,
      a.flg_add_languages,
      a.flg_new_event_bool,
      a.flg_new_event,
      a.id_event_vod_iptv,
      a.id_version,
      a.uid_svod_stb,
      a.uid_svod_iptv,
      a.title_key,
      a.seca_date,
      a.id_scrambling_type,
      a.flg_link_bool,
      a.flg_link,
      a.id_content_universal,
      a.id_season_universal,
      a.id_series_universal,
      a.episode_num,
      a.series_title,
      a.episode_title,
      a.flg_iptv_blackout_bool,
      a.flg_iptv_blackout,
      a.id_programme,
      a._process_mapping_start_ts,
      a._process_transaction_id ,
      a._process_mapping_version ,
      a.file_name,
    from updated_catalogue a
   union all
    select *  except(partition_date, snapshot_time, _clustering_hour, _clustering_minute),
      -- from broadcasting_management_snapshot.catalogue_linear_daily a
      from {{ source('skyita-da-daita-prod_broadcasting_management_snapshot', 'catalogue_linear_daily') }} a
    WHERE DATE(partition_date) = DATE_SUB(DATE("{{ var("ingestion_date") }}"), INTERVAL 1 DAY)
        and TIMESTAMP_TRUNC(date_event_display,DAY) < TIMESTAMP_SUB(TIMESTAMP(DATE("{{ var("ingestion_date") }}"), 'Europe/Rome'), INTERVAL 365 DAY)
)
