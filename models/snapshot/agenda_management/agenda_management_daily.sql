with
    new_data as
            (

                select *,
                FIRST_VALUE(IF(lower(status) = "aperto", date_modify_ts, NULL) IGNORE NULLS) OVER (PARTITION BY id ORDER BY date_modify_ts) AS date_appointment_opened_ts,
                FIRST_VALUE(IF(lower(status) = "aperto", DATETIME(date_modify_ts, "Europe/Rome"), NULL) IGNORE NULLS) OVER (PARTITION BY id ORDER BY date_modify_ts) AS date_appointment_opened_ltz,
                FIRST_VALUE(IF(lower(status) = "in corso", date_modify_ts, NULL) IGNORE NULLS) OVER (PARTITION BY id ORDER BY date_modify_ts) AS date_appointment_in_progress_ts,
                FIRST_VALUE(IF(lower(status) = "in corso", DATETIME(date_modify_ts, "Europe/Rome"), NULL) IGNORE NULLS) OVER (PARTITION BY id ORDER BY date_modify_ts) AS date_appointment_in_progress_ltz,
                FIRST_VALUE(IF(lower(status) = "annullato", date_modify_ts, NULL) IGNORE NULLS) OVER (PARTITION BY id ORDER BY date_modify_ts) AS date_appointment_cancelled_ts,
                FIRST_VALUE(IF(lower(status) = "annullato", DATETIME(date_modify_ts, "Europe/Rome"), NULL) IGNORE NULLS) OVER (PARTITION BY id ORDER BY date_modify_ts) AS date_appointment_cancelled_ltz,
                FIRST_VALUE(IF(lower(status) = "chiuso", date_modify_ts, NULL) IGNORE NULLS) OVER (PARTITION BY id ORDER BY date_modify_ts) AS date_appointment_closed_ts,
                FIRST_VALUE(IF(lower(status) = "chiuso", DATETIME(date_modify_ts, "Europe/Rome"), NULL) IGNORE NULLS) OVER (PARTITION BY id ORDER BY date_modify_ts) AS date_appointment_closed_ltz,
                FIRST_VALUE(IF(lower(status) = "rischedulato", date_modify_ts, NULL) IGNORE NULLS) OVER (PARTITION BY id ORDER BY date_modify_ts) AS date_appointment_rescheduled_ts,
                FIRST_VALUE(IF(lower(status) = "rischedulato", DATETIME(date_modify_ts, "Europe/Rome"), NULL) IGNORE NULLS) OVER (PARTITION BY id ORDER BY date_modify_ts) AS date_appointment_rescheduled_ltz,


                --from `agenda_management.agenda_management`
                {{ source('skyita-da-daita-prod_agenda_management','agenda_management') }}

                where _process_mapping_start_ts >=  TIMESTAMP_SUB(TIMESTAMP('2023-06-09 22:00:00+00'), INTERVAL 60 MINUTE)
                and _process_mapping_start_ts < TIMESTAMP(date_add(DATE(@"{{ var("ingestion_date") }}"), INTERVAL 1 DAY), 'Europe/Rome')
                and (id is not null and id<>'')

            ),




    snap_new_basic as
        (select
        _process_mapping_start_ts,
        _process_mapping_version,
        _process_transaction_id,
        DATETIME(date_creation_ts,'Europe/Rome') as date_creation_ltz,
        DATETIME(date_modify_appointment_ts,'Europe/Rome') as date_modify_appointment_ltz,
        DATETIME(date_modify_ts,'Europe/Rome') as date_modify_ltz,
        DATETIME(date_old_appointment_ts,'Europe/Rome') as date_old_appointment_ltz,
        DATETIME(date_scheduled_installer_ts,'Europe/Rome') as date_scheduled_installer_ltz,
        DATETIME(datetime_appointment_ts,'Europe/Rome') as datetime_appointment_ltz,
        DATETIME(time_scheduled_installer_ts,'Europe/Rome') as time_scheduled_installer_ltz,
        _datasource_transfer_ts,
        appointment_freezed,
        check_pin_iav,
        date_creation,
        date_creation_ts,
        date_creation_ymd,
        date_creation_ymd_ltz,
        date_last_modified_user,
        date_modify,
        date_modify_appointment,
        date_modify_appointment_ts,
        date_modify_appointment_ymd,
        date_modify_appointment_ymd_ltz,
        date_modify_ts,
        date_modify_ymd,
        date_modify_ymd_ltz,
        date_old_appointment,
        date_old_appointment_ts,
        date_old_appointment_ymd,
        date_old_appointment_ymd_ltz,
        date_scheduled_installer,
        date_scheduled_installer_ts,
        date_scheduled_installer_ymd,
        date_scheduled_installer_ymd_ltz,
        datetime_appointment,
        datetime_appointment_ts,
        datetime_appointment_ymd,
        datetime_appointment_ymd_ltz,
        event_subtype,
        extra_working_hour_slot,
        flg_appointment_confirmed_of,
        flg_appointment_confirmed_of_bool,
        flg_appointment_rescheduling,
        flg_extension_slot,
        flg_multivisit,
        flg_multivisit_bool,
        id,
        id_case,
        id_egon_building,
        id_inserimento,
        id_inserimento_dl,
        id_order,
        id_slot,
        installation_pin,
        installation_type,
        installer_pin_wholesaler,
        number_of_rescheduling,
        order_type,
        scheduling_channel,
        service_type,
        staircase,
        status,
        substatus,
        time_scheduled_installer,
        time_scheduled_installer_ts,
        time_scheduled_installer_ymd,
        time_scheduled_installer_ymd_ltz,
        token_id,
        wholesaler,
        work_order,
        date_appointment_opened_ts,
        DATETIME(date_appointment_opened_ts, "Europe/Rome") AS date_appointment_opened_ltz,
        date_appointment_in_progress_ts,
        DATETIME(date_appointment_in_progress_ts, "Europe/Rome") AS date_appointment_in_progress_ltz,
        date_appointment_cancelled_ts,
        DATETIME(date_appointment_cancelled_ts, "Europe/Rome") AS date_appointment_cancelled_ltz,
        date_appointment_closed_ts,
        DATETIME(date_appointment_closed_ts, "Europe/Rome") AS date_appointment_closed_ltz,
        date_appointment_rescheduled_ts,
        DATETIME(date_appointment_rescheduled_ts, "Europe/Rome") AS date_appointment_rescheduled_ltz,
        id_user_created,
        id_user_last_modified,
        datetime_dad,
        datetime_dad_ts,
        scenario_fulfilment_crm,

        from
            (select
                n.*, ROW_NUMBER() OVER(partition by n.id ORDER by n.date_modify DESC , _process_mapping_start_ts DESC) as rn
            from
                new_data n
            )
        where
            rn = 1),

    first_scheduling_channel_one as (
        select
            id,
            scheduling_channel
        from
            (select
                n.*, ROW_NUMBER() OVER(partition by n.id ORDER by n.date_modify asc) as rn
            from
              -- `agenda_management.agenda_management` n
                {{ source('skyita-da-daita-prod_agenda_management','agenda_management') }} n
            where id is not null and trim(id) != ""
            )
        where
            rn = 1),

    last_query_result as (
      select * 
      from
      --  `agenda_snapshot.agenda_management_daily`
          {{ source('skyita-da-daita-prod_agenda_snapshot','agenda_management_daily') }}

          where DATE(partition_date) = DATE('2023-06-09 22:00:00+00')
     ),

    union_status_history as (
    select id,
            status,
            substatus,
            date_modify,
            date_modify_ts,
            DATETIME(date_modify_ts, "Europe/Rome") AS date_modify_ltz,
    from new_data
    union all
    select id,
            status,
            substatus,
            date_modify,
            date_modify_ts,
            date_modify_ltz
    from last_query_result
    ),

    status_history as (
    select *
    from (
        select *,
            ROW_NUMBER() OVER (PARTITION BY id ORDER BY date_modify desc) AS rn
        from (
        select id,
                status,
                date_modify,
                date_modify_ts as date_current_status_ts,
                date_modify_ltz as date_current_status_ltz,
                LAG(status,1) OVER (PARTITION BY id ORDER BY date_modify) AS previous_status,
                LAG(date_modify_ts,1) OVER (PARTITION BY id ORDER BY date_modify) AS date_previous_status_ts,
                LAG(date_modify_ltz,1) OVER (PARTITION BY id ORDER BY date_modify) AS date_previous_status_ltz,


        from union_status_history
        )
        where status <> coalesce(previous_status,'')
    )
    where rn = 1
    ),

    substatus_history as (
    select *
    from (
        select *,
            ROW_NUMBER() OVER (PARTITION BY id ORDER BY date_modify desc) AS rn
        from (
        select id,
                substatus,
                date_modify,
                date_modify_ts as date_current_substatus_ts,
                date_modify_ltz as date_current_substatus_ltz,
                LAG(substatus,1) OVER (PARTITION BY id ORDER BY date_modify) AS previous_substatus,
                LAG(date_modify_ts,1) OVER (PARTITION BY id ORDER BY date_modify) AS date_previous_substatus_ts,
                LAG(date_modify_ltz,1) OVER (PARTITION BY id ORDER BY date_modify) AS date_previous_substatus_ltz
        from union_status_history
        )
        where substatus <> coalesce(previous_substatus,'')
    )
    where rn = 1
    ),

    snap_new as (
        select
        snap_new_basic.*,
        status_history.date_current_status_ts,
        status_history.date_current_status_ltz,
        status_history.previous_status,
        status_history.date_previous_status_ts,
        status_history.date_previous_status_ltz,
        substatus_history.date_current_substatus_ts,
        substatus_history.date_current_substatus_ltz,
        substatus_history.previous_substatus,
        substatus_history.date_previous_substatus_ts,
        substatus_history.date_previous_substatus_ltz,
        from snap_new_basic
        left join status_history on snap_new_basic.id = status_history.id
        left join substatus_history on snap_new_basic.id = substatus_history.id

    ),

    new_snapshot as (
    select
        if(n.id is null, lim._process_mapping_start_ts, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n._process_mapping_start_ts, lim._process_mapping_start_ts)) as _process_mapping_start_ts,
        if(n.id is null, lim._process_mapping_version, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n._process_mapping_version, lim._process_mapping_version)) as _process_mapping_version,
        if(n.id is null, lim._process_transaction_id, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n._process_transaction_id, lim._process_transaction_id)) as _process_transaction_id,
        if(n.id is null, lim.date_creation_ltz, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.date_creation_ltz, lim.date_creation_ltz)) as date_creation_ltz,
        if(n.id is null, lim.date_modify_appointment_ltz, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.date_modify_appointment_ltz, lim.date_modify_appointment_ltz)) as date_modify_appointment_ltz,
        if(n.id is null, lim.date_modify_ltz, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.date_modify_ltz, lim.date_modify_ltz)) as date_modify_ltz,
        if(n.id is null, lim.date_old_appointment_ltz, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.date_old_appointment_ltz, lim.date_old_appointment_ltz)) as date_old_appointment_ltz,
        if(n.id is null, lim.date_scheduled_installer_ltz, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.date_scheduled_installer_ltz, lim.date_scheduled_installer_ltz)) as date_scheduled_installer_ltz,
        if(n.id is null, lim.datetime_appointment_ltz, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.datetime_appointment_ltz, lim.datetime_appointment_ltz)) as datetime_appointment_ltz,
        if(n.id is null, lim.time_scheduled_installer_ltz, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.time_scheduled_installer_ltz, lim.time_scheduled_installer_ltz)) as time_scheduled_installer_ltz,
        if(n.id is null, lim._datasource_transfer_ts, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n._datasource_transfer_ts, lim._datasource_transfer_ts)) as _datasource_transfer_ts,
        if(n.id is null, lim.appointment_freezed, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.appointment_freezed, lim.appointment_freezed)) as appointment_freezed,
        if(n.id is null, lim.check_pin_iav, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.check_pin_iav, lim.check_pin_iav)) as check_pin_iav,
        if(n.id is null, lim.date_creation, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.date_creation, lim.date_creation)) as date_creation,
        if(n.id is null, lim.date_creation_ts, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.date_creation_ts, lim.date_creation_ts)) as date_creation_ts,
        if(n.id is null, lim.date_creation_ymd, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.date_creation_ymd, lim.date_creation_ymd)) as date_creation_ymd,
        if(n.id is null, lim.date_creation_ymd_ltz, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.date_creation_ymd_ltz, lim.date_creation_ymd_ltz)) as date_creation_ymd_ltz,
        if(n.id is null, lim.date_last_modified_user, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.date_last_modified_user, lim.date_last_modified_user)) as date_last_modified_user,
        if(n.id is null, lim.date_modify, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.date_modify, lim.date_modify)) as date_modify,
        if(n.id is null, lim.date_modify_appointment, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.date_modify_appointment, lim.date_modify_appointment)) as date_modify_appointment,
        if(n.id is null, lim.date_modify_appointment_ts, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.date_modify_appointment_ts, lim.date_modify_appointment_ts)) as date_modify_appointment_ts,
        if(n.id is null, lim.date_modify_appointment_ymd, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.date_modify_appointment_ymd, lim.date_modify_appointment_ymd)) as date_modify_appointment_ymd,
        if(n.id is null, lim.date_modify_appointment_ymd_ltz, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.date_modify_appointment_ymd_ltz, lim.date_modify_appointment_ymd_ltz)) as date_modify_appointment_ymd_ltz,
        if(n.id is null, lim.date_modify_ts, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.date_modify_ts, lim.date_modify_ts)) as date_modify_ts,
        if(n.id is null, lim.date_modify_ymd, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.date_modify_ymd, lim.date_modify_ymd)) as date_modify_ymd,
        if(n.id is null, lim.date_modify_ymd_ltz, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.date_modify_ymd_ltz, lim.date_modify_ymd_ltz)) as date_modify_ymd_ltz,
        if(n.id is null, lim.date_old_appointment, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.date_old_appointment, lim.date_old_appointment)) as date_old_appointment,
        if(n.id is null, lim.date_old_appointment_ts, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.date_old_appointment_ts, lim.date_old_appointment_ts)) as date_old_appointment_ts,
        if(n.id is null, lim.date_old_appointment_ymd, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.date_old_appointment_ymd, lim.date_old_appointment_ymd)) as date_old_appointment_ymd,
        if(n.id is null, lim.date_old_appointment_ymd_ltz, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.date_old_appointment_ymd_ltz, lim.date_old_appointment_ymd_ltz)) as date_old_appointment_ymd_ltz,
        if(n.id is null, lim.date_scheduled_installer, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.date_scheduled_installer, lim.date_scheduled_installer)) as date_scheduled_installer,
        if(n.id is null, lim.date_scheduled_installer_ts, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.date_scheduled_installer_ts, lim.date_scheduled_installer_ts)) as date_scheduled_installer_ts,
        if(n.id is null, lim.date_scheduled_installer_ymd, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.date_scheduled_installer_ymd, lim.date_scheduled_installer_ymd)) as date_scheduled_installer_ymd,
        if(n.id is null, lim.date_scheduled_installer_ymd_ltz, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.date_scheduled_installer_ymd_ltz, lim.date_scheduled_installer_ymd_ltz)) as date_scheduled_installer_ymd_ltz,
        if(n.id is null, lim.datetime_appointment, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.datetime_appointment, lim.datetime_appointment)) as datetime_appointment,
        if(n.id is null, lim.datetime_appointment_ts, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.datetime_appointment_ts, lim.datetime_appointment_ts)) as datetime_appointment_ts,
        if(n.id is null, lim.datetime_appointment_ymd, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.datetime_appointment_ymd, lim.datetime_appointment_ymd)) as datetime_appointment_ymd,
        if(n.id is null, lim.datetime_appointment_ymd_ltz, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.datetime_appointment_ymd_ltz, lim.datetime_appointment_ymd_ltz)) as datetime_appointment_ymd_ltz,
        if(n.id is null, lim.event_subtype, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.event_subtype, lim.event_subtype)) as event_subtype,
        if(n.id is null, lim.extra_working_hour_slot, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.extra_working_hour_slot, lim.extra_working_hour_slot)) as extra_working_hour_slot,
        if(n.id is null, lim.flg_appointment_confirmed_of, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.flg_appointment_confirmed_of, lim.flg_appointment_confirmed_of)) as flg_appointment_confirmed_of,
        if(n.id is null, lim.flg_appointment_confirmed_of_bool, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.flg_appointment_confirmed_of_bool, lim.flg_appointment_confirmed_of_bool)) as flg_appointment_confirmed_of_bool,
        if(n.id is null, lim.flg_appointment_rescheduling, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.flg_appointment_rescheduling, lim.flg_appointment_rescheduling)) as flg_appointment_rescheduling,
        if(n.id is null, lim.flg_extension_slot, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.flg_extension_slot, lim.flg_extension_slot)) as flg_extension_slot,
        if(n.id is null, lim.flg_multivisit, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.flg_multivisit, lim.flg_multivisit)) as flg_multivisit,
        if(n.id is null, lim.flg_multivisit_bool, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.flg_multivisit_bool, lim.flg_multivisit_bool)) as flg_multivisit_bool,
        if(n.id is null, lim.id, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.id, lim.id)) as id,
        if(n.id is null, lim.id_case, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.id_case, lim.id_case)) as id_case,
        if(n.id is null, lim.id_egon_building, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.id_egon_building, lim.id_egon_building)) as id_egon_building,
        if(n.id is null, lim.id_inserimento, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.id_inserimento, lim.id_inserimento)) as id_inserimento,
        if(n.id is null, lim.id_inserimento_dl, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.id_inserimento_dl, lim.id_inserimento_dl)) as id_inserimento_dl,
        if(n.id is null, lim.id_order, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.id_order, lim.id_order)) as id_order,
        if(n.id is null, lim.id_slot, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.id_slot, lim.id_slot)) as id_slot,
        if(n.id is null, lim.installation_pin, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.installation_pin, lim.installation_pin)) as installation_pin,
        if(n.id is null, lim.installation_type, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.installation_type, lim.installation_type)) as installation_type,
        if(n.id is null, lim.installer_pin_wholesaler, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.installer_pin_wholesaler, lim.installer_pin_wholesaler)) as installer_pin_wholesaler,
        if(n.id is null, lim.number_of_rescheduling, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.number_of_rescheduling, lim.number_of_rescheduling)) as number_of_rescheduling,
        if(n.id is null, lim.order_type, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.order_type, lim.order_type)) as order_type,
        if(n.id is null, lim.scheduling_channel, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.scheduling_channel, lim.scheduling_channel)) as scheduling_channel,
        if(n.id is null, lim.service_type, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.service_type, lim.service_type)) as service_type,
        if(n.id is null, lim.staircase, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.staircase, lim.staircase)) as staircase,
        if(n.id is null, lim.status, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.status, lim.status)) as status,
        if(n.id is null, lim.substatus, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.substatus, lim.substatus)) as substatus,
        if(n.id is null, lim.time_scheduled_installer, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.time_scheduled_installer, lim.time_scheduled_installer)) as time_scheduled_installer,
        if(n.id is null, lim.time_scheduled_installer_ts, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.time_scheduled_installer_ts, lim.time_scheduled_installer_ts)) as time_scheduled_installer_ts,
        if(n.id is null, lim.time_scheduled_installer_ymd, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.time_scheduled_installer_ymd, lim.time_scheduled_installer_ymd)) as time_scheduled_installer_ymd,
        if(n.id is null, lim.time_scheduled_installer_ymd_ltz, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.time_scheduled_installer_ymd_ltz, lim.time_scheduled_installer_ymd_ltz)) as time_scheduled_installer_ymd_ltz,
        if(n.id is null, lim.token_id, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.token_id, lim.token_id)) as token_id,
        if(n.id is null, lim.wholesaler, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.wholesaler, lim.wholesaler)) as wholesaler,
        if(n.id is null, lim.work_order, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.work_order, lim.work_order)) as work_order,

    -- date of the status changes of appointment
        if(n.id is null, lim.date_appointment_opened_ts, if(lim.date_appointment_opened_ts is null or n.date_appointment_opened_ts < lim.date_appointment_opened_ts, n.date_appointment_opened_ts, lim.date_appointment_opened_ts)) as date_appointment_opened_ts,
        if(n.id is null, lim.date_appointment_opened_ltz, if(lim.date_appointment_opened_ltz is null or n.date_appointment_opened_ltz < lim.date_appointment_opened_ltz, n.date_appointment_opened_ltz, lim.date_appointment_opened_ltz)) as date_appointment_opened_ltz,
        if(n.id is null, lim.date_appointment_in_progress_ts, if(lim.date_appointment_in_progress_ts is null or n.date_appointment_in_progress_ts < lim.date_appointment_in_progress_ts, n.date_appointment_in_progress_ts, lim.date_appointment_in_progress_ts)) as date_appointment_in_progress_ts,
        if(n.id is null, lim.date_appointment_in_progress_ltz, if(lim.date_appointment_in_progress_ltz is null or n.date_appointment_in_progress_ltz < lim.date_appointment_in_progress_ltz, n.date_appointment_in_progress_ltz, lim.date_appointment_in_progress_ltz)) as date_appointment_in_progress_ltz,
        if(n.id is null, lim.date_appointment_cancelled_ts, if(lim.date_appointment_cancelled_ts is null or n.date_appointment_cancelled_ts < lim.date_appointment_cancelled_ts, n.date_appointment_cancelled_ts, lim.date_appointment_cancelled_ts)) as date_appointment_cancelled_ts,
        if(n.id is null, lim.date_appointment_cancelled_ltz, if(lim.date_appointment_cancelled_ltz is null or n.date_appointment_cancelled_ltz < lim.date_appointment_cancelled_ltz, n.date_appointment_cancelled_ltz, lim.date_appointment_cancelled_ltz)) as date_appointment_cancelled_ltz,
        if(n.id is null, lim.date_appointment_closed_ts, if(lim.date_appointment_closed_ts is null or n.date_appointment_closed_ts < lim.date_appointment_closed_ts, n.date_appointment_closed_ts, lim.date_appointment_closed_ts)) as date_appointment_closed_ts,
        if(n.id is null, lim.date_appointment_closed_ltz, if(lim.date_appointment_closed_ltz is null or n.date_appointment_closed_ltz < lim.date_appointment_closed_ltz, n.date_appointment_closed_ltz, lim.date_appointment_closed_ltz)) as date_appointment_closed_ltz,
        if(n.id is null, lim.date_appointment_rescheduled_ts, if(lim.date_appointment_rescheduled_ts is null or n.date_appointment_rescheduled_ts < lim.date_appointment_rescheduled_ts, n.date_appointment_rescheduled_ts, lim.date_appointment_rescheduled_ts)) as date_appointment_rescheduled_ts,
        if(n.id is null, lim.date_appointment_rescheduled_ltz, if(lim.date_appointment_rescheduled_ltz is null or n.date_appointment_rescheduled_ltz < lim.date_appointment_rescheduled_ltz, n.date_appointment_rescheduled_ltz, lim.date_appointment_rescheduled_ltz)) as date_appointment_rescheduled_ltz,

        -- info on the current status and substatus of the appointment
        if(n.id is null, lim.date_current_status_ts, if(lim.date_current_status_ts is null or n.date_current_status_ts > lim.date_current_status_ts, n.date_current_status_ts, lim.date_current_status_ts)) as date_current_status_ts,
        if(n.id is null, lim.date_current_status_ltz, if(lim.date_current_status_ltz is null or n.date_current_status_ltz > lim.date_current_status_ltz, n.date_current_status_ltz, lim.date_current_status_ltz)) as date_current_status_ltz,
        if(n.id is null, lim.date_current_substatus_ts, if(lim.date_current_substatus_ts is null or n.date_current_substatus_ts > lim.date_current_substatus_ts, n.date_current_substatus_ts, lim.date_current_substatus_ts)) as date_current_substatus_ts,
        if(n.id is null, lim.date_current_substatus_ltz, if(lim.date_current_substatus_ltz is null or n.date_current_substatus_ltz > lim.date_current_substatus_ltz, n.date_current_substatus_ltz, lim.date_current_substatus_ltz)) as date_current_substatus_ltz,

        -- info on the previous status and substatus of the appointment
        if(n.id is null, lim.previous_status, if(lim.date_previous_status_ts is null or n.date_previous_status_ts > lim.date_previous_status_ts, n.previous_status, lim.previous_status)) as previous_status,
        if(n.id is null, lim.date_previous_status_ts, if(lim.date_previous_status_ts is null or n.date_previous_status_ts > lim.date_previous_status_ts, n.date_previous_status_ts, lim.date_previous_status_ts)) as date_previous_status_ts,
        if(n.id is null, lim.date_previous_status_ltz, if(lim.date_previous_status_ltz is null or n.date_previous_status_ltz > lim.date_previous_status_ltz, n.date_previous_status_ltz, lim.date_previous_status_ltz)) as date_previous_status_ltz,
        if(n.id is null, lim.previous_substatus, if(lim.date_previous_substatus_ts is null or n.date_previous_substatus_ts > lim.date_previous_substatus_ts, n.previous_substatus, lim.previous_substatus)) as previous_substatus,
        if(n.id is null, lim.date_previous_substatus_ts, if(lim.date_previous_substatus_ts is null or n.date_previous_substatus_ts > lim.date_previous_substatus_ts, n.date_previous_substatus_ts, lim.date_previous_substatus_ts)) as date_previous_substatus_ts,
        if(n.id is null, lim.date_previous_substatus_ltz, if(lim.date_previous_substatus_ltz is null or n.date_previous_substatus_ltz > lim.date_previous_substatus_ltz, n.date_previous_substatus_ltz, lim.date_previous_substatus_ltz)) as date_previous_substatus_ltz,

        if(n.id is null, lim.id_user_created, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.id_user_created, lim.id_user_created)) as id_user_created,
        if(n.id is null, lim.id_user_last_modified, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.id_user_last_modified, lim.id_user_last_modified)) as id_user_last_modified,
        if(n.id is null, lim.datetime_dad, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.datetime_dad, lim.datetime_dad)) as datetime_dad,
        if(n.id is null, lim.datetime_dad_ts, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.datetime_dad_ts, lim.datetime_dad_ts)) as datetime_dad_ts,
        if(n.id is null, lim.scenario_fulfilment_crm, if(lim.date_modify_ltz is null or n.date_modify_ltz > lim.date_modify_ltz, n.scenario_fulfilment_crm, lim.scenario_fulfilment_crm)) as scenario_fulfilment_crm,
       from
        snap_new n full outer join last_query_result lim
            on n.id = lim.id

    )




    SELECT
        distinct
        ns._process_mapping_start_ts,
        ns._process_mapping_version,
        ns._process_transaction_id,
        ns.date_creation_ltz,
        ns.date_modify_appointment_ltz,
        ns.date_modify_ltz,
        ns.date_old_appointment_ltz,
        ns.date_scheduled_installer_ltz,
        ns.datetime_appointment_ltz,
        ns.time_scheduled_installer_ltz,
        ns._datasource_transfer_ts,
        ns.appointment_freezed,
        ns.check_pin_iav,
        ns.date_creation,
        ns.date_creation_ts,
        ns.date_creation_ymd,
        ns.date_creation_ymd_ltz,
        ns.date_last_modified_user,
        ns.date_modify,
        ns.date_modify_appointment,
        ns.date_modify_appointment_ts,
        ns.date_modify_appointment_ymd,
        ns.date_modify_appointment_ymd_ltz,
        ns.date_modify_ts,
        ns.date_modify_ymd,
        ns.date_modify_ymd_ltz,
        ns.date_old_appointment,
        ns.date_old_appointment_ts,
        ns.date_old_appointment_ymd,
        ns.date_old_appointment_ymd_ltz,
        ns.date_scheduled_installer,
        ns.date_scheduled_installer_ts,
        ns.date_scheduled_installer_ymd,
        ns.date_scheduled_installer_ymd_ltz,
        ns.datetime_appointment,
        ns.datetime_appointment_ts,
        ns.datetime_appointment_ymd,
        ns.datetime_appointment_ymd_ltz,
        ns.event_subtype,
        ns.extra_working_hour_slot,
        ns.flg_appointment_confirmed_of,
        ns.flg_appointment_confirmed_of_bool,
        ns.flg_appointment_rescheduling,
        ns.flg_extension_slot,
        ns.flg_multivisit,
        ns.flg_multivisit_bool,
        ns.id,
        ns.id_case,
        ns.id_egon_building,
        ns.id_inserimento,
        ns.id_inserimento_dl,
        ns.id_order,
        ns.id_slot,
        ns.installation_pin,
        ns.installation_type,
        ns.installer_pin_wholesaler,
        ns.number_of_rescheduling,
        ns.order_type,
        ns.scheduling_channel,
        ns.service_type,
        ns.staircase,
        ns.status,
        ns.substatus,
        ns.time_scheduled_installer,
        ns.time_scheduled_installer_ts,
        ns.time_scheduled_installer_ymd,
        ns.time_scheduled_installer_ymd_ltz,
        ns.token_id,
        ns.wholesaler,
        ns.work_order,
        ns.date_appointment_opened_ts,
        ns.date_appointment_opened_ltz,
        ns.date_appointment_in_progress_ts,
        ns.date_appointment_in_progress_ltz,
        ns.date_appointment_cancelled_ts,
        ns.date_appointment_cancelled_ltz,
        ns.date_appointment_closed_ts,
        ns.date_appointment_closed_ltz,
        ns.date_appointment_rescheduled_ts,
        ns.date_appointment_rescheduled_ltz,
        ns.id_user_created,
        ns.id_user_last_modified,
        ns.datetime_dad,
        ns.datetime_dad_ts,
        ns.scenario_fulfilment_crm,
        LAG( ns.date_creation_ts,1) OVER (PARTITION BY  ns.id_order ORDER BY  ns.date_modify) AS previous_date_creation_ts,
        LAG( ns.date_creation_ltz,1) OVER (PARTITION BY  ns.id_order ORDER BY  ns.date_modify) AS previous_date_creation_ltz,
        LAG( ns.id,1) OVER (PARTITION BY  ns.id_order ORDER BY  ns.date_modify) AS  previous_id,
        LAG( ns.datetime_appointment_ts,1) OVER (PARTITION BY  ns.id_order ORDER BY  ns.date_modify) AS previous_datetime_appointment_ts,
        LAG( ns.datetime_appointment_ltz,1) OVER (PARTITION BY  ns.id_order ORDER BY  ns.date_modify) AS previous_datetime_appointment_ltz,
        LAG( ns.event_subtype,1) OVER (PARTITION BY  ns.id_order ORDER BY  ns.date_modify) AS previous_event_subtype,
        LAG(ns.scheduling_channel,1) OVER (PARTITION BY  ns.id_order ORDER BY  ns.date_modify) AS previous_scheduling_channel,
        LAG( ns.status) OVER (PARTITION BY  ns.id_order ORDER BY  ns.date_creation_ts) as status_previous_id,
        IF(LAG(upper( ns.status)) OVER (PARTITION BY  ns.id_order ORDER BY  ns.date_creation_ts) = "ANNULLATO" AND UPPER( ns.event_subtype) = 'ACTIVATION RESCHEDULING', TRUE, FALSE) AS flg_rescheduled_after_suspension,
        IF(UPPER( ns.status) IN ('ANNULLATO', 'RISCHEDULATO', 'RESCHEDULED OF') AND  ns.datetime_appointment_ltz >  ns.date_modify_ltz, TRUE, FALSE) AS flg_cancelled_rescheduled_before_DAC,
        IF( fsco.scheduling_channel is null or trim( fsco.scheduling_channel) ='',  fsco.scheduling_channel ,   fsco.scheduling_channel)   first_scheduling_channel,
        # Technical fields
        current_timestamp() as snapshot_time,
        TIMESTAMP("{{ var("ingestion_date") }}") as partition_date,
        extract(hour from TIMESTAMP("{{ var("ingestion_date") }}")) as _clustering_hour,
        extract(minute from TIMESTAMP("{{ var("ingestion_date") }}")) as _clustering_minute,
    from new_snapshot ns
     full outer join first_scheduling_channel_one fsco on ( ns.id = fsco.id)
    WHERE ns.id is not null and trim(ns.id) != ""