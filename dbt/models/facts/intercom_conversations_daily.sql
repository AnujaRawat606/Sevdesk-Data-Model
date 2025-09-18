{{ config(materialized='incremental', unique_key='date_day') }}

with

-- chats
conversations as (
    select *
    from {{ ref('stg_intercom_conversations') }}
)

-- chat messages
, conversation_parts as (
    select * 
    from {{ ref('stg_intercom_conversation_parts') }}
)

-- sevdesk clients
, clients as (
    select *
    from {{ ref('dim_client') }}
)

-- dates
, dates as (
    select * 
    from {{ ref('dim_dates') }}
)

, combined as(
  select
    conversations.conversation_id,
    conversation_parts.conversation_part_id,

    conversations.sev_client_id,
    conversations.author_contact_id,
    conversations.agent_assignee_id,
        
    conversations.created_at_utc,

    conversations.is_currently_open,
    conversations.status_label,
    conversations.rating,

    conversations.was_conversation_outside_office_hours,

    conversation_parts.created_at_utc as part_created_at_utc,
    conversation_parts.updated_at_utc as part_updated_at_utc,
    conversation_parts.part_type,

    conversation_parts.is_first_agent_reply,
    conversations.first_closed_at_utc,
    conversations.last_closed_at_utc,

    coalesce(date_diff(second, conversations.created_at_utc, case when conversation_parts.is_first_agent_reply = TRUE then conversation_parts.created_at_utc end), 0) as time_to_first_agent_reply_seconds,

    coalesce(date_diff(second, conversations.created_at_utc, conversations.first_closed_at_utc), 0) as time_to_first_close_seconds,
    coalesce(date_diff(second, conversations.created_at_utc, conversations.last_closed_at_utc) / 60, 0) as time_to_last_close_seconds

from conversations
join conversation_parts
  on conversations.conversation_id = conversation_parts.conversation_id
-- where conversations.conversation_id = 15233908271256
)

-- select *
-- from combined

, conversations_daily as (

    select
        DATE(combined.created_at_utc) as date_day,
        
        count(conversation_part_id) as chat_count,
        count( case when combined.was_conversation_outside_office_hours = true then conversation_part_id end) as chats_outside_business_hours_count,

        count(conversation_part_id) - count( case when combined.was_conversation_outside_office_hours = true then conversation_part_id end)  as chats_inside_business_hours_count,
        
        count( case when (combined.time_to_first_agent_reply_seconds <= 60 and is_first_agent_reply is true) then conversation_part_id  end) as chat_first_response_60_sec_count,

        case 
          when count(conversation_part_id) = 0 then 0 
          else count( case when (combined.time_to_first_agent_reply_seconds <= 60 and is_first_agent_reply is true) then conversation_part_id  end) / count(conversation_part_id)
        end as chat_reachability,

        avg(combined.time_to_last_close_seconds) as chat_avg_handling_time_seconds,
        avg(combined.rating) as chat_avg_rating
    
    from combined
    where combined.sev_client_id in (select sev_client_id from clients)
    group by DATE(combined.created_at_utc)

)

, spined as (

    select
        dates.date_day,
        dates.date_year,
        dates.quarter_name,
        dates.month,
        case 
          when dates.week < 10 then concat(dates.date_year, "-W0", dates.week) 
          else concat(dates.date_year, "-W", dates.week) 
        end as week, 

        coalesce(chat_count, 0) as chat_count,
        coalesce(chats_outside_business_hours_count, 0) as chats_outside_business_hours_count,
        coalesce(chats_inside_business_hours_count, 0) as chats_inside_business_hours_count,
        
        coalesce(chat_reachability, 0) as chat_reachability,
        round(coalesce(chat_avg_handling_time_seconds, 0), 2) as chat_avg_handling_time_seconds,
        round(coalesce(chat_avg_rating, 0), 2) as chat_avg_rating
    
    from dates 
    join conversations_daily
      on dates.date_day = conversations_daily.date_day

)

, final as (

    select
        date_day,
        date_year, 
        quarter_name,
        month,
        week,

        chat_count,
        chats_outside_business_hours_count,
        chats_inside_business_hours_count,
        
        chat_reachability,
        chat_avg_handling_time_seconds,
        chat_avg_rating
    
    from spined

)

select * from final

{% if is_incremental() %}
  where date_day > (select max(date_day) from {{ this }})
{% endif %}
