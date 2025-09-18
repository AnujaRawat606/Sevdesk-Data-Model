  select 
      * , 
      YEAR(date_day) as date_year
    from {{ ref('dates') }}
    where date_day >= current_date - INTERVAL 365 DAYS
