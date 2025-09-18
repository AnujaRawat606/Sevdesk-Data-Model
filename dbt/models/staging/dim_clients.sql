  select 
      sev_client_id,
      signed_up_at_utc,
      registration_finished_at_utc,
      has_active_contract_current	active_plan,
      is_small_settlement,
      coalesce(is_test_account, FALSE) as is_test_account,
      _valid_from_utc as client_valid_from_utc,
      _valid_to_utc as client_valid_to_utc,
      _is_first as client_is_first,
      _is_latest as client_is_latest
  from {{ ref('clients') }}
  where is_test_account is null or is_test_account = FALSE
