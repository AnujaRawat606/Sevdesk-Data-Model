select 
  conversation_id,
  author_contact_id,
  sev_client_id,
  coalesce(agent_assignee_id, 'unknown') as agent_assignee_id,
  created_at_utc,
  updated_at_utc,
  is_currently_open,
  status_label,
  coalesce(rating, 0) as rating,
  coalesce(was_conversation_outside_office_hours, FALSE) as was_conversation_outside_office_hours,
  first_closed_at_utc,
  last_closed_at_utc
from {{ ref('conversation') }}
