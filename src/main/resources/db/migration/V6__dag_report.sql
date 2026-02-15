create table if not exists dag_report (
  id bigserial primary key,
  dag_run_id bigint not null unique references dag_run(id) on delete cascade,
  dag_id text not null,
  dataset text not null,
  report_key text not null,
  status text not null, -- in_progress | success | failed
  error text null,
  uploaded_at timestamptz null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_dag_report_status on dag_report(status);
