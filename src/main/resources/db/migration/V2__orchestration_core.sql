-- Core orchestration tables

create table if not exists dag (
  id bigserial primary key,
  dag_id text not null unique,
  description text,
  is_active boolean not null default true,
  created_at timestamptz not null default now()
);

create table if not exists dag_run (
  id bigserial primary key,
  dag_id text not null,
  run_id text not null,
  run_date date not null,
  status text not null,
  started_at timestamptz,
  finished_at timestamptz,
  created_at timestamptz not null default now(),
  unique (dag_id, run_id)
);

create table if not exists task (
  id bigserial primary key,
  dag_id text not null,
  task_id text not null,
  task_type text not null,
  created_at timestamptz not null default now(),
  unique (dag_id, task_id)
);

create table if not exists task_run (
  id bigserial primary key,
  dag_run_id bigint not null references dag_run(id) on delete cascade,
  task_id text not null,
  status text not null,
  attempt int not null default 1,
  started_at timestamptz,
  finished_at timestamptz,
  error text,
  created_at timestamptz not null default now(),
  unique (dag_run_id, task_id, attempt)
);

create index if not exists idx_dag_run_status on dag_run(status);
create index if not exists idx_task_run_status on task_run(status);