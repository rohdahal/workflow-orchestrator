

-- Daily aggregates materialized from TLC parquet files (one row per dataset + pickup day).

create table if not exists tlc_daily_counts (
  id bigserial primary key,
  dataset text not null,
  pickup_date date not null,
  trip_count bigint not null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (dataset, pickup_date)
);

create index if not exists idx_tlc_daily_counts_dataset_date on tlc_daily_counts(dataset, pickup_date);