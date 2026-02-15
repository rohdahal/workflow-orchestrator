create table if not exists tlc_ingest_manifest (
  id bigserial primary key,
  dataset text not null,
  year_month text not null, -- 'YYYY-MM'
  source_file text not null,
  status text not null, -- 'in_progress' | 'success' | 'failed'
  rows_loaded bigint not null default 0,
  file_size_bytes bigint not null default 0,
  checksum text null,
  ingested_at timestamptz null,
  error text null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (dataset, year_month)
);

create index if not exists idx_tlc_ingest_manifest_status on tlc_ingest_manifest(status);