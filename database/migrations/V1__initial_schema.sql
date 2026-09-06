CREATE SCHEMA IF NOT EXISTS runtime;
CREATE SCHEMA IF NOT EXISTS observer;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_type t
    JOIN pg_namespace n ON n.oid = t.typnamespace
    WHERE t.typname = 'ledger_state' AND n.nspname = 'runtime'
  ) THEN
    CREATE TYPE runtime.ledger_state AS ENUM (
      'RECEIVED',
      'CLAIMED',
      'EXECUTING',
      'UNKNOWN',
      'RECONCILING',
      'EFFECT_CONFIRMED',
      'COMPLETED',
      'RETRYABLE_FAILURE',
      'FAILED_FINAL'
    );
  END IF;
END;
$$;

CREATE TABLE IF NOT EXISTS runtime.execution_ledger (
  operation_id text PRIMARY KEY,
  tool_name text NOT NULL,
  request_hash text NOT NULL,
  state runtime.ledger_state NOT NULL,
  effect_reference text,
  result_payload jsonb,
  attempt_count integer NOT NULL DEFAULT 0,
  owner_token text,
  lease_expiry timestamptz,
  variant text NOT NULL,
  downstream_capability text NOT NULL,
  last_attempt_id text,
  last_error text,
  reconciliation_attempts integer NOT NULL DEFAULT 0,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS runtime.ledger_transitions (
  id bigserial PRIMARY KEY,
  operation_id text NOT NULL,
  state_before runtime.ledger_state,
  state_after runtime.ledger_state NOT NULL,
  attempt_id text,
  reason text,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION runtime.is_legal_ledger_transition(
  before_state runtime.ledger_state,
  after_state runtime.ledger_state
) RETURNS boolean
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT
    before_state IS NULL
    OR after_state = 'FAILED_FINAL'
    OR (before_state = 'RECEIVED' AND after_state = 'CLAIMED')
    OR (before_state = 'CLAIMED' AND after_state = 'EXECUTING')
    OR (before_state = 'EXECUTING' AND after_state IN ('EFFECT_CONFIRMED', 'RETRYABLE_FAILURE', 'UNKNOWN'))
    OR (before_state = 'UNKNOWN' AND after_state = 'RECONCILING')
    OR (before_state = 'RECONCILING' AND after_state IN ('EFFECT_CONFIRMED', 'RETRYABLE_FAILURE', 'UNKNOWN'))
    OR (before_state = 'EFFECT_CONFIRMED' AND after_state = 'COMPLETED')
    OR (before_state = 'RETRYABLE_FAILURE' AND after_state = 'CLAIMED')
    OR (before_state = after_state);
$$;

CREATE OR REPLACE FUNCTION runtime.touch_updated_at()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS execution_ledger_touch_updated_at ON runtime.execution_ledger;
CREATE TRIGGER execution_ledger_touch_updated_at
BEFORE UPDATE ON runtime.execution_ledger
FOR EACH ROW EXECUTE FUNCTION runtime.touch_updated_at();

CREATE TABLE IF NOT EXISTS runtime.orders (
  order_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  customer_id text NOT NULL,
  product_id text NOT NULL,
  quantity integer NOT NULL CHECK (quantity > 0),
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS runtime.payments (
  payment_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  customer_id text NOT NULL,
  amount numeric(12,2) NOT NULL CHECK (amount >= 0),
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS runtime.inventory_reservations (
  reservation_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  product_id text NOT NULL,
  quantity integer NOT NULL CHECK (quantity > 0),
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS runtime.notifications (
  notification_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  customer_id text NOT NULL,
  template text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS runtime.service_idempotency (
  service_name text NOT NULL,
  operation_id text NOT NULL,
  request_hash text NOT NULL,
  effect_id text NOT NULL,
  result_payload jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (service_name, operation_id)
);

CREATE TABLE IF NOT EXISTS runtime.event_log (
  id bigserial PRIMARY KEY,
  experiment_id text,
  run_id text,
  seed bigint,
  variant text,
  downstream_capability text,
  failure_scenario text,
  failure_probability numeric,
  concurrency integer,
  operation_id text,
  attempt_id text,
  service text,
  event_type text NOT NULL,
  event_time timestamptz NOT NULL DEFAULT now(),
  duration_ms numeric,
  tool_name text,
  request_hash text,
  ledger_state_before text,
  ledger_state_after text,
  downstream_effect_id text,
  fault_injected boolean DEFAULT false,
  fault_location text,
  replayed boolean DEFAULT false,
  reconciled boolean DEFAULT false,
  result_status text,
  payload jsonb DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS observer.observer_effects (
  effect_id text PRIMARY KEY,
  operation_id text,
  effect_type text NOT NULL,
  service text NOT NULL,
  experiment_id text,
  run_id text,
  created_at timestamptz NOT NULL DEFAULT now(),
  payload jsonb DEFAULT '{}'::jsonb
);

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'runtime_service') THEN
    CREATE ROLE runtime_service LOGIN PASSWORD 'runtime_service';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'gateway_user') THEN
    CREATE ROLE gateway_user LOGIN PASSWORD 'gateway_user';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'orchestrator_user') THEN
    CREATE ROLE orchestrator_user LOGIN PASSWORD 'orchestrator_user';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'analysis_user') THEN
    CREATE ROLE analysis_user LOGIN PASSWORD 'analysis_user';
  END IF;
END;
$$;

GRANT USAGE ON SCHEMA runtime TO runtime_service, gateway_user, orchestrator_user, analysis_user;
GRANT USAGE ON SCHEMA observer TO runtime_service, analysis_user;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA runtime TO runtime_service, gateway_user, orchestrator_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA runtime TO runtime_service, gateway_user, orchestrator_user;
GRANT SELECT ON runtime.event_log, runtime.ledger_transitions TO analysis_user;

REVOKE ALL ON observer.observer_effects FROM runtime_service, gateway_user, orchestrator_user;
GRANT INSERT ON observer.observer_effects TO runtime_service;
GRANT SELECT ON observer.observer_effects TO analysis_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA runtime GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO runtime_service, gateway_user, orchestrator_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA observer GRANT INSERT ON TABLES TO runtime_service;
