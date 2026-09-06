TRUNCATE TABLE
  runtime.execution_ledger,
  runtime.ledger_transitions,
  runtime.orders,
  runtime.payments,
  runtime.inventory_reservations,
  runtime.notifications,
  runtime.service_idempotency,
  runtime.event_log,
  observer.observer_effects
RESTART IDENTITY;

