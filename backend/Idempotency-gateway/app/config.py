# Tune these without touching business logic.

# How long to fake a downstream payment call.
PAYMENT_PROCESSING_DELAY: float = 2.0

# Completed keys older than this are eligible for eviction.
# Stripe uses 24 h in production; 1 h is fine for a demo.
KEY_TTL_SECONDS: int = 3_600

# How often the background sweep runs.
CLEANUP_INTERVAL_SECONDS: int = 300

# How long a duplicate request will wait for an in-flight result
# before giving up with a 504.
IN_FLIGHT_TIMEOUT_SECONDS: float = 30.0
