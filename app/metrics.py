from prometheus_client import Counter, Histogram

REQUESTS = Counter('payments_http_requests_total', 'HTTP requests', ['method', 'route', 'status'])
LATENCY = Histogram('payments_http_request_seconds', 'HTTP request latency', ['route'])
PAYMENTS_CREATED = Counter('payments_created_total', 'Payments created', ['replayed'])
PAYMENTS_FINISHED = Counter('payments_finished_total', 'Payments reaching a final state', ['status'])
ACQUIRER_LATENCY = Histogram('payments_acquirer_seconds', 'Time spent calling the acquirer')
ACQUIRER_ERRORS = Counter('payments_acquirer_errors_total', 'Acquirer timeouts and errors')
