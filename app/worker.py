"""Background processes. Each runs as its own deployment:

    python -m app.worker relay      outbox -> broker
    python -m app.worker process    broker -> acquirer -> payment status
    python -m app.worker reaper     finds payments stuck in 'processing' and puts them back in the queue
"""
import logging
import socket
import sys
import time
from datetime import timedelta

from sqlalchemy import select
from sqlalchemy.orm.exc import StaleDataError

from . import metrics
from .acquirer import AcquirerTimeout, MockAcquirer
from .broker import Broker
from .config import settings
from .db import SessionLocal
from .logs import setup_logging
from .models import Outbox, Payment, now
from .service import transition

log = logging.getLogger('payments.worker')


def relay_once(session_factory, broker, batch=100):
    """Publishes unpublished outbox rows in order. If publishing fails half-way, the rest stay unpublished and go
    next time. A message can be published twice (crash between publish and commit), so consumers are idempotent."""
    with session_factory() as s:
        rows = s.scalars(select(Outbox).where(Outbox.published_at.is_(None)).order_by(Outbox.id).limit(batch)).all()
        for row in rows:
            broker.publish(row.topic, row.payload)
            row.published_at = now()
            s.commit()
        return len(rows)


def handle(session_factory, acquirer, message):
    """Processes one payment.created message. Returns True when the message can be acknowledged."""
    if message['topic'] not in ('payment.created', 'payment.retry'):
        return True                                          # not ours; other consumers care about other events
    pid = message['payload']['payment_id']
    with session_factory() as s:
        p = s.get(Payment, pid)
        if p is None or p.status not in ('pending', 'processing'):
            return True                                      # already done (duplicate delivery): nothing to do
        if p.status == 'pending':
            transition(s, p, 'processing', 'payment.processing')
        p.attempts += 1
        attempt = p.attempts
        s.commit()

        started = time.perf_counter()
        try:
            result = acquirer.charge(p.id, p.amount, p.currency, p.payment_method_token, attempt)
        except AcquirerTimeout as e:
            metrics.ACQUIRER_ERRORS.inc()
            if attempt >= settings.max_attempts:
                transition(s, p, 'failed', 'payment.failed', failure_code='acquirer_unavailable', attempts=attempt)
                p.failure_code = 'acquirer_unavailable'
                s.commit()
                metrics.PAYMENTS_FINISHED.labels('failed').inc()
                log.warning('payment failed after retries', extra={'payment_id': p.id, 'attempts': attempt})
                return True
            log.info('acquirer timeout, will retry', extra={'payment_id': p.id, 'attempt': attempt, 'error': str(e)})
            return False                                     # not acknowledged: redelivered after claim_idle_ms
        finally:
            metrics.ACQUIRER_LATENCY.observe(time.perf_counter() - started)

        if result.approved:
            p.acquirer_ref = result.acquirer_ref
            transition(s, p, 'succeeded', 'payment.succeeded', acquirer_ref=result.acquirer_ref)
        else:
            p.failure_code = result.decline_code
            transition(s, p, 'failed', 'payment.failed', failure_code=result.decline_code)
        try:
            s.commit()
        except StaleDataError:
            # another worker finished this payment first; its result stands
            s.rollback()
            return True
        metrics.PAYMENTS_FINISHED.labels(p.status).inc()
        log.info('payment %s', p.status, extra={'payment_id': p.id, 'attempt': attempt})
        return True


def process_once(session_factory, broker, acquirer, consumer, block_ms=2000):
    messages = broker.claim_stale(consumer, settings.claim_idle_ms) + broker.read(consumer, block_ms=block_ms)
    for mid, message in messages:
        if handle(session_factory, acquirer, message):
            broker.ack(mid)
    return len(messages)


def reap_once(session_factory, broker):
    """A payment can get stuck in 'processing' if its message was lost (e.g. Redis restarted without persistence).
    Republish a retry for anything that has sat there too long; handle() is idempotent so this is always safe."""
    cutoff = now() - timedelta(seconds=settings.stuck_after_seconds)
    with session_factory() as s:
        stuck = s.scalars(select(Payment).where(Payment.status == 'processing', Payment.updated_at < cutoff)).all()
        for p in stuck:
            broker.publish('payment.retry', {'payment_id': p.id})
            p.updated_at = now()
        s.commit()
    if stuck:
        log.warning('requeued stuck payments', extra={'count': len(stuck)})
    return len(stuck)


def main():
    setup_logging()
    role = sys.argv[1] if len(sys.argv) > 1 else 'process'
    broker = Broker.from_url(settings.redis_url, settings.stream, settings.consumer_group)
    broker.ensure_group()
    consumer = socket.gethostname()
    acquirer = MockAcquirer()
    log.info('worker starting', extra={'role': role, 'consumer': consumer})
    while True:
        try:
            if role == 'relay':
                if not relay_once(SessionLocal, broker):
                    time.sleep(0.5)
            elif role == 'reaper':
                reap_once(SessionLocal, broker)
                time.sleep(60)
            else:
                process_once(SessionLocal, broker, acquirer, consumer)
        except Exception:
            log.exception('worker loop error')
            time.sleep(2)


if __name__ == '__main__':
    main()
