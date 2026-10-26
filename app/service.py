"""Payment state changes. All of them go through transition(), which checks the move is legal, records the event
and writes the outbox message, in the caller's transaction."""
import hashlib
import json

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from .models import Outbox, Payment, PaymentEvent, Refund

TRANSITIONS = {
    'pending': {'processing', 'cancelled'},
    'processing': {'succeeded', 'failed'},
    'succeeded': {'partially_refunded', 'refunded'},
    'partially_refunded': {'partially_refunded', 'refunded'},
}
FINAL = {'failed', 'cancelled', 'refunded'}


class Conflict(Exception):
    """409: the request can't be applied to the payment as it is now."""


class IdempotencyMismatch(Exception):
    """422: the idempotency key was already used with a different request body."""


def request_hash(body: dict):
    return hashlib.sha256(json.dumps(body, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def transition(session, payment, to, event_type, **detail):
    if to not in TRANSITIONS.get(payment.status, set()):
        raise Conflict('payment is %s and cannot become %s' % (payment.status, to))
    session.add(PaymentEvent(payment_id=payment.id, type=event_type, from_status=payment.status, to_status=to,
                             detail=detail))
    payment.status = to
    session.add(Outbox(topic=event_type, payload={'payment_id': payment.id, 'status': to,
                                                  'merchant_id': payment.merchant_id, **detail}))


def create_payment(session, merchant_id, key, body):
    """Returns (payment, replayed). Same key + same body -> the original payment. Same key + different body ->
    IdempotencyMismatch, because silently returning the old payment for a new request would be worse."""
    digest = request_hash(body)
    existing = session.scalar(select(Payment).where(Payment.merchant_id == merchant_id,
                                                    Payment.idempotency_key == key))
    if existing:
        if existing.request_hash != digest:
            raise IdempotencyMismatch('Idempotency-Key %r was already used with a different request' % key)
        return existing, True

    payment = Payment(merchant_id=merchant_id, idempotency_key=key, request_hash=digest, amount=body['amount'],
                      currency=body['currency'], payment_method_token=body['payment_method'],
                      reference=body.get('reference', ''))
    session.add(payment)
    session.flush()
    session.add(PaymentEvent(payment_id=payment.id, type='payment.created', to_status='pending', detail={}))
    session.add(Outbox(topic='payment.created', payload={'payment_id': payment.id, 'status': 'pending',
                                                         'merchant_id': merchant_id}))
    try:
        session.commit()
    except IntegrityError:
        # two requests with the same key at the same moment: the other one won
        session.rollback()
        return create_payment(session, merchant_id, key, body)
    return payment, False


def cancel(session, payment):
    transition(session, payment, 'cancelled', 'payment.cancelled')
    session.commit()
    return payment


def refund(session, payment, key, amount, reason=''):
    existing = session.scalar(select(Refund).where(Refund.payment_id == payment.id, Refund.idempotency_key == key))
    if existing:
        if existing.amount != amount:
            raise IdempotencyMismatch('Idempotency-Key %r was already used for a refund of %d' % (key, existing.amount))
        return existing, True
    if payment.status not in ('succeeded', 'partially_refunded'):
        raise Conflict('only a succeeded payment can be refunded (this one is %s)' % payment.status)
    remaining = payment.amount - payment.refunded_amount
    if amount > remaining:
        raise Conflict('refund of %d is more than the %d left to refund' % (amount, remaining))

    r = Refund(payment_id=payment.id, idempotency_key=key, amount=amount, reason=reason)
    session.add(r)
    session.flush()      # assigns the refund id, which goes into the event
    payment.refunded_amount += amount
    to = 'refunded' if payment.refunded_amount == payment.amount else 'partially_refunded'
    transition(session, payment, to, 'payment.refunded', refund_id=r.id, amount=amount)
    session.commit()
    return r, False
