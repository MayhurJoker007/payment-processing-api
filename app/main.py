import logging
import secrets
import time
import uuid
from datetime import datetime
from typing import Annotated

from fastapi import Depends, FastAPI, Header, HTTPException, Request, Response
from fastapi.responses import JSONResponse, PlainTextResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, Field
from sqlalchemy import select, text
from sqlalchemy.orm import Session

from . import metrics, service
from .config import settings
from .db import get_session
from .logs import request_id, setup_logging
from .models import Payment

setup_logging()
log = logging.getLogger('payments.api')
app = FastAPI(title='Payment Processing API', version='1.0.0',
              description='Accepts payment requests, processes them asynchronously and tracks their lifecycle.')

CURRENCIES = {'ZAR', 'USD', 'EUR', 'GBP', 'BWP', 'NAD'}


# ---- request plumbing ----------------------------------------------------------------

@app.middleware('http')
async def observe(request: Request, call_next):
    rid = request.headers.get('X-Request-Id') or uuid.uuid4().hex
    token = request_id.set(rid)
    started = time.perf_counter()
    try:
        response = await call_next(request)
    finally:
        request_id.reset(token)
    route = request.scope.get('route')
    path = route.path if route else 'unmatched'
    metrics.REQUESTS.labels(request.method, path, response.status_code).inc()
    metrics.LATENCY.labels(path).observe(time.perf_counter() - started)
    response.headers['X-Request-Id'] = rid
    return response


def merchant(x_api_key: Annotated[str | None, Header()] = None):
    for key, merchant_id in settings.api_keys.items():
        if x_api_key and secrets.compare_digest(x_api_key, key):
            return merchant_id
    raise HTTPException(401, 'missing or invalid X-Api-Key')


def idempotency_key(idempotency_key: Annotated[str | None, Header()] = None):
    if not idempotency_key or not (8 <= len(idempotency_key) <= 100):
        raise HTTPException(400, 'an Idempotency-Key header of 8 to 100 characters is required')
    return idempotency_key


def owned_payment(payment_id: str, merchant_id: str = Depends(merchant), session: Session = Depends(get_session)):
    p = session.get(Payment, payment_id)
    if p is None or p.merchant_id != merchant_id:
        raise HTTPException(404, 'payment not found')     # 404, not 403: don't confirm other merchants' ids exist
    return p


@app.exception_handler(service.Conflict)
async def conflict(request, exc):
    return JSONResponse({'detail': str(exc)}, status_code=409)


@app.exception_handler(service.IdempotencyMismatch)
async def mismatch(request, exc):
    return JSONResponse({'detail': str(exc)}, status_code=422)


# ---- schemas ------------------------------------------------------------------------

class PaymentIn(BaseModel):
    amount: int = Field(gt=0, le=10_000_000_00, description='in minor units: 15000 is R150.00')
    currency: str = Field(pattern=r'^[A-Z]{3}$')
    payment_method: str = Field(min_length=4, max_length=64, description='tokenised card or account reference')
    reference: str = Field('', max_length=100)


class EventOut(BaseModel):
    type: str
    from_status: str | None
    to_status: str
    at: datetime
    detail: dict


class RefundOut(BaseModel):
    id: str
    amount: int
    reason: str
    created_at: datetime


class PaymentOut(BaseModel):
    id: str
    status: str
    amount: int
    currency: str
    reference: str
    refunded_amount: int
    failure_code: str | None
    attempts: int
    created_at: datetime
    updated_at: datetime
    events: list[EventOut] = []
    refunds: list[RefundOut] = []

    @classmethod
    def of(cls, p):
        return cls(id=p.id, status=p.status, amount=p.amount, currency=p.currency, reference=p.reference,
                   refunded_amount=p.refunded_amount, failure_code=p.failure_code, attempts=p.attempts,
                   created_at=p.created_at, updated_at=p.updated_at,
                   events=[EventOut(type=e.type, from_status=e.from_status, to_status=e.to_status, at=e.at,
                                    detail=e.detail) for e in p.events],
                   refunds=[RefundOut(id=r.id, amount=r.amount, reason=r.reason, created_at=r.created_at)
                            for r in p.refunds])


class RefundIn(BaseModel):
    amount: int = Field(gt=0)
    reason: str = Field('', max_length=200)


# ---- routes -----------------------------------------------------------------------

@app.post('/v1/payments', response_model=PaymentOut, status_code=202, tags=['payments'])
def create_payment(body: PaymentIn, response: Response, merchant_id: str = Depends(merchant),
                   key: str = Depends(idempotency_key), session: Session = Depends(get_session)):
    """Accepted for processing (202). Poll GET /v1/payments/{id} or consume the payment.* events for the result."""
    if body.currency not in CURRENCIES:
        raise HTTPException(422, 'currency %s is not supported' % body.currency)
    payment, replayed = service.create_payment(session, merchant_id, key, body.model_dump())
    metrics.PAYMENTS_CREATED.labels(str(replayed).lower()).inc()
    if replayed:
        response.status_code = 200
        response.headers['Idempotent-Replayed'] = 'true'
    else:
        log.info('payment accepted', extra={'payment_id': payment.id, 'merchant_id': merchant_id,
                                            'amount': payment.amount, 'currency': payment.currency})
    session.refresh(payment)
    return PaymentOut.of(payment)


@app.get('/v1/payments/{payment_id}', response_model=PaymentOut, tags=['payments'])
def get_payment(payment: Payment = Depends(owned_payment)):
    return PaymentOut.of(payment)


@app.get('/v1/payments', response_model=list[PaymentOut], tags=['payments'])
def list_payments(status: str | None = None, limit: int = 50, merchant_id: str = Depends(merchant),
                  session: Session = Depends(get_session)):
    q = select(Payment).where(Payment.merchant_id == merchant_id).order_by(Payment.created_at.desc())
    if status:
        q = q.where(Payment.status == status)
    return [PaymentOut.of(p) for p in session.scalars(q.limit(min(limit, 200)))]


@app.post('/v1/payments/{payment_id}/cancel', response_model=PaymentOut, tags=['payments'])
def cancel_payment(payment: Payment = Depends(owned_payment), session: Session = Depends(get_session)):
    # owned_payment and this route share the request's session, so the payment is already attached to it
    return PaymentOut.of(service.cancel(session, payment))


@app.post('/v1/payments/{payment_id}/refunds', response_model=RefundOut, status_code=201, tags=['payments'])
def refund_payment(body: RefundIn, response: Response, payment: Payment = Depends(owned_payment),
                   key: str = Depends(idempotency_key), session: Session = Depends(get_session)):
    r, replayed = service.refund(session, payment, key, body.amount, body.reason)
    if replayed:
        response.status_code = 200
        response.headers['Idempotent-Replayed'] = 'true'
    return RefundOut(id=r.id, amount=r.amount, reason=r.reason, created_at=r.created_at)


@app.get('/healthz', tags=['ops'])
def health():
    return {'status': 'ok'}


@app.get('/readyz', tags=['ops'])
def ready(session: Session = Depends(get_session)):
    session.execute(text('select 1'))
    return {'status': 'ready'}


@app.get('/metrics', tags=['ops'], include_in_schema=False)
def prometheus():
    return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)
