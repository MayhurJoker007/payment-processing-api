from datetime import timedelta

import fakeredis
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app import worker
from app.acquirer import MockAcquirer
from app.broker import Broker
from app.config import settings
from app.db import get_session
from app.main import app
from app.models import Base, Outbox, Payment, now

KEY = {'X-Api-Key': 'test_key_demo_shop'}


@pytest.fixture
def env(monkeypatch):
    engine = create_engine('sqlite://', connect_args={'check_same_thread': False}, poolclass=StaticPool)
    Base.metadata.create_all(engine)
    Session = sessionmaker(engine, expire_on_commit=False)

    def session():
        with Session() as s:
            yield s

    app.dependency_overrides[get_session] = session
    monkeypatch.setattr(settings, 'api_keys', {'test_key_demo_shop': 'demo-shop', 'test_key_other': 'other-shop'})
    monkeypatch.setattr(settings, 'claim_idle_ms', 0)        # redeliver unacknowledged messages straight away
    broker = Broker(fakeredis.FakeRedis(), 'payments', 'processors')
    broker.ensure_group()
    yield type('Env', (), {'client': TestClient(app), 'Session': Session, 'broker': broker,
                           'acquirer': MockAcquirer()})
    app.dependency_overrides.clear()


def create(env, key='order-10001-pay', token='tok_visa_ok', amount=15000, headers=KEY):
    return env.client.post('/v1/payments', json={'amount': amount, 'currency': 'ZAR', 'payment_method': token,
                                                 'reference': 'INV-10001'},
                           headers={**headers, 'Idempotency-Key': key})


def pump(env, rounds=6):
    """Run relay and processor until things settle, like the real worker loops would."""
    for _ in range(rounds):
        worker.relay_once(env.Session, env.broker)
        worker.process_once(env.Session, env.broker, env.acquirer, 'worker-1', block_ms=None)


def status(env, pid):
    return env.client.get('/v1/payments/' + pid, headers=KEY).json()


def test_payment_lifecycle_and_events(env):
    r = create(env)
    assert r.status_code == 202 and r.json()['status'] == 'pending'
    pid = r.json()['id']
    pump(env)
    body = status(env, pid)
    assert body['status'] == 'succeeded'
    assert [e['to_status'] for e in body['events']] == ['pending', 'processing', 'succeeded']


def test_idempotency_replays_and_rejects_changed_bodies(env):
    first = create(env)
    again = create(env)
    assert again.status_code == 200 and again.headers['Idempotent-Replayed'] == 'true'
    assert again.json()['id'] == first.json()['id']
    changed = create(env, amount=99999)
    assert changed.status_code == 422 and 'different request' in changed.json()['detail']
    with env.Session() as s:
        assert len(s.scalars(select(Payment)).all()) == 1


def test_idempotency_key_is_required_and_scoped_per_merchant(env):
    no_key = env.client.post('/v1/payments', json={'amount': 100, 'currency': 'ZAR', 'payment_method': 'tok_visa_ok'},
                             headers=KEY)
    assert no_key.status_code == 400
    a = create(env, key='shared-key-123')
    b = create(env, key='shared-key-123', headers={'X-Api-Key': 'test_key_other'})
    assert a.json()['id'] != b.json()['id']


def test_declines_are_final_and_not_retried(env):
    pid = create(env, token='tok_insufficient').json()['id']
    pump(env)
    body = status(env, pid)
    assert body['status'] == 'failed' and body['failure_code'] == 'insufficient_funds' and body['attempts'] == 1


def test_timeouts_are_retried_without_double_charging(env):
    pid = create(env, token='tok_flaky').json()['id']
    pump(env)
    body = status(env, pid)
    assert body['status'] == 'succeeded' and body['attempts'] == 3
    assert list(env.acquirer.charges) == [pid]                 # one charge, despite three attempts


def test_gives_up_after_max_attempts(env, monkeypatch):
    monkeypatch.setattr(settings, 'max_attempts', 3)
    pid = create(env, token='tok_down').json()['id']
    pump(env, rounds=8)
    body = status(env, pid)
    assert body['status'] == 'failed' and body['failure_code'] == 'acquirer_unavailable' and body['attempts'] == 3
    assert env.broker.pending_count() == 0


def test_duplicate_delivery_is_harmless(env):
    pid = create(env).json()['id']
    pump(env)
    env.broker.publish('payment.created', {'payment_id': pid})     # e.g. relay crashed after publish, before commit
    pump(env)
    body = status(env, pid)
    assert body['status'] == 'succeeded' and body['attempts'] == 1


def test_stuck_payments_are_requeued(env):
    pid = create(env).json()['id']
    worker.relay_once(env.Session, env.broker)
    env.broker.r.delete('payments')                               # the message is lost
    env.broker.ensure_group()
    with env.Session() as s:
        p = s.get(Payment, pid)
        p.status, p.updated_at = 'processing', now() - timedelta(hours=1)
        s.commit()
    assert worker.reap_once(env.Session, env.broker) == 1
    pump(env)
    assert status(env, pid)['status'] == 'succeeded'


def test_refunds(env):
    pid = create(env, amount=10000).json()['id']
    pump(env)
    refund = lambda key, amount: env.client.post('/v1/payments/%s/refunds' % pid, json={'amount': amount},
                                                 headers={**KEY, 'Idempotency-Key': key})
    assert refund('refund-0001', 4000).status_code == 201
    assert refund('refund-0001', 4000).status_code == 200             # replay
    assert refund('refund-0002', 7000).status_code == 409             # more than the R60 left
    assert refund('refund-0003', 6000).status_code == 201
    body = status(env, pid)
    assert body['status'] == 'refunded' and body['refunded_amount'] == 10000 and len(body['refunds']) == 2


def test_cancel_only_while_pending(env):
    pid = create(env).json()['id']
    assert env.client.post('/v1/payments/%s/cancel' % pid, headers=KEY).json()['status'] == 'cancelled'
    pump(env)
    assert status(env, pid)['status'] == 'cancelled'                    # the worker leaves it alone
    pid2 = create(env, key='order-10002-pay').json()['id']
    pump(env)
    assert env.client.post('/v1/payments/%s/cancel' % pid2, headers=KEY).status_code == 409


def test_merchants_cannot_see_each_others_payments(env):
    pid = create(env).json()['id']
    assert env.client.get('/v1/payments/' + pid, headers={'X-Api-Key': 'test_key_other'}).status_code == 404
    assert env.client.get('/v1/payments/' + pid).status_code == 401


def test_every_state_change_writes_an_outbox_event(env):
    pid = create(env).json()['id']
    pump(env)
    with env.Session() as s:
        topics = [o.topic for o in s.scalars(select(Outbox).order_by(Outbox.id))]
        assert topics == ['payment.created', 'payment.processing', 'payment.succeeded']
        assert all(o.published_at for o in s.scalars(select(Outbox)))


def test_validation_and_ops_endpoints(env):
    bad = env.client.post('/v1/payments', json={'amount': 10.5, 'currency': 'ZAR', 'payment_method': 'tok_visa_ok'},
                          headers={**KEY, 'Idempotency-Key': 'order-x-0001'})
    assert bad.status_code == 422
    assert env.client.get('/readyz').status_code == 200
    r = env.client.get('/healthz', headers={'X-Request-Id': 'abc123'})
    assert r.headers['X-Request-Id'] == 'abc123'
    assert 'payments_http_requests_total' in env.client.get('/metrics').text
