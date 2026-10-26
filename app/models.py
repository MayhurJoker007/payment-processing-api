import uuid
from datetime import datetime, timezone

from sqlalchemy import JSON, BigInteger, CheckConstraint, ForeignKey, Index, String, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


def now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def new_id(prefix):
    return '%s_%s' % (prefix, uuid.uuid4().hex[:20])


class Base(DeclarativeBase):
    pass


class Payment(Base):
    __tablename__ = 'payment'
    __table_args__ = (
        # one payment per merchant + idempotency key, enforced by the database
        UniqueConstraint('merchant_id', 'idempotency_key', name='uq_payment_idempotency'),
        CheckConstraint('amount > 0', name='ck_payment_amount_positive'),
        CheckConstraint('refunded_amount >= 0 AND refunded_amount <= amount', name='ck_payment_refund_bounds'),
        Index('ix_payment_status_updated', 'status', 'updated_at'),
    )

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=lambda: new_id('pay'))
    merchant_id: Mapped[str] = mapped_column(String(40))
    idempotency_key: Mapped[str] = mapped_column(String(100))
    request_hash: Mapped[str] = mapped_column(String(64))
    amount: Mapped[int] = mapped_column(BigInteger)                 # minor units (cents): no floats near money
    currency: Mapped[str] = mapped_column(String(3))
    payment_method_token: Mapped[str] = mapped_column(String(64))
    reference: Mapped[str] = mapped_column(String(100), default='')
    status: Mapped[str] = mapped_column(String(20), default='pending')
    failure_code: Mapped[str | None] = mapped_column(String(40))
    acquirer_ref: Mapped[str | None] = mapped_column(String(64))
    attempts: Mapped[int] = mapped_column(default=0)
    refunded_amount: Mapped[int] = mapped_column(BigInteger, default=0)
    version: Mapped[int] = mapped_column(default=1)
    created_at: Mapped[datetime] = mapped_column(default=now)
    updated_at: Mapped[datetime] = mapped_column(default=now, onupdate=now)

    __mapper_args__ = {'version_id_col': version}   # optimistic locking: two workers can't both move a payment

    events: Mapped[list['PaymentEvent']] = relationship(back_populates='payment', order_by='PaymentEvent.id')
    refunds: Mapped[list['Refund']] = relationship(back_populates='payment', order_by='Refund.created_at')


class Refund(Base):
    __tablename__ = 'refund'
    __table_args__ = (UniqueConstraint('payment_id', 'idempotency_key', name='uq_refund_idempotency'),)

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=lambda: new_id('re'))
    payment_id: Mapped[str] = mapped_column(ForeignKey('payment.id'))
    idempotency_key: Mapped[str] = mapped_column(String(100))
    amount: Mapped[int] = mapped_column(BigInteger)
    reason: Mapped[str] = mapped_column(String(200), default='')
    created_at: Mapped[datetime] = mapped_column(default=now)

    payment: Mapped[Payment] = relationship(back_populates='refunds')


class PaymentEvent(Base):
    """The payment's history, one row per state change."""
    __tablename__ = 'payment_event'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    payment_id: Mapped[str] = mapped_column(ForeignKey('payment.id'), index=True)
    type: Mapped[str] = mapped_column(String(40))
    from_status: Mapped[str | None] = mapped_column(String(20))
    to_status: Mapped[str] = mapped_column(String(20))
    detail: Mapped[dict] = mapped_column(JSON, default=dict)
    at: Mapped[datetime] = mapped_column(default=now)

    payment: Mapped[Payment] = relationship(back_populates='events')


class Outbox(Base):
    """Events to publish, written in the same transaction as the change that caused them. A relay publishes
    them to the broker afterwards. No change without its event, and no event for a change that rolled back."""
    __tablename__ = 'outbox'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    topic: Mapped[str] = mapped_column(String(40))
    payload: Mapped[dict] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(default=now)
    published_at: Mapped[datetime | None] = mapped_column(index=True)
