import os
from dataclasses import dataclass, field


def _merchants(raw):
    # "merchant_a:key1,merchant_b:key2". In a cluster this comes from a Secret.
    pairs = (item.split(':', 1) for item in raw.split(',') if ':' in item)
    return {key.strip(): merchant.strip() for merchant, key in pairs}


@dataclass
class Settings:
    database_url: str = field(default_factory=lambda: os.environ.get('DATABASE_URL', 'sqlite:///./payments.db'))
    redis_url: str = field(default_factory=lambda: os.environ.get('REDIS_URL', 'redis://localhost:6379/0'))
    stream: str = os.environ.get('PAYMENT_STREAM', 'payments')
    consumer_group: str = os.environ.get('PAYMENT_CONSUMER_GROUP', 'processors')
    api_keys: dict = field(default_factory=lambda: _merchants(
        os.environ.get('MERCHANT_API_KEYS', 'demo-shop:test_key_demo_shop')))
    max_attempts: int = int(os.environ.get('PAYMENT_MAX_ATTEMPTS', '5'))
    # a message nobody has acknowledged for this long is claimed by another worker (crash recovery)
    claim_idle_ms: int = int(os.environ.get('PAYMENT_CLAIM_IDLE_MS', '30000'))
    stuck_after_seconds: int = int(os.environ.get('PAYMENT_STUCK_AFTER_SECONDS', '300'))


settings = Settings()
