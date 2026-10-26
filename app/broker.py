"""Redis Streams as the message broker. A consumer group means several worker pods share the work, and a message
that a crashed worker never acknowledged is claimed by another one after a while."""
import json

import redis


class Broker:
    def __init__(self, client, stream, group):
        self.r, self.stream, self.group = client, stream, group

    @classmethod
    def from_url(cls, url, stream, group):
        return cls(redis.Redis.from_url(url), stream, group)

    def ensure_group(self):
        try:
            self.r.xgroup_create(self.stream, self.group, id='0', mkstream=True)
        except redis.ResponseError as e:
            if 'BUSYGROUP' not in str(e):
                raise

    def publish(self, topic, payload):
        return self.r.xadd(self.stream, {'topic': topic, 'payload': json.dumps(payload)}, maxlen=100_000)

    def read(self, consumer, count=10, block_ms=2000):
        resp = self.r.xreadgroup(self.group, consumer, {self.stream: '>'}, count=count, block=block_ms)
        return [(mid, self._decode(fields)) for _, entries in (resp or []) for mid, fields in entries]

    def claim_stale(self, consumer, idle_ms, count=10):
        """Messages delivered to some worker but not acknowledged within idle_ms: take them over."""
        _, entries, *_ = self.r.xautoclaim(self.stream, self.group, consumer, min_idle_time=idle_ms,
                                           start_id='0-0', count=count)
        return [(mid, self._decode(fields)) for mid, fields in entries if fields]

    def ack(self, message_id):
        self.r.xack(self.stream, self.group, message_id)

    def pending_count(self):
        return self.r.xpending(self.stream, self.group)['pending']

    @staticmethod
    def _decode(fields):
        f = {k.decode() if isinstance(k, bytes) else k: v.decode() if isinstance(v, bytes) else v
             for k, v in fields.items()}
        return {'topic': f['topic'], 'payload': json.loads(f['payload'])}
