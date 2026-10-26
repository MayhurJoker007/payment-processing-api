"""JSON logs, one object per line, so the cluster's log collector can index fields like payment_id and
request_id instead of grepping text."""
import contextvars
import json
import logging
import os
from datetime import datetime, timezone

request_id = contextvars.ContextVar('request_id', default=None)
_STANDARD = set(vars(logging.LogRecord('', 0, '', 0, '', (), None))) | {'message', 'asctime'}


class JsonFormatter(logging.Formatter):
    def format(self, record):
        entry = {
            'ts': datetime.fromtimestamp(record.created, timezone.utc).isoformat(timespec='milliseconds'),
            'level': record.levelname.lower(),
            'logger': record.name,
            'msg': record.getMessage(),
        }
        if rid := request_id.get():
            entry['request_id'] = rid
        entry.update({k: v for k, v in vars(record).items() if k not in _STANDARD})
        if record.exc_info:
            entry['exc'] = self.formatException(record.exc_info)
        return json.dumps(entry, default=str)


def setup_logging():
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(os.environ.get('LOG_LEVEL', 'INFO'))
