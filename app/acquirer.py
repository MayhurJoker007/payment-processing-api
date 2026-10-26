"""A stand-in for the card acquirer / payment provider. Deterministic test tokens, like real providers' sandboxes:

    tok_visa_ok          approved
    tok_insufficient     declined: insufficient_funds  (final, not retried)
    tok_stolen           declined: stolen_card          (final)
    tok_flaky            times out on the first two attempts, then approves
    tok_down             always times out
"""
from dataclasses import dataclass

DECLINES = {'tok_insufficient': 'insufficient_funds', 'tok_stolen': 'stolen_card'}


class AcquirerTimeout(Exception):
    """We don't know whether the charge happened. Retrying is safe because we always send the same reference."""


@dataclass
class AcquirerResult:
    approved: bool
    acquirer_ref: str | None = None
    decline_code: str | None = None


class MockAcquirer:
    def __init__(self):
        self.charges = {}          # our payment id -> result, to prove a retry never charges twice

    def charge(self, payment_id, amount, currency, token, attempt):
        if payment_id in self.charges:           # the acquirer is idempotent on our reference
            return self.charges[payment_id]
        if token == 'tok_down' or (token == 'tok_flaky' and attempt <= 2):
            raise AcquirerTimeout('acquirer did not answer within 10s')
        if token in DECLINES:
            result = AcquirerResult(False, decline_code=DECLINES[token])
        else:
            result = AcquirerResult(True, acquirer_ref='acq_%s' % payment_id[-12:])
        self.charges[payment_id] = result
        return result
