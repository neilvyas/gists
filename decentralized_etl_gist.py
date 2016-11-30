"""Implements an ETL pipeline with decentralized handler definitions.

We register the handlers at the class-level but define the run function at the
instance level.  This way, we can essentially have a decentralized class
definition while still providing an instance to keep track of state.

Issues
------

This handles event-level effects very well, but I haven't massaged handling
account-level effects or handling account state very well at this point. Will
fix!
"""
from collections import defaultdict
from collections import namedtuple


# EffPipeline.py
# --------------
Eff = namedtuple('Eff', ('ticker', 'amt', 'acct_id', 'ts'))
"""We can ETL all of our event types into an effect type, then apply all the
resulting effects to a given state to arrive at a new state."""


def get_common_fields(logline):
    return [logline[field] for field in (
        'ticker',
        'amt',
        'acct_id',
        'ts',
    )]


class EffPipeline(object):
    _handlers = defaultdict(list)

    @classmethod
    def handle(cls, event_type):
        def decorator(f):
            cls._handlers[event_type].append(f)
            return f
        return decorator

    def run(self, loglines):
        for logline in loglines:
            event_type = logline['type']
            handlers = self._handlers.get(event_type, [])

            for handler in handlers:
                try:
                    effects = handler(self, logline)
                except TypeError:
                    effects = handler(logline)

                for effect in effects:
                    yield effect


# handlers/S.py
# -------------
@EffPipeline.handle('buy')
@EffPipeline.handle('sell')
def txn_handler_stateless(logline):
    """Handle independent transactions."""
    ticker, amt, acct_id, ts = get_common_fields(logline)
    dir_ = 1 if logline['type'] == 'buy' else -1
    price = logline['price']

    yield Eff(ticker, dir_ * amt, acct_id, ts)
    yield Eff('CASH', -1 * dir_ * amt * price, acct_id, ts)


# handlers/T.py
# -------------
@EffPipeline.handle('T')
def T_handler_stateful(state, logline):
    """Handle stateful transactions, where the amount that clears is either
    the current amount or the maximum of T txns cleared so far."""
    ticker, amt, acct_id, ts = get_common_fields(logline)
    T_count = getattr(state, 'T_count', 0)
    state.T_count = T_count + 1

    yield Eff(ticker, max(T_count, amt), acct_id, ts)


# account_state.py
# ----------------
class AccountState:
    def __init__(self, T_count=0, posns=dict()):
        self.T_count = T_count

        posns_ = defaultdict(int)
        posns_.update(posns)
        self.posns = posns_

    def __eq__(self, other):
        """Just compare field values. This should be ok because we're just
        using AccountState as a struct type thing."""
        if isinstance(other, self.__class__):
            return self.__dict__ == other.__dict__
        return False


# return None to be pythonic and indicate mutation of inputs.
def update_pipeline_w_acct_state(pipeline, acct):
    pipeline.T_count = acct.T_count
    return None


def update_acct_w_pipeline_state(pipeline, acct):
    # this function looks a little different because T_count is guaranteed to exist and be correct,
    # since we handled initialization of pipeline state.
    acct.T_count = pipeline.T_count
    return None


# eff_runner.py
# -------------
def run_acct_effs(effs, state=None):
    if state is None:
        state = defaultdict(int)
    for eff in effs:
        state[eff.ticker] += eff.amt

    return state


# The following 3 test suites can all be property-based tests.
# Since the functions are all so small, testing them is dead simple
# (I don't even think we really need tests)

# tests/handlers/txn.py
# -------------------

def test_txn_handler():
    test_buy_event = dict(ticker='test', type='buy', amt=2, price=50, ts=1, acct_id=1)
    test_sell_event = dict(ticker='test', type='sell', amt=2, price=50, ts=1, acct_id=1)

    desired_buy_effs = {Eff('test', 2, 1, 1) , Eff('CASH', -100, 1, 1)}
    desired_sell_effs = {Eff('test',-2, 1, 1), Eff('CASH', 100, 1, 1)}

    assert set(txn_handler_stateless(test_buy_event)) == desired_buy_effs
    assert set(txn_handler_stateless(test_sell_event)) == desired_sell_effs

test_txn_handler()

# tests/handlers/T.py
# -------------------

# mock this out or use a dummy object with just object.T_count.
def test_T_handler():
    test_T_event = dict(ticker='test', type='T', amt=0, ts=1, acct_id=1)
    test_pipeline = EffPipeline()

    T_count_is_min_effs = set(T_handler_stateful(test_pipeline, test_T_event))
    assert T_count_is_min_effs == {Eff('test', 0, 1, 1)}
    assert test_pipeline.T_count == 1

    T_count_is_max_effs = set(T_handler_stateful(test_pipeline, test_T_event))
    assert T_count_is_max_effs == {Eff('test', 1, 1, 1)}
    assert test_pipeline.T_count == 2

test_T_handler()

# tests/eff_runner.py
# -------------------

def test_eff_runner():
    test_eff = [Eff('test', 50, 1, 1)]
    state = run_acct_effs(test_eff)
    assert state == dict(test=50)

    state = run_acct_effs(test_eff, state)
    assert state == dict(test=100)

test_eff_runner()

# tests/register_handlers.py
# -------------------

assert EffPipeline._handlers, "The pipeline has no handlers registered!"
# maybe write some sort of test to check that we pass the correct parameters?
# Don't really know how to do that without mucking with existing handlers or
# adding new ones.

# pipeline_runner.py
# ------------------
if __name__ == "__main__":
    from itertools import groupby


    # We assume you can run GROUP BY acct_id ORDER BY ts, or something.
    # Because T is a stateful handler, we have that for each account all T events must be
    # handled in order.
    loglines = [
        # First account, the one with some initial state.
        dict(ticker="GOOG", type="buy", amt=4, acct_id=1, ts=1, price=640),

        # T_count = 4, we take amt = max(T_count, logline.amt).
        dict(ticker="AAPL", type="T", amt=5, acct_id=1, ts=2),  # amt=5
        dict(ticker="AAPL", type="T", amt=30, acct_id=1, ts=3), # amt=30
        dict(ticker="AAPL", type="T", amt=4, acct_id=1, ts=4),  # amt=6

        # Second account.
        dict(ticker="GOOG", type="buy", amt=4, acct_id=2, ts=1, price=700),
        dict(ticker="MS", type="sell", amt=4, acct_id=2, ts=1, price=400),
    ]

    # have an account with some initial state.

    acct_with_state = AccountState(
        T_count=4,
        posns={"AAPL": 14, "CASH": 10,},
    )

    acct_states = {
        1: acct_with_state,
    }

    # This screams MapReduce-able, for example.
    for acct_id, acct_loglines in groupby(loglines, lambda logline: logline['acct_id']):
        acct_state = acct_states.get(acct_id, AccountState())
        eff_pipeline = EffPipeline()
        update_pipeline_w_acct_state(eff_pipeline, acct_state)

        effs = eff_pipeline.run(acct_loglines)

        acct_state.posns = run_acct_effs(effs, acct_state.posns)
        update_acct_w_pipeline_state(eff_pipeline, acct_state)

        acct_states[acct_id] = acct_state

    # If you want to check it by staring at it.
    final_states = {
        1: AccountState(
            T_count=(4 + 3),
            posns={
                "AAPL": (14 + 5 + 30 + 6),
                "GOOG": 4,
                "CASH": (10 + -4 * 640),
            }
        ),
        2: AccountState(
            T_count=0,
            posns={
                "GOOG": 4,
                "MS": -4,
                "CASH": (-4 * 700 + 4 * 400),
            }
        ),
    }

    assert acct_states == final_states
