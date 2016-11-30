"""
StateSentinel
=============

Generate a StateSentinel, which mocks out the specified object and monitors
attribute access.

Why do this?
------------

Suppose you have a function that receives a reference to an object as an
argument in order to access certain fields, but you also want to guarantee
that this function doesn't have any side effects that change the state of
this object.

Ordinarily, you'd use something like `unittest.mock`, but mock doesn't seem
to have good support for checking attribute access and assignment. So, we
can write our own sentinel that mimics the structure of our object but also
guards against side effects, as well as tracking attribute accesses.

Are there alternative approaches? You could copy the object and check for
changes between the copy and the reference you passed the function, but
this may require writing lots of boilerplate code and messy custom logic.
Instead, why not just add a wrapper that watches for attribute access?

In my opinion, you should just prefer using immutable containers, like
`collections.namedtuple`, for this purpose, rather than passing references
to mutable objects, thus avoiding the problem entirely. But sometimes you
can't.

Usage
-----

See the import guarded block at the bottom of the file for usage and
tests, which describe the desired behavior.
"""
from collections import Counter


class _StateSentinelMixin(object):
    """Generic StateSentinel mixin.

    It turns out trying to achieve genericism through subclassing is kind
    of a pain; maybe we should just go with class-level decoration.
    """

    guard_state = False

    def __init__(self):
        self._attr_accesses = Counter()
        self._attr_access_order = []

    def __setattr__(self, attr, value):
        if self.guard_state:
            # Distinguish between existing attributes being updated and
            # whole new ones being created, since the latter is "even worse"
            if attr not in self.__dict__:
                raise AssertionError("Attemped to add new attribute to state!")
            raise AssertionError('Attempted to modify state!')

        dict.__setattr__(self, attr, value)
        return

    def __getattribute__(self, attr):
        # You have to be pretty hacky when overriding __getattribute__ because
        # it's very easy to get caught up in infinite recursion.
        def _getattr(self, attr, default=None):
            try:
                return object.__getattribute__(self, attr)
            except AttributeError as e:
                if default is None:
                    # FIXME: adjust the traceback to remove noise
                    # from `_StateSentinelMixin`.
                    raise e
                return default

        _ATTR_ACCESSES, _ATTR_ACCESS_ORDER = logging_attrs = \
            ('_attr_accesses', '_attr_access_order')
        assertion_methods = ('assert_accessed_times', 'assert_access_order')

        is_logging = (attr in logging_attrs + assertion_methods)
        # Should slots be available, only allow access to slots.
        in_slots = (attr in _getattr(self, '__slots__', (attr)))

        if not is_logging and in_slots:
            _attr_accesses = _getattr(self, _ATTR_ACCESSES)
            _attr_accesses[attr] += 1
            _getattr(self, _ATTR_ACCESS_ORDER).append(attr)

        return _getattr(self, attr)

    def assert_accessed_times(self, attr, n=1):
        """
        Assert that `sentinel.attr` was accessed `n` times.
        """
        assert self._attr_accesses.get(attr, 0) == n

    def assert_access_order(self, access_order):
        """
        Assert that the order in which attributes of sentinel were accessed was
        `access_order`.
        """
        assert self._attr_access_order == access_order


# Public API
# ----------
def StateSentinel(cls):
    """
    Create a StateSentinel constructor for the given class.

    We could do this using class decoration instead of inheritance, and
    I think that approach would be superior in every way.
    """
    # We need `_StateSentinelMixin` to be first in the mro.
    class _AppliedStateSentinel(_StateSentinelMixin, cls):
        def __init__(self, *args, **kwargs):
            cls.__init__(self, *args, **kwargs)
            _StateSentinelMixin.__init__(self)
            # Now that we're done with initialization, begin guarding state.
            self.guard_state = True

    # Give our class a descriptive name for tracebacks.
    _AppliedStateSentinel.__name__ = "{}Sentinel".format(cls.__name__)

    return _AppliedStateSentinel


# Tests and behavior description
# ------------------------------
if __name__ == "__main__":
    class MyState(object):

        __slots__ = ('state_var')

        def __init__(self, state_var):
            self.state_var = state_var

        def update_state(self, update):
            self.state_var += update

    # Create a StateSentinel constructor for the MyState class.
    MyStateSentinel = StateSentinel(MyState)

    state = MyStateSentinel(4)

    assert state.state_var == 4

    # Whoops, we typo'd! This is similar to using something like
    # autospec when mocking.
    try:
        state.stat_var
    except AttributeError as e:
        # Make sure we get nice names in errors instead of the generic
        # `_AppliedStateSentinel`
        assert MyState.__name__ in str(e)

    # something like `mock.assert_called_once_with(...)`
    state.assert_accessed_times('state_var', 1)
    state.assert_access_order(['state_var'])

    # Check that we're not allowed to explicitly change any state values.
    try:
        state.state_var = 5
        assignment = True
    except AssertionError:
        assignment = False

    assert not assignment
    assert state.state_var == 4

    # Check that we're not allowed to indirectly change any state values,
    # as well.
    try:
        state.update_state(4)
        state_method_update = True
    except AssertionError:
        state_method_update = False

    assert not state_method_update
    assert state.state_var == 4
