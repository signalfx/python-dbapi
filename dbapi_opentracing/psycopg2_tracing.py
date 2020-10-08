from threading import Lock

from .tracing import _ConnectionTracing, _Cursor, _operation_name

try:
    from psycopg2.extensions import connection as PsycopgConnection
    from psycopg2.extensions import cursor as PsycopgCursor
    from psycopg2.sql import Composed
except ImportError:
    PsycopgConnection = object
    PsycopgCursor = object
    Composed = type('Composed', tuple(), {}) 


class _PsycopgCursorTracing(_Cursor):
    """
    Traced mixin for subclass of psycopg2 cursor.  Intended to be used by connection.cursor(cursor_factory).
    """
    def __init__(self, cursor_factory, tracer=None, span_tags=None, trace_execute=True, trace_executemany=True,
                 trace_callproc=True, *args, **kwargs):
        _Cursor.__init__(self, tracer=tracer, span_tags=span_tags, trace_execute=trace_execute,
                         trace_executemany=trace_executemany, trace_callproc=trace_callproc)
        # Since we should support any psycopg cursor type, proxy methods for traced execution
        self._cursor_factory = cursor_factory

    def _get_statement(self, args):
        if isinstance(args[1], bytes):
            arg = args[1].decode('utf8', 'replace')
        elif isinstance(args[1], Composed):
            if len(args[1].seq) > 0:
                arg = args[1].seq[0].string
            else:
                arg = ''
        else:
            arg = args[1]
        return arg.split(' ')[0]

    def _get_query(self, args):
        query = args[1]
        if isinstance(query, Composed):
            query = query.as_string(self.connection)
        return self._format_query(query)

    def execute(self, *args, **kwargs):
        if not self._self_trace_execute:
            return self._cursor_factory.execute(self, *args, **kwargs)

        return self._traced_execution(self._cursor_factory.execute, self, *args, **kwargs)

    def executemany(self, *args, **kwargs):
        if not self._self_trace_executemany:
            return self._cursor_factory.executemany(self, *args, **kwargs)

        return self._traced_execution(self._cursor_factory.executemany, self, *args, **kwargs)

    def callproc(self, *args, **kwargs):
        if not self._self_trace_callproc:
            return self._cursor_factory.callproc(self, *args, **kwargs)

        return self._traced_execution(self._cursor_factory.callproc, self, *args, **kwargs)


# Storage for CursorFactory classes to prevent redundant definitions
_cursor_factory_classes = {}
_cursor_factory_lock = Lock()


class PsycopgCursorTracing(object):
    """
    Traced psycopg cursor_factory-compatible pseudo-metaclass, which generates and instantiates traced
    cursor_factory subclass.
    """
    def __new__(cls, *args, **kwargs):
        factory = kwargs.pop('cursor_factory', PsycopgCursor)
        with _cursor_factory_lock:
            if factory not in _cursor_factory_classes:

                class CursorFactory(_PsycopgCursorTracing, factory):
                    """Traced cursor_factory instance."""
                    def __init__(self, conn, *a, **kw):
                        # Pop all _PsycopgCursorTracing tracing flags to be able to
                        # pass custom cursor factory (kw)args
                        _PsycopgCursorTracing.__init__(
                            self, cursor_factory=factory,
                            tracer=kw.pop('tracer', None),
                            span_tags=kw.pop('span_tags', None),
                            trace_execute=kw.pop('trace_execute', True),
                            trace_executemany=kw.pop('trace_executemany', True),
                            trace_callproc=kw.pop('trace_callproc', True)
                        )
                        factory.__init__(self, conn, *a, **kw)

                CursorFactory.__name__ = factory.__name__
                _cursor_factory_classes[factory] = CursorFactory

        return _cursor_factory_classes[factory](*args, **kwargs)


class _PsycopgConnectionTracing(_ConnectionTracing):
    """
    Traced mixin for psycopg2 connection.  `connection_factory` should be provided as invoking classes' psycopg
    connection superclass (psycopg.extensions.connection as default) for proxying traced commit and cursor.
    """
    def __init__(self, dsn, connection_factory=PsycopgConnection, cursor_factory=PsycopgCursor, tracer=None,
                 span_tags=None, trace_commit=True, trace_rollback=True, trace_execute=True, trace_executemany=True,
                 trace_callproc=True, *args, **kwargs):
        _ConnectionTracing.__init__(
            self, tracer=tracer, span_tags=span_tags, trace_commit=trace_commit, trace_rollback=trace_rollback,
            trace_execute=trace_execute, trace_executemany=trace_executemany, trace_callproc=trace_callproc
        )
        self._connection_factory = connection_factory
        self._cursor_factory = cursor_factory

        self._commit_operation_name = _operation_name(self, self.commit)
        self._rollback_operation_name = _operation_name(self, self.rollback)

    def cursor(self, name=None, *args, **kwargs):
        trace_execute = kwargs.pop('trace_execute', self._self_trace_execute)
        trace_executemany = kwargs.pop('trace_executemany', self._self_trace_executemany)
        trace_callproc = kwargs.pop('trace_callproc', self._self_trace_callproc)

        cursor_factory = kwargs.pop('cursor_factory', self._cursor_factory)
        return PsycopgCursorTracing(conn=self, name=name, cursor_factory=cursor_factory, tracer=self._self_tracer,
                                    span_tags=self._self_span_tags, trace_execute=trace_execute,
                                    trace_executemany=trace_executemany, trace_callproc=trace_callproc, *args, **kwargs)

    def commit(self):
        if not self._self_trace_commit:
            return self._connection_factory.commit(self)

        return self._traced_execution(self._commit_operation_name, self._connection_factory.commit, self)

    def rollback(self):
        if not self._self_trace_rollback:
            return self._connection_factory.rollback(self)

        return self._traced_execution(self._rollback_operation_name, self._connection_factory.rollback, self)


# Storage for ConnectionFactory classes to prevent redundant definitions
_connection_factory_classes = {}
_connection_factory_lock = Lock()


class PsycopgConnectionTracing(object):
    """
    Traced psycopg connection_factory-compatible pseudo-metaclass, which generates and instantiates traced
    connection_factory subclass.

    connection = psycopg2.connect(dsn, connection_factory=PsycopgConnectionTracing)
    assert isinstance(connection, psycopg2.extensions.connection)

    or

    connection = psycopg2.connect(
        dsn, connection_factory=lambda dsn: PsycopgConnectionTracing(
            dsn, connection_factory=LogicalReplicationConnection
        )
    )
    assert isinstance(connection, LogicalReplicationConnection)
    """
    def __new__(cls, *args, **kwargs):
        factory = kwargs.pop('connection_factory', PsycopgConnection)

        with _connection_factory_lock:
            if factory not in _cursor_factory_classes:

                class ConnectionFactory(_PsycopgConnectionTracing, factory):

                    def __init__(self, dsn, *a, **kw):
                        # Pop all _PsycopgConnectionTracing tracing flags to be able to
                        # pass custom connection factory (kw)args
                        pct_args = dict(
                            dsn=dsn, connection_factory=factory,
                            tracer=kw.pop('tracer', None),
                            span_tags=kw.pop('span_tags', None),
                            trace_commit=kw.pop('trace_commit', True),
                            trace_rollback=kw.pop('trace_rollback', True),
                            trace_execute=kw.pop('trace_execute', True),
                            trace_executemany=kw.pop('trace_executemany', True),
                            trace_callproc=kw.pop('trace_callproc', True)
                        )
                        if 'cursor_factory' in kw:
                            pct_args['cursor_factory'] = kw['cursor_factory']

                        _PsycopgConnectionTracing.__init__(self, **pct_args)
                        factory.__init__(self, dsn, *a, **kw)

                ConnectionFactory.__name__ = factory.__name__
                _connection_factory_classes[factory] = ConnectionFactory

        return _connection_factory_classes[factory](*args, **kwargs)
