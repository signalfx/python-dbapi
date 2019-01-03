import traceback

from opentracing.ext import tags
import opentracing
import wrapt


def _operation_name(caller, function, statement=''):
    """Span operation name obtained from caller's method and sql statement, if any."""
    class_name = caller.__class__.__name__
    operation_name = function.__name__
    return '{}.{}({})'.format(class_name, operation_name, statement)


class ConnectionTracing(wrapt.ObjectProxy):
    """A wrapper for a DB API Connection object with traced commit() and rollback() methods."""

    def __init__(self, connection, tracer=None, span_tags=None,
                 trace_commit=True, trace_rollback=True, trace_execute=True,
                 trace_executemany=True, trace_callproc=True, *args, **kwargs):
        super(ConnectionTracing, self).__init__(connection)
        self._self_tracer = tracer or opentracing.tracer
        self._self_span_tags = span_tags or {}
        self._self_trace_commit = trace_commit
        self._self_trace_rollback = trace_rollback
        self._self_trace_execute = trace_execute
        self._self_trace_executemany = trace_executemany
        self._self_trace_callproc = trace_callproc
        self._self_commit_operation_name = _operation_name(self, self.__wrapped__.commit)
        self._self_rollback_operation_name = _operation_name(self, self.__wrapped__.rollback)

    def cursor(self, *args, **kwargs):
        trace_execute = kwargs.pop('trace_execute', self._self_trace_execute)
        trace_executemany = kwargs.pop('trace_executemany', self._self_trace_executemany)
        trace_callproc = kwargs.pop('trace_callproc', self._self_trace_callproc)
        return Cursor(self.__wrapped__.cursor(*args, **kwargs), self._self_tracer, self._self_span_tags,
                      trace_execute=trace_execute, trace_executemany=trace_executemany, trace_callproc=trace_callproc)

    def commit(self):
        if not self._self_trace_commit:
            return self.__wrapped__.commit()

        with self._self_tracer.start_active_span(self._self_commit_operation_name) as scope:
            scope.span.set_tag(tags.DATABASE_TYPE, 'sql')
            for tag, value in self._self_span_tags.items():
                scope.span.set_tag(tag, value)
            return self.__wrapped__.commit()

    def rollback(self):
        if not self._self_trace_rollback:
            return self.__wrapped__.rollback()

        with self._self_tracer.start_active_span(self._self_rollback_operation_name) as scope:
            scope.span.set_tag(tags.DATABASE_TYPE, 'sql')
            for tag, value in self._self_span_tags.items():
                scope.span.set_tag(tag, value)
            return self.__wrapped__.rollback()

    def __enter__(self):
        return self.cursor()

    def __exit__(self, exc, value, tb):
        # C extension clients (e.g. psycopg2) require self.__wrapped__.__class__ to be in ConnectionTracing.__bases__,
        # which wrapt doesn't provide.  We need to trace w/ a best-guess operation here instead of passing self to
        # self.__wrapped__.__class__.__exit__() as we can w/ pure python clients.
        if exc:
            if not self._self_trace_rollback:
                return self.__wrapped__.__exit__(exc, value, tb)
            operation_name = self._self_rollback_operation_name
        else:
            if not self._self_trace_commit:
                return self.__wrapped__.__exit__(exc, value, tb)
            operation_name = self._self_commit_operation_name

        with self._self_tracer.start_active_span(operation_name) as scope:
            scope.span.set_tag(tags.DATABASE_TYPE, 'sql')
            for tag, value in self._self_span_tags.items():
                scope.span.set_tag(tag, value)
            return self.__wrapped__.__exit__(exc, value, tb)


class Cursor(wrapt.ObjectProxy):
    """A wrapper for a DB API Cursor object with traced execute(), executemany(), and callproc() methods."""

    def __init__(self, cursor, tracer=None, span_tags=None,
                 trace_execute=True, trace_executemany=True, trace_callproc=True, *args, **kwargs):
        super(Cursor, self).__init__(cursor)
        self._self_tracer = tracer or opentracing.tracer
        self._self_span_tags = span_tags or {}
        self._self_trace_execute = trace_execute
        self._self_trace_executemany = trace_executemany
        self._self_trace_callproc = trace_callproc

    def _format_query(self, query):
        return ' '.join(query.split(' '))

    def _traced_execution(self, function, *args, **kwargs):
        statement = args[0].split(' ')[0]
        operation_name = _operation_name(self, function, statement)
        with self._self_tracer.start_active_span(operation_name) as scope:
            span = scope.span
            span.set_tag(tags.DATABASE_TYPE, 'sql')
            span.set_tag(tags.DATABASE_STATEMENT, self._format_query(args[0]))
            for tag, value in self._self_span_tags.items():
                span.set_tag(tag, value)
            try:
                val = function(*args, **kwargs)
            except Exception:
                span.set_tag(tags.ERROR, True)
                span.log_kv({'event': 'error',
                             'error.object': traceback.format_exc()})
                raise
            span.set_tag('db.rows_produced', self.rowcount)
        return val

    def execute(self, *args, **kwargs):
        if not self._self_trace_execute:
            return self.__wrapped__.execute(*args, **kwargs)

        return self._traced_execution(self.__wrapped__.execute, *args, **kwargs)

    def executemany(self, *args, **kwargs):
        if not self._self_trace_executemany:
            return self.__wrapped__.executemany(*args, **kwargs)

        return self._traced_execution(self.__wrapped__.executemany, *args, **kwargs)

    def callproc(self, *args, **kwargs):
        if not self._self_trace_callproc:
            return self.__wrapped__.callproc(*args, **kwargs)

        return self._traced_execution(self.__wrapped__.callproc, *args, **kwargs)

    def __enter__(self):
        return self
