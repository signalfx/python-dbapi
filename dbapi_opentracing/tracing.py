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

    def __init__(self, connection, tracer=None, span_tags=None):
        super(ConnectionTracing, self).__init__(connection)
        self._self_tracer = tracer or opentracing.tracer
        self._self_span_tags = span_tags or {}

    def cursor(self, *args, **kwargs):
        return Cursor(self.__wrapped__.cursor(*args, **kwargs), self._self_tracer, self._self_span_tags)

    def commit(self):
        operation_name = _operation_name(self, self.__wrapped__.commit)
        with self._self_tracer.start_active_span(operation_name) as scope:
            scope.span.set_tag(tags.DATABASE_TYPE, 'sql')
            for tag, value in self._self_span_tags.items():
                scope.span.set_tag(tag, value)
            return self.__wrapped__.commit()

    def rollback(self):
        operation_name = _operation_name(self, self.__wrapped__.rollback)
        with self._self_tracer.start_active_span(operation_name) as scope:
            scope.span.set_tag(tags.DATABASE_TYPE, 'sql')
            for tag, value in self._self_span_tags.items():
                scope.span.set_tag(tag, value)
            return self.__wrapped__.rollback()

    def __enter__(self):
        return self.cursor()

    def __exit__(self, exc, value, tb):
        # Pass ConnectionTracing instance to wrapped's __exit__()
        # for traced commit() and rollback()
        return self.__wrapped__.__class__.__exit__(self, exc, value, tb)


class Cursor(wrapt.ObjectProxy):
    """A wrapper for a DB API Cursor object with traced execute(), executemany(), and callproc() methods."""

    def __init__(self, cursor, tracer=None, span_tags=None):
        super(Cursor, self).__init__(cursor)
        self._self_tracer = tracer or opentracing.tracer
        self._self_span_tags = span_tags or {}

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
        return self._traced_execution(self.__wrapped__.execute, *args, **kwargs)

    def executemany(self, *args, **kwargs):
        return self._traced_execution(self.__wrapped__.executemany, *args, **kwargs)

    def callproc(self, *args, **kwargs):
        return self._traced_execution(self.__wrapped__.callproc, *args, **kwargs)

    def __enter__(self):
        return self
