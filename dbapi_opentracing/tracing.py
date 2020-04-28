import traceback

from opentracing.ext import tags
import opentracing
import wrapt


def _operation_name(caller, func, statement=''):
    """Span operation name obtained from caller's method and sql statement, if any."""
    class_name = caller.__class__.__name__
    operation_name = func.__name__
    if isinstance(statement, bytes):
        statement = statement.decode('utf8', 'replace')
    return u'{}.{}({})'.format(class_name, operation_name, statement)


class _ConnectionTracing(object):
    """
    Base for traced connections.  Tracer and trace flag attributes will be in ObjectProxy attribute format despite not
    having direct wrapt parent class, to ensure their functionality in ConnectionTracing.
    """

    def __init__(self, tracer=None, span_tags=None, trace_commit=True, trace_rollback=True, trace_execute=True,
                 trace_executemany=True, trace_callproc=True, *args, **kwargs):
        self._self_tracer = tracer or opentracing.tracer
        self._self_span_tags = span_tags or {}
        self._self_trace_commit = trace_commit
        self._self_trace_rollback = trace_rollback
        self._self_trace_execute = trace_execute
        self._self_trace_executemany = trace_executemany
        self._self_trace_callproc = trace_callproc

    def _traced_execution(self, operation_name, func, *args, **kwargs):
        """Execute function under active span and return its value"""
        with self._self_tracer.start_active_span(operation_name) as scope:
            span = scope.span
            span.set_tag(tags.DATABASE_TYPE, 'sql')
            span.set_tag(tags.SPAN_KIND, tags.SPAN_KIND_RPC_CLIENT)

            for tag, value in self._self_span_tags.items():
                scope.span.set_tag(tag, value)

            try:
                val = func(*args, **kwargs)
            except Exception as e:
                span.set_tag(tags.ERROR, True)
                span.set_tag('sfx.error.message', str(e))
                span.set_tag('sfx.error.object', str(e.__class__))
                span.set_tag('sfx.error.kind', e.__class__.__name__)
                span.set_tag('sfx.error.stack', traceback.format_exc())
                raise
            return val

    def __enter__(self):
        return self.cursor()


class ConnectionTracing(_ConnectionTracing, wrapt.ObjectProxy):
    """A wrapper for instantiated DB API Connection objects with traced commit() and rollback() methods."""

    def __init__(self, connection, tracer=None, span_tags=None, trace_commit=True, trace_rollback=True,
                 trace_execute=True, trace_executemany=True, trace_callproc=True, *args, **kwargs):
        wrapt.ObjectProxy.__init__(self, connection)
        _ConnectionTracing.__init__(self, tracer, span_tags, trace_commit, trace_rollback, trace_execute,
                                    trace_executemany, trace_callproc)

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

        return self._traced_execution(self._self_commit_operation_name, self.__wrapped__.commit)

    def rollback(self):
        if not self._self_trace_rollback:
            return self.__wrapped__.rollback()

        return self._traced_execution(self._self_rollback_operation_name, self.__wrapped__.rollback)

    def __exit__(self, exc, value, tb):
        # C extension clients (e.g. psycopg2) require self.__wrapped__.__class__ to be in ConnectionTracing.__bases__,
        # which wrapt cannot provide due to __slots__ conflict.  We need to trace w/ a best-guess operation here instead
        # of passing self to self.__wrapped__.__class__.__exit__() as we can w/ pure python clients. For psycopg tracing
        # in general, using PsycopgConnectionTracing is required for full functionality to allow traced commit/rollback
        # from __exit__() as well as compatibility with extensions and extras.
        if exc:
            if not self._self_trace_rollback:
                return self.__wrapped__.__exit__(exc, value, tb)
            operation_name = self._self_rollback_operation_name
        else:
            if not self._self_trace_commit:
                return self.__wrapped__.__exit__(exc, value, tb)
            operation_name = self._self_commit_operation_name

        return self._traced_execution(operation_name, self.__wrapped__.__exit__, exc, value, tb)


class _Cursor(object):
    """
    Base for traced cursors.  Tracer and trace flag attributes will be in ObjectProxy attribute format despite not
    having direct wrapt parent class, to ensure their functionality in CursorTracing.
    """

    def __init__(self, tracer=None, span_tags=None, trace_execute=True, trace_executemany=True, trace_callproc=True,
                 *args, **kwargs):
        self._self_tracer = tracer or opentracing.tracer
        self._self_span_tags = span_tags or {}
        self._self_trace_execute = trace_execute
        self._self_trace_executemany = trace_executemany
        self._self_trace_callproc = trace_callproc

    def _get_statement(self, args):
        """Converts _traced_execution() `args` to partial operation name statement"""
        raise NotImplementedError

    def _get_query(self, args):
        """Converts _traced_execution() `args` to db.statement tag value"""
        raise NotImplementedError

    def _format_query(self, query):
        if isinstance(query, bytes):
            return query.decode('utf8', 'replace')
        return query

    def _traced_execution(self, func, *args, **kwargs):
        statement = self._get_statement(args)
        operation_name = _operation_name(self, func, statement)
        with self._self_tracer.start_active_span(operation_name) as scope:
            span = scope.span
            span.set_tag(tags.DATABASE_TYPE, 'sql')
            span.set_tag(tags.SPAN_KIND, tags.SPAN_KIND_RPC_CLIENT)
            span.set_tag(tags.DATABASE_STATEMENT, self._get_query(args))

            for tag, value in self._self_span_tags.items():
                span.set_tag(tag, value)

            try:
                val = func(*args, **kwargs)
            except Exception as e:
                span.set_tag(tags.ERROR, True)
                span.set_tag('sfx.error.message', str(e))
                span.set_tag('sfx.error.object', str(e.__class__))
                span.set_tag('sfx.error.kind', e.__class__.__name__)
                span.set_tag('sfx.error.stack', traceback.format_exc())
                raise
            span.set_tag('db.rows_produced', self.rowcount)
        return val

    def __enter__(self):
        return self


class Cursor(_Cursor, wrapt.ObjectProxy):
    """A wrapper for a DB API Cursor object with traced execute(), executemany(), and callproc() methods."""

    def __init__(self, cursor, tracer=None, span_tags=None, trace_execute=True, trace_executemany=True,
                 trace_callproc=True, *args, **kwargs):
        wrapt.ObjectProxy.__init__(self, cursor)
        _Cursor.__init__(self, tracer, span_tags, trace_execute, trace_executemany, trace_callproc)

    def _get_statement(self, args):
        if isinstance(args[0], bytes):
            arg = args[0].decode('utf8', 'replace')
        else:
            arg = args[0]
        return arg.split(' ')[0]

    def _get_query(self, args):
        return self._format_query(args[0])

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
