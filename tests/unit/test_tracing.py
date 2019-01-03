# Copyright (C) 2018 SignalFx, Inc. All rights reserved.
import types

from opentracing.mocktracer import MockTracer
from opentracing.ext import tags
from mock import Mock, patch
import pytest

from dbapi_opentracing.tracing import ConnectionTracing

row_count = 'SomeRowCount'


class SomeException(Exception):
    pass


class MockDBAPICursor(Mock):
    execute = Mock(spec=types.MethodType)
    execute.__name__ = 'execute'

    executemany = Mock(spec=types.MethodType)
    executemany.__name__ = 'executemany'

    callproc = Mock(spec=types.MethodType)
    callproc.__name__ = 'callproc'

    rowcount = row_count

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return self


class MockDBAPIConnection(Mock):
    cursor = Mock(spec=types.MethodType, return_value=MockDBAPICursor())

    commit = Mock(spec=types.MethodType)
    commit.__name__ = 'commit'

    rollback = Mock(spec=types.MethodType)
    rollback.__name__ = 'rollback'

    def __exit__(self, exc, value, tb):
        if exc:
            return self.rollback()
        return self.commit()


class DBAPITestSuite(object):

    @pytest.fixture(autouse=True)
    def setup(self):
        self.tracer = MockTracer()
        self.dbapi_connection = MockDBAPIConnection()
        self.connection = ConnectionTracing(self.dbapi_connection, self.tracer)


class TestConnectionTracingCursorContext(DBAPITestSuite):

    def test_execute_is_traced(self):
        statement = 'SELECT * FROM SOME_TABLE'
        with self.connection.cursor() as cursor:
            cursor.execute(statement)
        spans = self.tracer.finished_spans()
        assert len(spans) == 1
        span = spans.pop()
        assert span.operation_name == 'MockDBAPICursor.execute(SELECT)'
        assert span.tags[tags.DATABASE_TYPE] == 'sql'
        assert span.tags[tags.DATABASE_STATEMENT] == statement
        assert span.tags['db.rows_produced'] == row_count

    def test_executemany_is_traced(self):
        statement = 'DROP DB'
        with self.connection.cursor() as cursor:
            cursor.executemany(statement)
        spans = self.tracer.finished_spans()
        assert len(spans) == 1
        span = spans.pop()
        assert span.operation_name == 'MockDBAPICursor.executemany(DROP)'
        assert span.tags[tags.DATABASE_TYPE] == 'sql'
        assert span.tags[tags.DATABASE_STATEMENT] == statement
        assert span.tags['db.rows_produced'] == row_count

    def test_callproc_is_traced(self):
        procedure = 'my_procedure'
        with self.connection.cursor() as cursor:
            cursor.callproc(procedure)
        spans = self.tracer.finished_spans()
        assert len(spans) == 1
        span = spans.pop()
        assert span.operation_name == 'MockDBAPICursor.callproc(my_procedure)'
        assert span.tags[tags.DATABASE_TYPE] == 'sql'
        assert span.tags[tags.DATABASE_STATEMENT] == procedure
        assert span.tags['db.rows_produced'] == row_count


class TestConnectionTracingCursorWhitelist(DBAPITestSuite):

    def test_execute_is_not_traced(self):
        with self.connection.cursor(trace_execute=False) as cursor:
            cursor.execute('SELECT * FROM SOME_TABLE')
        assert not self.tracer.finished_spans()

    def test_executemany_is_not_traced(self):
        with self.connection.cursor(trace_executemany=False) as cursor:
            cursor.executemany('DROP DB')
        assert not self.tracer.finished_spans()

    def test_callproc_is_not_traced(self):
        with self.connection.cursor(trace_callproc=False) as cursor:
            cursor.callproc('my_procedure')
        assert not self.tracer.finished_spans()


class TestConnectionTracingConnectionContext(DBAPITestSuite):

    def test_execute_and_commit_are_traced(self):
        statement = 'SELECT * FROM SOME_TABLE'
        with self.connection as cursor:
            cursor.execute(statement)
        spans = self.tracer.finished_spans()
        assert len(spans) == 2
        execute, commit = spans
        assert execute.operation_name == 'MockDBAPICursor.execute(SELECT)'
        assert execute.tags[tags.DATABASE_TYPE] == 'sql'
        assert execute.tags[tags.DATABASE_STATEMENT] == statement
        assert execute.tags['db.rows_produced'] == row_count
        assert commit.operation_name == 'MockDBAPIConnection.commit()'
        assert commit.tags == {tags.DATABASE_TYPE: 'sql'}

    def test_executemany_and_commit_are_traced(self):
        statement = 'INSERT INTO some_table VALUES (%s, %s, %s)'
        with self.connection as cursor:
            cursor.executemany(statement)
        spans = self.tracer.finished_spans()
        assert len(spans) == 2
        executemany, commit = spans
        assert executemany.operation_name == 'MockDBAPICursor.executemany(INSERT)'
        assert executemany.tags[tags.DATABASE_TYPE] == 'sql'
        assert executemany.tags[tags.DATABASE_STATEMENT] == statement
        assert executemany.tags['db.rows_produced'] == row_count
        assert commit.operation_name == 'MockDBAPIConnection.commit()'
        assert commit.tags == {tags.DATABASE_TYPE: 'sql'}

    def test_callproc_and_commit_are_traced(self):
        procedure = 'my_procedure'
        with self.connection as cursor:
            cursor.callproc(procedure)
        spans = self.tracer.finished_spans()
        assert len(spans) == 2
        callproc, commit = spans
        assert callproc.operation_name == 'MockDBAPICursor.callproc(my_procedure)'
        assert callproc.tags[tags.DATABASE_TYPE] == 'sql'
        assert callproc.tags[tags.DATABASE_STATEMENT] == procedure
        assert callproc.tags['db.rows_produced'] == row_count
        assert commit.operation_name == 'MockDBAPIConnection.commit()'
        assert commit.tags == {tags.DATABASE_TYPE: 'sql'}

    def test_execute_and_rollback_are_traced(self):
        statement = 'SELECT * FROM some_table'
        with self.connection as cursor:
            with patch.object(MockDBAPICursor, 'execute', side_effect=SomeException()) as execute:
                execute.__name__ = 'execute'
                cursor.execute(statement)
        spans = self.tracer.finished_spans()
        assert len(spans) == 2
        execute, rollback = spans
        assert execute.operation_name == 'MockDBAPICursor.execute(SELECT)'
        assert execute.tags[tags.DATABASE_TYPE] == 'sql'
        assert execute.tags[tags.DATABASE_STATEMENT] == statement
        assert execute.tags[tags.ERROR] is True
        assert 'db.rows_produced' not in execute.tags
        assert 'SomeException' in execute.logs[0].key_values['error.object']
        assert rollback.operation_name == 'MockDBAPIConnection.rollback()'
        assert rollback.tags == {tags.DATABASE_TYPE: 'sql'}

    def test_executemany_and_rollback_are_traced(self):
        statement = 'INSERT INTO some_table VALUES (%s, %s, %s)'
        with self.connection as cursor:
            with patch.object(MockDBAPICursor, 'executemany', side_effect=SomeException()) as executemany:
                executemany.__name__ = 'executemany'
                cursor.executemany(statement)
        spans = self.tracer.finished_spans()
        assert len(spans) == 2
        executemany, rollback = spans
        assert executemany.operation_name == 'MockDBAPICursor.executemany(INSERT)'
        assert executemany.tags[tags.DATABASE_TYPE] == 'sql'
        assert executemany.tags[tags.DATABASE_STATEMENT] == statement
        assert executemany.tags[tags.ERROR] is True
        assert 'db.rows_produced' not in executemany.tags
        assert 'SomeException' in executemany.logs[0].key_values['error.object']

    def test_callproc_and_rollback_are_traced(self):
        procedure = 'my_procedure'
        with self.connection as cursor:
            with patch.object(MockDBAPICursor, 'callproc', side_effect=SomeException()) as callproc:
                callproc.__name__ = 'callproc'
                cursor.callproc(procedure)
        spans = self.tracer.finished_spans()
        assert len(spans) == 2
        callproc, rollback = spans
        assert callproc.operation_name == 'MockDBAPICursor.callproc(my_procedure)'
        assert callproc.tags[tags.DATABASE_TYPE] == 'sql'
        assert callproc.tags[tags.DATABASE_STATEMENT] == procedure
        assert callproc.tags[tags.ERROR] is True
        assert 'db.rows_produced' not in callproc.tags
        assert 'SomeException' in callproc.logs[0].key_values['error.object']
        assert rollback.operation_name == 'MockDBAPIConnection.rollback()'
        assert rollback.tags == {tags.DATABASE_TYPE: 'sql'}


class TestConnectionTracingConnectionContextWhitelist(DBAPITestSuite):

    def test_execute_and_commit_are_not_traced(self):
        connection = ConnectionTracing(self.dbapi_connection, self.tracer,
                                       trace_execute=False, trace_commit=False)
        with connection as cursor:
            cursor.execute('SELECT * FROM SOME_TABLE')
        assert not self.tracer.finished_spans()

    def test_executemany_and_commit_are_not_traced(self):
        connection = ConnectionTracing(self.dbapi_connection, self.tracer,
                                       trace_executemany=False, trace_commit=False)
        with connection as cursor:
            cursor.executemany('INSERT INTO some_table VALUES (%s, %s, %s)')
        assert not self.tracer.finished_spans()

    def test_callproc_and_commit_are_not_traced(self):
        connection = ConnectionTracing(self.dbapi_connection, self.tracer,
                                       trace_callproc=False, trace_commit=False)
        with connection as cursor:
            cursor.callproc('my_procedure')
        assert not self.tracer.finished_spans()

    def test_execute_and_rollback_are_not_traced(self):
        connection = ConnectionTracing(self.dbapi_connection, self.tracer,
                                       trace_execute=False, trace_rollback=False)
        with connection as cursor:
            with patch.object(MockDBAPICursor, 'execute', side_effect=SomeException()) as execute:
                execute.__name__ = 'execute'
                cursor.execute('SELECT * FROM some_table')
        assert not self.tracer.finished_spans()

    def test_executemany_and_rollback_are_not_traced(self):
        connection = ConnectionTracing(self.dbapi_connection, self.tracer,
                                       trace_executemany=False, trace_rollback=False)
        with connection as cursor:
            with patch.object(MockDBAPICursor, 'executemany', side_effect=SomeException()) as executemany:
                executemany.__name__ = 'executemany'
                cursor.executemany('INSERT INTO some_table VALUES (%s, %s, %s)')
        assert not self.tracer.finished_spans()

    def test_callproc_and_rollback_are_not_traced(self):
        connection = ConnectionTracing(self.dbapi_connection, self.tracer,
                                       trace_callproc=False, trace_rollback=False)
        with connection as cursor:
            with patch.object(MockDBAPICursor, 'callproc', side_effect=SomeException()) as callproc:
                callproc.__name__ = 'callproc'
                cursor.callproc('my_procedure')
        assert not self.tracer.finished_spans()


class TestConnectionTracing(object):

    def test_custom_span_tags(self):
        span_tags = dict(one=123, two=234)
        tracer = MockTracer()
        connection = ConnectionTracing(MockDBAPIConnection(), tracer, span_tags=span_tags)

        with connection as cursor:
            cursor.execute('insert')
            cursor.executemany('insert', [1, 2])
            cursor.callproc('procedure')

        spans = tracer.finished_spans()
        assert len(spans) == 4
        for span in spans:
            assert span.tags[tags.DATABASE_TYPE] == 'sql'
            assert span.tags['one'] == 123
            assert span.tags['two'] == 234
