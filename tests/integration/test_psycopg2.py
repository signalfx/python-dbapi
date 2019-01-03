# Copyright (C) 2018 SignalFx, Inc. All rights reserved.
from datetime import datetime
from time import sleep
import os.path

from opentracing.mocktracer import MockTracer
from psycopg2.extras import DictCursor
from opentracing.ext import tags
import psycopg2
import docker
import pytest

from dbapi_opentracing import ConnectionTracing
from .conftest import DBAPITest


@pytest.fixture(scope='session')
def postgres_container():
    client = docker.from_env()

    env = dict(POSTGRES_USER='postgres', POSTGRES_PASSWORD='pass', POSTGRES_DB='test_db')
    initdb_d = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'psycopg2/initdb.d')
    volumes = ['{}:/docker-entrypoint-initdb.d'.format(initdb_d)]
    postgres = client.containers.run('postgres:latest', environment=env, ports={'5432/tcp': 5432},
                                     volumes=volumes, detach=True)
    try:
        yield postgres
    finally:
        postgres.remove(force=True)


class Psycopg2Test(DBAPITest):

    @pytest.fixture
    def connection_tracing(self, postgres_container):
        while True:
            try:
                conn = psycopg2.connect(host='127.0.0.1', user='test_user', password='test_password',
                                        dbname='test_db', port=5432, options='-c search_path=test_schema')
                break
            except psycopg2.OperationalError:
                sleep(.25)
        tracer = MockTracer()
        return tracer, ConnectionTracing(conn, tracer, span_tags=dict(custom='tag'))


class TestPsycopgCursorContext(Psycopg2Test):

    def test_successful_execute(self, connection_tracing):
        tracer, conn = connection_tracing
        with tracer.start_active_span('Parent'):
            with conn.cursor(cursor_factory=DictCursor) as cursor:
                cursor.execute('insert into table_one values (%s, %s, %s, %s)',
                               (self.random_string(), self.random_string(),
                                datetime.now(), datetime.now()))
                cursor.execute('insert into table_two values (%s, %s, %s, %s)',
                               (self.random_int(), self.random_int(),
                                self.random_float(), self.random_float()))
            conn.commit()
        spans = tracer.finished_spans()
        assert len(spans) == 4
        first, second, commit, parent = spans
        for span in (first, second):
            assert span.operation_name == 'DictCursor.execute(insert)'
            assert span.tags['custom'] == 'tag'
            assert span.tags[tags.DATABASE_TYPE] == 'sql'
            assert span.tags['db.rows_produced'] == 1
            assert span.parent_id == parent.context.span_id
            assert tags.ERROR not in span.tags
        assert first.tags[tags.DATABASE_STATEMENT] == 'insert into table_one values (%s, %s, %s, %s)'
        assert second.tags[tags.DATABASE_STATEMENT] == 'insert into table_two values (%s, %s, %s, %s)'
        assert commit.operation_name == 'connection.commit()'
        assert parent.operation_name == 'Parent'

    def test_unsuccessful_execute(self, connection_tracing):
        one = self.random_string()
        two = self.random_string()
        tracer, conn = connection_tracing
        with tracer.start_active_span('Parent'):
            with pytest.raises(psycopg2.IntegrityError):
                with conn.cursor(cursor_factory=DictCursor) as cursor:
                    cursor.execute('insert into table_one values (%s, %s, %s, %s)',
                                   (one, two, datetime.now(), datetime.now()))
                    cursor.execute('insert into table_one values (%s, %s, %s, %s)',
                                   (one, two, datetime.now(), datetime.now()))
            conn.commit()
        spans = tracer.finished_spans()
        assert len(spans) == 4
        first, second, commit, parent = spans
        for span in (first, second):
            assert span.operation_name == 'DictCursor.execute(insert)'
            assert span.tags['custom'] == 'tag'
            assert span.tags[tags.DATABASE_TYPE] == 'sql'
            assert span.tags[tags.DATABASE_STATEMENT] == 'insert into table_one values (%s, %s, %s, %s)'
            assert span.parent_id == parent.context.span_id
        assert first.tags['db.rows_produced'] == 1
        assert 'db.rows_produced' not in second.tags
        assert tags.ERROR not in first.tags
        assert second.tags[tags.ERROR] is True
        assert commit.operation_name == 'connection.commit()'
        assert parent.operation_name == 'Parent'

    def test_successful_executemany(self, connection_tracing):
        tracer, conn = connection_tracing
        with tracer.start_active_span('Parent'):
            with conn.cursor(cursor_factory=DictCursor) as cursor:
                cursor.executemany('insert into table_one values (%s, %s, %s, %s)',
                                   [(self.random_string(), self.random_string(),
                                     datetime.now(), datetime.now()),
                                    (self.random_string(), self.random_string(),
                                     datetime.now(), datetime.now())])
                cursor.executemany('insert into table_two values (%s, %s, %s, %s)',
                                   [(self.random_int(), self.random_int(),
                                     self.random_float(), self.random_float()),
                                    (self.random_int(), self.random_int(),
                                     self.random_float(), self.random_float())])
            conn.commit()
        spans = tracer.finished_spans()
        assert len(spans) == 4
        first, second, commit, parent = spans
        for span in (first, second):
            assert span.operation_name == 'DictCursor.executemany(insert)'
            assert span.tags['custom'] == 'tag'
            assert span.tags[tags.DATABASE_TYPE] == 'sql'
            assert span.tags['db.rows_produced'] == 2
            assert span.parent_id == parent.context.span_id
            assert tags.ERROR not in span.tags
        assert first.tags[tags.DATABASE_STATEMENT] == 'insert into table_one values (%s, %s, %s, %s)'
        assert second.tags[tags.DATABASE_STATEMENT] == 'insert into table_two values (%s, %s, %s, %s)'
        assert commit.operation_name == 'connection.commit()'
        assert parent.operation_name == 'Parent'

    def test_unsuccessful_executemany(self, connection_tracing):
        one, two, three, four = [self.random_string() for _ in range(4)]
        two = self.random_string()
        tracer, conn = connection_tracing
        with tracer.start_active_span('Parent'):
            with pytest.raises(psycopg2.IntegrityError):
                with conn.cursor(cursor_factory=DictCursor) as cursor:
                    cursor.executemany('insert into table_one values (%s, %s, %s, %s)',
                                       [(one, two, datetime.now(), datetime.now()),
                                        (three, four, datetime.now(), datetime.now())])
                    cursor.executemany('insert into table_one values (%s, %s, %s, %s)',
                                       [(one, two, datetime.now(), datetime.now()),
                                        (three, four, datetime.now(), datetime.now())])
            conn.commit()
        spans = tracer.finished_spans()
        assert len(spans) == 4
        first, second, commit, parent = spans
        for span in (first, second):
            assert span.operation_name == 'DictCursor.executemany(insert)'
            assert span.tags['custom'] == 'tag'
            assert span.tags[tags.DATABASE_TYPE] == 'sql'
            assert span.tags[tags.DATABASE_STATEMENT] == 'insert into table_one values (%s, %s, %s, %s)'
            assert span.parent_id == parent.context.span_id
        assert first.tags['db.rows_produced'] == 2
        assert 'db.rows_produced' not in second.tags
        assert tags.ERROR not in first.tags
        assert second.tags[tags.ERROR] is True
        assert commit.operation_name == 'connection.commit()'
        assert parent.operation_name == 'Parent'

    def test_successful_callproc(self, connection_tracing):
        tracer, conn = connection_tracing
        with tracer.start_active_span('Parent'):
            with conn.cursor(cursor_factory=DictCursor) as cursor:
                cursor.callproc('test_function_one')
                cursor.callproc('test_function_two')
            conn.commit()
        spans = tracer.finished_spans()
        assert len(spans) == 4
        first, second, commit, parent = spans
        for span in (first, second):
            assert span.tags['custom'] == 'tag'
            assert span.tags[tags.DATABASE_TYPE] == 'sql'
            assert span.tags['db.rows_produced']  # don't make assumptions about db state
            assert span.parent_id == parent.context.span_id
            assert tags.ERROR not in span.tags
        assert first.tags[tags.DATABASE_STATEMENT] == 'test_function_one'
        assert second.tags[tags.DATABASE_STATEMENT] == 'test_function_two'
        assert first.operation_name == 'DictCursor.callproc(test_function_one)'
        assert second.operation_name == 'DictCursor.callproc(test_function_two)'
        assert commit.operation_name == 'connection.commit()'
        assert parent.operation_name == 'Parent'

    def test_unsuccessful_callproc(self, connection_tracing):
        tracer, conn = connection_tracing
        with tracer.start_active_span('Parent'):
            with pytest.raises(psycopg2.ProgrammingError):
                with conn.cursor(cursor_factory=DictCursor) as cursor:
                    cursor.callproc('test_function_one')
                    cursor.callproc('not_a_function')
            conn.commit()
        spans = tracer.finished_spans()
        assert len(spans) == 4
        first, second, commit, parent = spans
        for span in (first, second):
            assert span.tags['custom'] == 'tag'
            assert span.tags[tags.DATABASE_TYPE] == 'sql'
            assert span.parent_id == parent.context.span_id
        assert first.operation_name == 'DictCursor.callproc(test_function_one)'
        assert second.operation_name == 'DictCursor.callproc(not_a_function)'
        assert first.tags[tags.DATABASE_STATEMENT] == 'test_function_one'
        assert second.tags[tags.DATABASE_STATEMENT] == 'not_a_function'
        assert first.tags['db.rows_produced']
        assert 'db.rows_produced' not in second.tags
        assert tags.ERROR not in first.tags
        assert second.tags[tags.ERROR] is True
        assert commit.operation_name == 'connection.commit()'
        assert parent.operation_name == 'Parent'


class TestPsycopgConnectionContext(Psycopg2Test):

    def test_successful_execute(self, connection_tracing):
        tracer, conn = connection_tracing
        with tracer.start_active_span('Parent'):
            with conn as cursor:
                cursor.execute('insert into table_one values (%s, %s, %s, %s)',
                               (self.random_string(), self.random_string(),
                                datetime.now(), datetime.now()))
                cursor.execute('insert into table_two values (%s, %s, %s, %s)',
                               (self.random_int(), self.random_int(),
                                self.random_float(), self.random_float()))
        spans = tracer.finished_spans()
        assert len(spans) == 4
        first, second, commit, parent = spans
        for span in (first, second):
            assert span.operation_name == 'cursor.execute(insert)'
            assert span.tags['custom'] == 'tag'
            assert span.tags[tags.DATABASE_TYPE] == 'sql'
            assert span.tags['db.rows_produced'] == 1
            assert span.parent_id == parent.context.span_id
            assert tags.ERROR not in span.tags
        assert first.tags[tags.DATABASE_STATEMENT] == 'insert into table_one values (%s, %s, %s, %s)'
        assert second.tags[tags.DATABASE_STATEMENT] == 'insert into table_two values (%s, %s, %s, %s)'
        assert commit.operation_name == 'connection.commit()'
        assert parent.operation_name == 'Parent'

    def test_unsuccessful_execute(self, connection_tracing):
        one = self.random_string()
        two = self.random_string()
        tracer, conn = connection_tracing
        with tracer.start_active_span('Parent'):
            with pytest.raises(psycopg2.IntegrityError):
                with conn as cursor:
                    cursor.execute('insert into table_one values (%s, %s, %s, %s)',
                                   (one, two, datetime.now(), datetime.now()))
                    cursor.execute('insert into table_one values (%s, %s, %s, %s)',
                                   (one, two, datetime.now(), datetime.now()))
        spans = tracer.finished_spans()
        assert len(spans) == 4
        first, second, rollback, parent = spans
        for span in (first, second):
            assert span.operation_name == 'cursor.execute(insert)'
            assert span.tags['custom'] == 'tag'
            assert span.tags[tags.DATABASE_TYPE] == 'sql'
            assert span.tags[tags.DATABASE_STATEMENT] == 'insert into table_one values (%s, %s, %s, %s)'
            assert span.parent_id == parent.context.span_id
        assert first.tags['db.rows_produced'] == 1
        assert 'db.rows_produced' not in second.tags
        assert tags.ERROR not in first.tags
        assert second.tags[tags.ERROR] is True
        assert rollback.operation_name == 'connection.rollback()'
        assert parent.operation_name == 'Parent'

    def test_successful_executemany(self, connection_tracing):
        tracer, conn = connection_tracing
        with tracer.start_active_span('Parent'):
            with conn as cursor:
                cursor.executemany('insert into table_one values (%s, %s, %s, %s)',
                                   [(self.random_string(), self.random_string(),
                                     datetime.now(), datetime.now()),
                                    (self.random_string(), self.random_string(),
                                     datetime.now(), datetime.now())])
                cursor.executemany('insert into table_two values (%s, %s, %s, %s)',
                                   [(self.random_int(), self.random_int(),
                                     self.random_float(), self.random_float()),
                                    (self.random_int(), self.random_int(),
                                     self.random_float(), self.random_float())])
        spans = tracer.finished_spans()
        assert len(spans) == 4
        first, second, commit, parent = spans
        for span in (first, second):
            assert span.operation_name == 'cursor.executemany(insert)'
            assert span.tags['custom'] == 'tag'
            assert span.tags[tags.DATABASE_TYPE] == 'sql'
            assert span.tags['db.rows_produced'] == 2
            assert span.parent_id == parent.context.span_id
            assert tags.ERROR not in span.tags
        assert first.tags[tags.DATABASE_STATEMENT] == 'insert into table_one values (%s, %s, %s, %s)'
        assert second.tags[tags.DATABASE_STATEMENT] == 'insert into table_two values (%s, %s, %s, %s)'
        assert commit.operation_name == 'connection.commit()'
        assert parent.operation_name == 'Parent'

    def test_unsuccessful_executemany(self, connection_tracing):
        one, two, three, four = [self.random_string() for _ in range(4)]
        two = self.random_string()
        tracer, conn = connection_tracing
        with tracer.start_active_span('Parent'):
            with pytest.raises(psycopg2.IntegrityError):
                with conn as cursor:
                    cursor.executemany('insert into table_one values (%s, %s, %s, %s)',
                                       [(one, two, datetime.now(), datetime.now()),
                                        (three, four, datetime.now(), datetime.now())])
                    cursor.executemany('insert into table_one values (%s, %s, %s, %s)',
                                       [(one, two, datetime.now(), datetime.now()),
                                        (three, four, datetime.now(), datetime.now())])
        spans = tracer.finished_spans()
        assert len(spans) == 4
        first, second, rollback, parent = spans
        for span in (first, second):
            assert span.operation_name == 'cursor.executemany(insert)'
            assert span.tags['custom'] == 'tag'
            assert span.tags[tags.DATABASE_TYPE] == 'sql'
            assert span.tags[tags.DATABASE_STATEMENT] == 'insert into table_one values (%s, %s, %s, %s)'
            assert span.parent_id == parent.context.span_id
        assert first.tags['db.rows_produced'] == 2
        assert 'db.rows_produced' not in second.tags
        assert tags.ERROR not in first.tags
        assert second.tags[tags.ERROR] is True
        assert rollback.operation_name == 'connection.rollback()'
        assert parent.operation_name == 'Parent'

    def test_successful_callproc(self, connection_tracing):
        tracer, conn = connection_tracing
        with tracer.start_active_span('Parent'):
            with conn as cursor:
                cursor.callproc('test_function_one')
                cursor.callproc('test_function_two')
        spans = tracer.finished_spans()
        assert len(spans) == 4
        first, second, commit, parent = spans
        for span in (first, second):
            assert span.tags['custom'] == 'tag'
            assert span.tags[tags.DATABASE_TYPE] == 'sql'
            assert span.tags['db.rows_produced']  # don't make assumptions about db state
            assert span.parent_id == parent.context.span_id
            assert tags.ERROR not in span.tags
        assert first.tags[tags.DATABASE_STATEMENT] == 'test_function_one'
        assert second.tags[tags.DATABASE_STATEMENT] == 'test_function_two'
        assert first.operation_name == 'cursor.callproc(test_function_one)'
        assert second.operation_name == 'cursor.callproc(test_function_two)'
        assert commit.operation_name == 'connection.commit()'
        assert parent.operation_name == 'Parent'

    def test_unsuccessful_callproc(self, connection_tracing):
        tracer, conn = connection_tracing
        with tracer.start_active_span('Parent'):
            with pytest.raises(psycopg2.ProgrammingError):
                with conn as cursor:
                    cursor.callproc('test_function_one')
                    cursor.callproc('not_a_function')
        spans = tracer.finished_spans()
        assert len(spans) == 4
        first, second, rollback, parent = spans
        for span in (first, second):
            assert span.tags['custom'] == 'tag'
            assert span.tags[tags.DATABASE_TYPE] == 'sql'
            assert span.parent_id == parent.context.span_id
        assert first.operation_name == 'cursor.callproc(test_function_one)'
        assert second.operation_name == 'cursor.callproc(not_a_function)'
        assert first.tags[tags.DATABASE_STATEMENT] == 'test_function_one'
        assert second.tags[tags.DATABASE_STATEMENT] == 'not_a_function'
        assert first.tags['db.rows_produced']
        assert 'db.rows_produced' not in second.tags
        assert tags.ERROR not in first.tags
        assert second.tags[tags.ERROR] is True
        assert rollback.operation_name == 'connection.rollback()'
        assert parent.operation_name == 'Parent'
