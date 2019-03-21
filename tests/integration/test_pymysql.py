# Copyright (C) 2018 SignalFx, Inc. All rights reserved.
from datetime import datetime
from time import sleep
import os.path

from opentracing.mocktracer import MockTracer
from pymysql.cursors import DictCursor
from opentracing.ext import tags
import pymysql
import docker
import pytest

from dbapi_opentracing import ConnectionTracing
from .conftest import DBAPITest


@pytest.fixture(scope='session')
def mysql_container():
    client = docker.from_env()
    env = dict(MYSQL_ROOT_PASSWORD='pass',
               MYSQL_ROOT_HOST='%')
    cwd = os.path.dirname(os.path.abspath(__file__))
    conf_d = os.path.join(cwd, 'pymysql/conf.d')
    initdb_d = os.path.join(cwd, 'pymysql/initdb.d')
    volumes = ['{}:/etc/mysql/conf.d'.format(conf_d),
               '{}:/docker-entrypoint-initdb.d'.format(initdb_d)]
    mysql = client.containers.run('mysql:latest', environment=env, ports={'3306/tcp': 3306},
                                  volumes=volumes, detach=True)
    try:
        yield mysql
    finally:
        mysql.remove(v=True, force=True)


class PyMySQLTest(DBAPITest):

    @pytest.fixture
    def connection_tracing(self, mysql_container):
        for _ in range(240):
            try:
                conn = pymysql.connect(host='127.0.0.1', user='test_user', password='test_password',
                                       db='test_db', port=3306, cursorclass=DictCursor)
                break
            except pymysql.OperationalError:
                sleep(.25)
        tracer = MockTracer()
        return tracer, ConnectionTracing(conn, tracer, span_tags=dict(custom='tag'))


class TestPyMYSQLCursorContext(PyMySQLTest):

    def test_successful_execute(self, connection_tracing):
        tracer, conn = connection_tracing
        with tracer.start_active_span('Parent'):
            with conn.cursor() as cursor:
                cursor.execute(u'insert into table_one values (%s, %s, %s, %s)',
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
        assert commit.operation_name == 'Connection.commit()'
        assert parent.operation_name == 'Parent'

    def test_unsuccessful_execute(self, connection_tracing):
        one = self.random_string()
        two = self.random_string()
        tracer, conn = connection_tracing
        with tracer.start_active_span('Parent'):
            with pytest.raises(pymysql.IntegrityError):
                with conn.cursor() as cursor:
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
        assert commit.operation_name == 'Connection.commit()'
        assert parent.operation_name == 'Parent'

    def test_successful_executemany(self, connection_tracing):
        tracer, conn = connection_tracing
        with tracer.start_active_span('Parent'):
            with conn.cursor() as cursor:
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
        assert commit.operation_name == 'Connection.commit()'
        assert parent.operation_name == 'Parent'

    def test_unsuccessful_executemany(self, connection_tracing):
        one, two, three, four = [self.random_string() for _ in range(4)]
        two = self.random_string()
        tracer, conn = connection_tracing
        with tracer.start_active_span('Parent'):
            with pytest.raises(pymysql.IntegrityError):
                with conn.cursor() as cursor:
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
        assert commit.operation_name == 'Connection.commit()'
        assert parent.operation_name == 'Parent'

    def test_successful_callproc(self, connection_tracing):
        tracer, conn = connection_tracing
        with tracer.start_active_span('Parent'):
            with conn.cursor() as cursor:
                cursor.callproc('test_procedure_one')
                cursor.callproc(u'test_procedure_two')
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
        assert first.tags[tags.DATABASE_STATEMENT] == 'test_procedure_one'
        assert second.tags[tags.DATABASE_STATEMENT] == 'test_procedure_two'
        assert first.operation_name == 'DictCursor.callproc(test_procedure_one)'
        assert second.operation_name == 'DictCursor.callproc(test_procedure_two)'
        assert commit.operation_name == 'Connection.commit()'
        assert parent.operation_name == 'Parent'

    def test_unsuccessful_callproc(self, connection_tracing):
        tracer, conn = connection_tracing
        with tracer.start_active_span('Parent'):
            with pytest.raises(pymysql.InternalError):
                with conn.cursor() as cursor:
                    cursor.callproc('test_procedure_one')
                    cursor.callproc('not_a_procedure')
            conn.commit()
        spans = tracer.finished_spans()
        assert len(spans) == 4
        first, second, commit, parent = spans
        for span in (first, second):
            assert span.tags['custom'] == 'tag'
            assert span.tags[tags.DATABASE_TYPE] == 'sql'
            assert span.parent_id == parent.context.span_id
        assert first.operation_name == 'DictCursor.callproc(test_procedure_one)'
        assert second.operation_name == 'DictCursor.callproc(not_a_procedure)'
        assert first.tags[tags.DATABASE_STATEMENT] == 'test_procedure_one'
        assert second.tags[tags.DATABASE_STATEMENT] == 'not_a_procedure'
        assert first.tags['db.rows_produced']
        assert 'db.rows_produced' not in second.tags
        assert tags.ERROR not in first.tags
        assert second.tags[tags.ERROR] is True
        assert commit.operation_name == 'Connection.commit()'
        assert parent.operation_name == 'Parent'


class TestPyMYSQLConnectionContext(PyMySQLTest):

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
            assert span.operation_name == 'DictCursor.execute(insert)'
            assert span.tags['custom'] == 'tag'
            assert span.tags[tags.DATABASE_TYPE] == 'sql'
            assert span.tags['db.rows_produced'] == 1
            assert span.parent_id == parent.context.span_id
            assert tags.ERROR not in span.tags
        assert first.tags[tags.DATABASE_STATEMENT] == 'insert into table_one values (%s, %s, %s, %s)'
        assert second.tags[tags.DATABASE_STATEMENT] == 'insert into table_two values (%s, %s, %s, %s)'
        assert commit.operation_name == 'Connection.commit()'
        assert parent.operation_name == 'Parent'

    def test_unsuccessful_execute(self, connection_tracing):
        one = self.random_string()
        two = self.random_string()
        tracer, conn = connection_tracing
        with tracer.start_active_span('Parent'):
            with pytest.raises(pymysql.IntegrityError):
                with conn as cursor:
                    cursor.execute('insert into table_one values (%s, %s, %s, %s)',
                                   (one, two, datetime.now(), datetime.now()))
                    cursor.execute('insert into table_one values (%s, %s, %s, %s)',
                                   (one, two, datetime.now(), datetime.now()))
        spans = tracer.finished_spans()
        assert len(spans) == 4
        first, second, rollback, parent = spans
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
        assert rollback.operation_name == 'Connection.rollback()'
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
            assert span.operation_name == 'DictCursor.executemany(insert)'
            assert span.tags['custom'] == 'tag'
            assert span.tags[tags.DATABASE_TYPE] == 'sql'
            assert span.tags['db.rows_produced'] == 2
            assert span.parent_id == parent.context.span_id
            assert tags.ERROR not in span.tags
        assert first.tags[tags.DATABASE_STATEMENT] == 'insert into table_one values (%s, %s, %s, %s)'
        assert second.tags[tags.DATABASE_STATEMENT] == 'insert into table_two values (%s, %s, %s, %s)'
        assert commit.operation_name == 'Connection.commit()'
        assert parent.operation_name == 'Parent'

    def test_unsuccessful_executemany(self, connection_tracing):
        one, two, three, four = [self.random_string() for _ in range(4)]
        two = self.random_string()
        tracer, conn = connection_tracing
        with tracer.start_active_span('Parent'):
            with pytest.raises(pymysql.IntegrityError):
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
            assert span.operation_name == 'DictCursor.executemany(insert)'
            assert span.tags['custom'] == 'tag'
            assert span.tags[tags.DATABASE_TYPE] == 'sql'
            assert span.tags[tags.DATABASE_STATEMENT] == 'insert into table_one values (%s, %s, %s, %s)'
            assert span.parent_id == parent.context.span_id
        assert first.tags['db.rows_produced'] == 2
        assert 'db.rows_produced' not in second.tags
        assert tags.ERROR not in first.tags
        assert second.tags[tags.ERROR] is True
        assert rollback.operation_name == 'Connection.rollback()'
        assert parent.operation_name == 'Parent'

    def test_successful_callproc(self, connection_tracing):
        tracer, conn = connection_tracing
        with tracer.start_active_span('Parent'):
            with conn as cursor:
                cursor.callproc('test_procedure_one')
                cursor.callproc('test_procedure_two')
        spans = tracer.finished_spans()
        assert len(spans) == 4
        first, second, commit, parent = spans
        for span in (first, second):
            assert span.tags['custom'] == 'tag'
            assert span.tags[tags.DATABASE_TYPE] == 'sql'
            assert span.tags['db.rows_produced']  # don't make assumptions about db state
            assert span.parent_id == parent.context.span_id
            assert tags.ERROR not in span.tags
        assert first.tags[tags.DATABASE_STATEMENT] == 'test_procedure_one'
        assert second.tags[tags.DATABASE_STATEMENT] == 'test_procedure_two'
        assert first.operation_name == 'DictCursor.callproc(test_procedure_one)'
        assert second.operation_name == 'DictCursor.callproc(test_procedure_two)'
        assert commit.operation_name == 'Connection.commit()'
        assert parent.operation_name == 'Parent'

    def test_unsuccessful_callproc(self, connection_tracing):
        tracer, conn = connection_tracing
        with tracer.start_active_span('Parent'):
            with pytest.raises(pymysql.InternalError):
                with conn as cursor:
                    cursor.callproc('test_procedure_one')
                    cursor.callproc('not_a_procedure')
        spans = tracer.finished_spans()
        assert len(spans) == 4
        first, second, rollback, parent = spans
        for span in (first, second):
            assert span.tags['custom'] == 'tag'
            assert span.tags[tags.DATABASE_TYPE] == 'sql'
            assert span.parent_id == parent.context.span_id
        assert first.operation_name == 'DictCursor.callproc(test_procedure_one)'
        assert second.operation_name == 'DictCursor.callproc(not_a_procedure)'
        assert first.tags[tags.DATABASE_STATEMENT] == 'test_procedure_one'
        assert second.tags[tags.DATABASE_STATEMENT] == 'not_a_procedure'
        assert first.tags['db.rows_produced']
        assert 'db.rows_produced' not in second.tags
        assert tags.ERROR not in first.tags
        assert second.tags[tags.ERROR] is True
        assert rollback.operation_name == 'Connection.rollback()'
        assert parent.operation_name == 'Parent'
