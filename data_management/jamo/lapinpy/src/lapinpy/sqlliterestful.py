from threading import Lock

import sqlite3
from sqlite3 import OperationalError, IntegrityError

from .restful import Restful


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


# TODO: Is this still being used?
class SqlLiteRestful(Restful):

    def __init__(self, host, user, password, database):
        Restful.__init__(self, host, user, password, database)
        self.lock = Lock()

    def connect(self):
        connection = sqlite3.connect(self.database)
        connection.row_factory = dict_factory
        return connection

    def getCvss(self, *tables, **fields):
        ret = {}
        tempStructs = {}
        prepend = False
        if 'prepend' in fields:
            prepend = fields['prepend']

        for table in tables:
            temp = {}
            if prepend:
                rows = self.query('select %s_id, %s_name from %s_cv' % (table, table, table))
                for row in rows:
                    temp[row['%s_name' % table]] = row['%s_id' % table]
            else:
                rows = self.query('select %s_id,status from %s_cv' % (table, table))
                for row in rows:
                    temp[row['status']] = row['%s_id' % table]
            ret[table] = temp
            tempStructs[table] = lambda: 1
            tempStructs[table].__dict__.update(ret[table])
        self.__dict__.update(tempStructs)
        return ret

    def query(self, query, values=None, extras=None):
        if extras is not None and 'tq' in extras:
            if 'order by' not in extras['tq'] and 'sort' in extras:
                query += ' order by %s ' % extras['sort']
            query += " %s" % extras['tq']
        ret = self.__execute(query, values)
        if len(ret) > 0:
            first = ret[0]
            removeCols = []
            for col in first:
                if col.count('.') > 0:
                    removeCols.append(col)
            if len(removeCols) > 0:
                for row in ret:
                    for col in removeCols:
                        del row[col]

        return ret

    def delete(self, query, *values):
        return self.__execute(query, values)

    def put_connection(self, connection):
        connection.commit()
        connection.close()

    def runScript(self, script):
        connection = self.get_connection()
        cur = connection.cursor()
        with open(script) as f:
            contents = f.read()
        cur.executescript(contents)
        self.put_connection(connection)

    def __execute(self, statement, values=None, onTry=0):
        statement = statement.replace('%s', '?')
        if values is not None and not isinstance(values, (list, tuple)):
            values = (values,)
        self.lock.acquire()
        connection = self.get_connection()
        cur = connection.cursor()
        try:
            if values is None:
                ret = cur.execute(statement)
            else:
                ret = cur.execute(statement, values)
        except OperationalError as e:
            if e.message != 'database is locked' or onTry >= 3:
                del connection
                self.lock.release()
                raise
            self.lock.release()
            return self.__execute(statement, values, onTry + 1)
        except IntegrityError:
            self.put_connection(connection)
            self.lock.release()
            raise
        except Exception:
            self.lock.release()
            raise

        if statement.lower().startswith('select'):
            ret = cur.fetchall()
        elif statement.lower().startswith('insert'):
            ret = int(cur.lastrowid)

        self.put_connection(connection)
        self.lock.release()
        return ret

    def smart_insert(self, table, values, nonExcaped=[]):
        sql = 'insert into %s (' % table
        sql_values = '('
        real_values = []
        for key in values:
            value = values[key]
            if value is not None:
                sql += ' ' + key + ','
                if value == 'now':
                    sql_values += 'datetime("now"),'
                else:
                    sql_values += '%s,'
                    real_values.append(value)
        if len(values) > 0:
            sql = sql[:-1]
            sql_values = sql_values[:-1]
        sql += ') values ' + sql_values + ')'
        return self.__execute(sql, real_values)

    def smart_modify(self, table, where, values, tryagain=True):
        sql = 'update %s set ' % table
        real_values = []
        for key in values:
            value = values[key]
            if value is not None:
                if value == 'now':
                    sql += ' ' + key + '=datetime("now"),'
                else:
                    sql += ' ' + key + '=%s,'
                    real_values.append(value)
        sql = sql[:-1]
        sql += ' where %s' % where
        return self.__execute(sql, real_values)

    def modify(self, query, *values):
        return self.__execute(query, values)
