from . import restful
import pymysql
from pymysql.constants import FIELD_TYPE
from pymysql.converters import conversions as conv
from . import sdmlogger
import time
import bson


# Update the pymysql conversions to wrap convert_mysql_timestamp to
# return None for Null dates (which are coming in as 0000-00-00 00:00:00
# instead of Null
def datetime_or_None(obj):
    if obj[:5] == "0000-":
        return None
    else:
        return pymysql.converters.convert_mysql_timestamp(obj)


def int64(obj, mapping=None):
    return int(obj)


conv = conv.copy()
conv[FIELD_TYPE.TIMESTAMP] = datetime_or_None
conv[bson.int64.Int64] = int64


class MySQLRestful(restful.Restful):
    '''Restful class used to connect to a mysql database
    This will handle connection pooling and auto connecting
    and general error handling
    '''

    def __init__(self, host, user, password, database, host_port=None):
        if host_port is None:
            host_port = 3306
        restful.Restful.__init__(self, host, user, password, database, host_port=host_port)

    def connect(self):
        # attempt to retry a connection if it fails
        for attempt in range(5):
            try:
                kwargs = dict(host=self.host, user=self.user, password=self.password, db=self.database, cursorclass=pymysql.cursors.DictCursor, conv=conv)
                if self.host_port is not None:
                    kwargs['port'] = self.host_port
                connection = pymysql.connect(**kwargs)
                connection.autocommit(True)
                return connection
            except Exception as e:
                sdmlogger.getLogger('mysql').warning('mysql connection failed, retrying %d (%s)' % (attempt, e))
                time.sleep(5)
        raise pymysql.Error

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

    def get_howami(self, args, kwargs):
        connection = self.get_connection()
        isalive = connection.open == 1
        self.put_connection(connection)
        return {'mysql_connection_alive': isalive}

    def query(self, query, values=None, extras=None, uselimit=True):
        '''Run a query on the current database

        Args:
            query (string): A mysql query, to replace variables use %s and pass the value to the values kwarg

        Keyword Arguments:
            values (list): A list of arguments that will be injected to the query string. This will do injection checks
            extras (dict): A dictionary of extras that are used for paging

        Example:
            >> self.query('select * from file where file_name=%s', ['test_file'])

        Returns:
            A list of dictionaries where the keys are the columns and the values are the value of the column.
        '''
        if extras is not None and 'tq' in extras:
            if 'order by' not in extras['tq'] and 'sort' in extras:
                query += ' order by %s ' % extras['sort']
            query += " %s" % extras['tq']
        if 'limit' not in query and uselimit:
            query += ' limit 500'
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
        '''Run a delete command, really you can pass in query to this and it will execute
        but only delete commands should be passed.

        Args:
            query (str): A mysql delete query, to replace variables use %s and pass the value to the values list in the correct order
            *values (*): Multiple value strings or numbers that will replace %s in your query string, use this for injection protection

        Example:
            >> self.delete('delete from file where file_name like %s and file_size>%s', 'hello%', 1234)
        '''
        return self.__execute(query, values)

    def __execute(self, statement, values=None, onTry=0):
        connection = self.get_connection()
        cur = connection.cursor()
        try:
            if values is None:
                ret = cur.execute(statement)
            else:
                ret = cur.execute(statement, values)
        except pymysql.OperationalError:
            cur.close()
            del connection
            if onTry >= 3:
                connection = self.connect()
                cur = connection.cursor()
                try:
                    if values is None:
                        ret = cur.execute(statement)
                    else:
                        ret = cur.execute(statement, values)
                except Exception:
                    sdmlogger.getLogger('mysql').error(
                        'mysql statement "%s" failed to run with values %s' % (statement, values))
                    raise
            else:
                return self.__execute(statement, values, onTry + 1)
        except pymysql.IntegrityError:
            self.put_connection(connection)
            return
        if statement.lower().startswith('select') or statement.lower().startswith('show'):
            ret = cur.fetchall()
        elif statement.lower().startswith('insert'):
            ret = int(cur.lastrowid)

        cur.close()
        self.put_connection(connection)
        return ret

    def smart_insert(self, table, values):
        '''Attempt to automatically create a sql statement and insert it
        into the table specified

        Args:
            table (str): The table that the row will be inserted into.
            values (dict): A dictionary where the keys are the columns and the values are the values.

        Returns:
            An int of the auto increment key if exists

        Example:
            >> self.smart_insert('file', {'file_path':'/home/d/dummy','file_name':'my_file'})
        '''
        # connection = self.get_connection()
        # cur = connection.cursor(cursorclass=pymysql.cursors.DictCursor)
        sql = 'insert into %s (' % table
        sql_values = '('
        real_values = []
        for key in values:
            value = values[key]
            if value is not None:
                sql += ' ' + key + ','
                if value == 'now()':
                    sql_values += 'now(),'
                else:
                    sql_values += '%s,'
                    real_values.append(value)
        if len(values) > 0:
            sql = sql[:-1]
            sql_values = sql_values[:-1]
        sql += ') values ' + sql_values + ')'
        real_values = [None if x == 'NULL' else x for x in real_values]
        return self.__execute(sql, real_values)

    def smart_modify(self, table, where, values, tryagain=True):
        '''Run a mysql update statement without forming the statement.

        Args:
            table (str): The table that will be updated.
            where (str): A where clause that will ensure that only the records you want get updated.
            values (dict): A dictionary who's key are columns and the values are the values to get updated.

        Example:
            self.smart_modify('file', 'file_size>10', {'file_size':123})
        '''
        sql = 'update %s set ' % table
        real_values = []
        for key in values:
            value = values[key]
            if value is not None:
                if value == 'now()':
                    sql += ' ' + key + '=now(),'
                else:
                    sql += ' ' + key + '=%s,'
                    real_values.append(value)
        sql = sql[:-1]
        sql += ' where %s' % where
        real_values = [None if x == 'NULL' else x for x in real_values]
        return self.__execute(sql, real_values)

    def modify(self, query, *values):
        '''Run an update command, really you can pass in query to this and it will execute
        but only update commands should be passed.

        Args:
            query (str): A mysql delete query, to replace variables use %s and pass the value to the values list in the correct order
            *values (*): Multiple value strings or numbers that will replace %s in your query string, use this for injection protection

        Example:
            >> self.modify('update file set file_name=%s where file_name like %s and file_size>%s', 'ha', 'hello%', 1234)
        '''
        return self.__execute(query, values)

    def parse_default_query(self, query, select_count, parameters):
        default_query = parameters['query'].replace('?', '%')
        query_parts = default_query.split(' ')
        join_clause_indicator = ['left', 'right', 'join', 'inner', 'outer']

        for index, part in enumerate(query_parts):
            if part == 'like':
                like_index = index + 1
                if '%' not in query_parts[like_index]:
                    query_parts[like_index] = '"%' + query_parts[like_index].replace('"', '') + '%"'
            elif part == 'nin':
                query_parts[index] = 'not in'
            elif part == 'in':
                pass
            elif part in join_clause_indicator:
                if 'where' in query_parts[index:]:
                    where_index = query_parts.index('where')
                    query += ' '.join(query_parts[index: where_index])
                    query_parts = query_parts[:index] + query_parts[where_index:]
                else:
                    query += ' '.join(query_parts[index:])
                    query_parts = query_parts[:index]
                break

                # If it is more complex than a where clause, the whole query (minus SELECT and FROM) is expected.
        if 'where' not in query_parts and len(query_parts) > 0 and (len(query_parts) > 1 or query_parts[0] != ''):
            query += ' where '
            select_count += ' where '

        default_query = ' '.join(query_parts)
        query += default_query
        select_count += default_query

        return query, select_count

    def construct_query(self, collection, parameters, return_count):
        if parameters['fields'] is not None:
            fields = ','.join(parameters['fields'])

            if parameters['id_field'] not in fields:
                fields += ',{}'.format(parameters['id_field'])
        else:
            fields = '*'

        # SELECT and FROM should never be set in a default_query
        query = 'select {} from {} '.format(fields, collection)
        select_count = 'select count(*) as record_count from {} '.format(collection)

        if parameters['query'].strip() != '':
            query, select_count = self.parse_default_query(query, select_count, parameters)

        if 'sort' in parameters and parameters['sort'] != '':
            query += ' order by {}'.format(parameters['sort'])

        # Added for restful api calls
        if 'page' not in parameters:
            parameters['page'] = 1

        query += ' limit {},{}'.format((parameters['page'] - 1) * return_count, return_count)

        return query, select_count

    def queryResults_dataChange(self, parameters, collection):
        return_count = parameters.get('return_count', 100)
        query, select_count = self.construct_query(collection, parameters, return_count)

        # Added for restful api calls
        if not parameters.get('__ui', None) and not parameters.get('queryResults', None):
            return self.__execute(query)

        record_count = self.__execute(select_count)[0]['record_count']
        data = []
        if record_count > 0:
            data = self.__execute(query)

        return {'record_count': record_count,
                'return_count': return_count,
                'data': data}
