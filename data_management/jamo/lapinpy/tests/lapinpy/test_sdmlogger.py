import unittest
from lapinpy import sdmlogger
from lapinpy.sdmlogger import SDMLogger
from parameterized import parameterized
import logging
try:
    ## PYTHON3_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    from unittest.mock import patch, Mock, MagicMock, call
    ### PYTHON3_END ###  # noqa: E266 - to be removed after migration cleanup
except ImportError:
    ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    from mock import patch, Mock, MagicMock, call
    ### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup


class TestSdmLogger(unittest.TestCase):

    def setUp(self):
        sdmlogger._handlers = {True: [], False: []}
        sdmlogger._loggers = {}
        sdmlogger._level = {True: logging.NOTSET, False: logging.NOTSET}
        sdmlogger._emailTo = {True: None, False: None}
        sdmlogger._curlObj = {True: None, False: None}
        sdmlogger._logFieldName = {True: 'source', False: 'entities'}

    @parameterized.expand([
        ('not_query_log', False),
        ('query_log', True)
    ])
    @patch.object(sdmlogger, 'RotatingFileHandler')
    @patch.object(sdmlogger, 'TimedRotatingFileHandler')
    def test_config(self, _description, is_query_log, timed_rotating_file_handler, rotating_file_handler):
        curl = Mock()

        sdmlogger.config('/tmp/foo', curl=curl, verbose=True, emailTo='foo@bar.com', isQueryLog=is_query_log)

        if is_query_log:
            timed_rotating_file_handler.assert_called_with('/tmp/foo', backupCount=52, encoding='utf-8', interval=1,
                                                           when='W6')
            self.assertEqual(sdmlogger._emailTo, {False: None, True: 'foo@bar.com'})
        else:
            rotating_file_handler.assert_called_with('/tmp/foo', backupCount=10, maxBytes=5242880)
            self.assertEqual(sdmlogger._emailTo, {False: 'foo@bar.com', True: None})
        self.assertEqual(len(sdmlogger._handlers), 2)

    @patch.object(sdmlogger, 'TimedRotatingFileHandler')
    def test_configQueryLog(self, timed_rotating_file_handler):
        curl = Mock()

        sdmlogger.configQueryLog('/tmp/foo', curl=curl, verbose=True, emailTo='foo@bar.com')

        timed_rotating_file_handler.assert_called_with('/tmp/foo', backupCount=52, encoding='utf-8', interval=1,
                                                       when='W6')
        self.assertEqual(sdmlogger._emailTo, {False: None, True: 'foo@bar.com'})
        self.assertEqual(len(sdmlogger._handlers), 2)

    @parameterized.expand([
        ('not_query_log', False, {False: 'foo@bar.com', True: None}),
        ('query_log', True, {False: None, True: 'foo@bar.com'}),
    ])
    def test_setEmail(self, _description, is_query_log, expected):
        sdmlogger.setEmail('foo@bar.com', isQueryLog=is_query_log)

        self.assertEqual(sdmlogger._emailTo, expected)

    def test_getLogger_exists(self):
        logger = Mock()
        sdmlogger._loggers = {
            'foo': logger,
        }

        self.assertEqual(sdmlogger.getLogger('foo'), logger)

    def test_getLogger_does_not_exist(self):
        self.assertEqual(sdmlogger.getLogger('foo'), sdmlogger._loggers.get('foo'))

    def test_getQueryLogger(self):
        logger = sdmlogger.getQueryLogger('foo', logging.INFO)

        self.assertEqual(logger.logger.level, logging.INFO)

    @patch.object(sdmlogger, 'smtplib')
    @patch.object(sdmlogger, 'MIMEMultipart')
    @patch.object(sdmlogger, 'MIMEBase')
    @patch.object(sdmlogger, 'os')
    @patch.object(sdmlogger, 'open')
    @patch.object(sdmlogger, 'encode_base64')
    def test_sendEmail(self, encode_base64, open, mock_os, mimebase, mimemultipart, smtp):
        def verify_mime_assignment_call(key, value):
            self.assertIn(call.__setitem__(key, value), mime.mock_calls)

        part = Mock()
        mimebase.return_value = part
        attachment = Mock()
        mime = MagicMock()
        mime.as_string.return_value = 'Mime as a string'
        mimemultipart.return_value = mime
        server = Mock()
        smtp.SMTP.return_value = server
        mock_os.path.getsize.return_value = 100
        open.read.return_value = 'attachment data'

        sdmlogger.sendEmail(to='foo@bar.com', subject='some subject', body='hello world',
                            fromAddress='bar@foo.com', host='some_host', attachments=[attachment],
                            replyTo='bar2@foo.com', cc='foo2@bar.com', bcc='foo3@bar.com', mime='plain')

        verify_mime_assignment_call('From', 'bar@foo.com')
        verify_mime_assignment_call('To', 'foo@bar.com')
        verify_mime_assignment_call('Cc', 'foo2@bar.com')
        verify_mime_assignment_call('Subject', 'some subject')
        mime.add_header.assert_called_with('reply-to', 'bar2@foo.com')
        mime.attach.assert_called_with(part)
        server.sendmail.assert_called_with('bar@foo.com', ['foo@bar.com', 'foo2@bar.com', 'foo3@bar.com'],
                                           'Mime as a string')

    def test_SDMLogger_setEntities(self):
        logger = SDMLogger('/tmp/foo')

        logger.setEntities(foo='bar')

        self.assertDictEqual({'foo': 'bar'}, logger.entities)

    def test_SDMLogger_removeEntities(self):
        logger = SDMLogger('/tmp/foo')
        logger.entities = {'foo': 'foo1', 'bar': 'bar1'}

        logger.removeEntities('foo')

        self.assertDictEqual({'bar': 'bar1'}, logger.entities)

    def test_SDMLogger_clearEntities(self):
        logger = SDMLogger('/tmp/foo')
        logger.entities = {'foo': 'foo1', 'bar': 'bar1'}
        logger.entityStr = 'foo'

        logger.clearEntities()

        self.assertEqual(len(logger.entities), 0)
        self.assertEqual(logger.entityStr, '')

    def test_SDMLogger_updateEntityStr(self):
        logger = SDMLogger('/tmp/foo')
        logger.entities = {'foo': 'foo1', 'bar': 'bar1'}

        logger.updateEntityStr()

        self.assertEqual(logger.entityStr, 'foo:foo1, bar:bar1')

    @parameterized.expand([
        ('debug', call('foo', bar='bar', extra={'entities': ''})),
        ('info', call('foo', bar='bar', extra={'entities': ''})),
        ('warning', call('foo', bar='bar', extra={'entities': ''})),
        ('error', call('foo', bar='bar', extra={'entities': ''})),
        ('exception', call('foo', bar='bar')),
    ])
    def test_SDMLogger_logger_levels(self, level, expected_logger_call):
        logger_mock = Mock()
        logger = SDMLogger('/tmp/foo')
        logger.logger = logger_mock

        getattr(logger, level)('foo', bar='bar')

        self.assertIn(expected_logger_call, getattr(logger_mock, level).mock_calls)

    @patch.object(sdmlogger, 'traceback')
    def test_SDMLogger_critical(self, error):
        logger_mock = Mock()
        logger = SDMLogger('/tmp/foo')
        logger.logger = logger_mock
        error.format_exc.return_value = 'An error has occurred.\nCheck the logs.\n'

        logger.critical('foo', bar='bar')

        self.assertIn(call.critical('foo', bar='bar', extra={'entities': ''}), logger_mock.mock_calls)
        self.assertIn(call.critical('An error has occurred.\nCheck the logs.\n', extra={'entities': ''}),
                      logger_mock.mock_calls)


if __name__ == '__main__':
    unittest.main()
