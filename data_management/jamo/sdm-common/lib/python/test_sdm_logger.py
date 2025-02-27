import logging
import unittest
import sdm_logger
from sdm_logger import SDMLogger
from io import StringIO
from parameterized import parameterized
try:
    ## PYTHON3_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    from unittest.mock import patch, Mock, MagicMock, call
    ### PYTHON3_END ###  # noqa: E266 - to be removed after migration cleanup
except ImportError:
    ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    from mock import patch, Mock, MagicMock, call
    ### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup


class TestSdmLogger(unittest.TestCase):

    def tearDown(self):
        sdm_logger._handlers = []

    @patch('sdm_logger.RotatingFileHandler')
    def test_config(self, file_handler):
        curl = Mock()

        sdm_logger.config('/tmp/foo', curl=curl, verbose=True, emailTo='foo@bar.com', backupCount=3)

        file_handler.assert_called_with('/tmp/foo', backupCount=3, maxBytes=4194304)
        self.assertEqual(sdm_logger._emailTo, 'foo@bar.com')
        self.assertEqual(len(sdm_logger._handlers), 2)

    def test_setEmail(self):
        sdm_logger.setEmail('foo@bar.com')

        self.assertEqual(sdm_logger._emailTo, 'foo@bar.com')

    def test_getLogger_exists(self):
        logger = Mock()
        sdm_logger._loggers = {
            'foo': logger,
        }

        self.assertEqual(sdm_logger.getLogger('foo'), logger)

    def test_getLogger_does_not_exist(self):
        self.assertEqual(sdm_logger.getLogger('foo'), sdm_logger._loggers.get('foo'))

    @patch('sdm_logger.smtplib.SMTP')
    @patch('sdm_logger.MIMEMultipart')
    @patch('sdm_logger.MIMEBase')
    @patch('sdm_logger.os.path')
    @patch('sdm_logger.open')
    @patch('sdm_logger.encode_base64')
    def test_sendEmail(self, encode_base64, open, path, mimebase, mimemultipart, smtp):
        def verify_mime_assignment_call(key, value):
            self.assertIn(call.__setitem__(key, value), mime.mock_calls)

        part = Mock()
        mimebase.return_value = part
        attachment = Mock()
        mime = MagicMock()
        mime.as_string.return_value = 'Mime as a string'
        mimemultipart.return_value = mime
        server = Mock()
        smtp.return_value = server
        path.getsize.return_value = 100
        open.read.return_value = 'attachment data'

        sdm_logger.sendEmail(to='foo@bar.com', subject='some subject', body='hello world',
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

    def test_sdm_logger_setEntities(self):
        logger = SDMLogger('/tmp/foo')

        logger.setEntities(foo='bar')

        self.assertDictEqual({'foo': 'bar'}, logger.entities)

    def test_sdm_logger_removeEntities(self):
        logger = SDMLogger('/tmp/foo')
        logger.entities = {'foo': 'foo1', 'bar': 'bar1'}

        logger.removeEntities('foo')

        self.assertDictEqual({'bar': 'bar1'}, logger.entities)

    def test_sdm_logger_clearEntities(self):
        logger = SDMLogger('/tmp/foo')
        logger.entities = {'foo': 'foo1', 'bar': 'bar1'}
        logger.entityStr = 'foo'

        logger.clearEntities()

        self.assertEqual(len(logger.entities), 0)
        self.assertEqual(logger.entityStr, '')

    def test_sdm_logger_updateEntityStr(self):
        logger = SDMLogger('/tmp/foo')
        logger.entities = {'foo': 'foo1', 'bar': 'bar1'}

        logger.updateEntityStr()

        self.assertEqual(logger.entityStr, 'foo:foo1, bar:bar1')

    @parameterized.expand([
        ('debug', logging.DEBUG),
        ('info', logging.INFO),
        ('warning', logging.WARNING),
        ('error', logging.ERROR),
    ])
    def test_sdm_logger_levels(self, description, level):
        sdm_logger._level = level

        with patch('sdm_logger.sys.stderr', new=StringIO()) as stderr:
            logger = SDMLogger('/tmp/foo')

            getattr(logger, description)(u'foobar')

            self.assertEqual(stderr.getvalue(), u'/tmp/foo - {} -  - foobar\n'.format(description.upper()))


if __name__ == '__main__':
    unittest.main()
