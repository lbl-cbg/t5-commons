from builtins import str
from builtins import object
import logging
import traceback
import smtplib
import sys
import os
from logging.handlers import RotatingFileHandler
from logging import Formatter
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import COMMASPACE
from email.encoders import encode_base64
### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
from past.builtins import basestring
from future import standard_library
standard_library.install_aliases()
### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup


_handlers = []
_loggers = {}
_level = logging.NOTSET
_emailTo = None
_curlObj = None


def config(file, level=logging.DEBUG, curl=None, verbose=False, emailTo=None, backupCount=5):
    global _handlers, _curlObj, _level, _emailTo
    _emailTo = emailTo
    _handlers = []
    _curlObj = curl
    _level = level
    fileHandler = RotatingFileHandler(file, maxBytes=1024 * 1024 * 4, backupCount=backupCount)
    fileHandler.setFormatter(
        Formatter('%(asctime)s - %(name)s - %(levelname)s - %(entities)s - %(message)s', '%m/%d/%Y %I:%M:%S %p'))
    fileHandler.setLevel(level)
    _handlers.append(fileHandler)
    if verbose:
        stdOutHandler = logging.StreamHandler(sys.stderr)
        stdOutHandler.setLevel(level)
        stdOutHandler.setFormatter(
            Formatter('%(name)s - %(levelname)s - %(entities)s - %(message)s', '%m/%d/%Y %I:%M:%S %p'))
        _handlers.append(stdOutHandler)


def setEmail(emails):
    global _emailTo
    _emailTo = emails


def getLogger(name):
    global _loggers
    if name in _loggers:
        return _loggers[name]
    _loggers[name] = SDMLogger(name)
    return _loggers[name]


def sendEmail(to, subject, body, fromAddress='sdm@localhost', host='localhost', attachments=[], replyTo=None, cc=[],
              bcc=[], mime='plain'):
    ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    if isinstance(to, basestring):
    ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
    ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
    # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
    # if isinstance(to, str):  # noqa: E115 - remove this noqa comment after migration cleanup
    ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
        to = [to]

    ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    if isinstance(cc, basestring):
    ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
    ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
    # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
    # if isinstance(cc, str):  # noqa: E115 - remove this noqa comment after migration cleanup
    ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
        cc = [cc]

    ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    if isinstance(bcc, basestring):
    ### PYTHON2_END ###  # noqa: E266,E115 - to be removed after migration cleanup
    ### PYTHON3_BEGIN ###  # noqa: E266,E115 - to be removed after migration cleanup
    # TODO: uncomment code below during cleanup  # noqa: E115 - to be removed after migration cleanup
    # if isinstance(bcc, str):  # noqa: E115 - remove this noqa comment after migration cleanup
    ### PYTHON3_END ###  # noqa: E266,E115 - to be removed after migration cleanup
        bcc = [bcc]

    try:
        server = smtplib.SMTP(host)
        msg = MIMEMultipart()
        msg['From'] = fromAddress
        msg['To'] = COMMASPACE.join(to)
        if len(cc) > 0:
            msg['Cc'] = COMMASPACE.join(cc)
        msg['Subject'] = subject
        if replyTo is not None:
            msg.add_header('reply-to', replyTo)

        msg.attach(MIMEText(body, mime))
        for f in attachments:
            if os.path.getsize(f) > 4194304:
                continue
            part = MIMEBase('application', "octet-stream")
            part.set_payload(open(f, "rb").read())
            encode_base64(part)
            part.add_header('Content-Disposition', 'attachment; filename="%s"' % os.path.basename(f))
            msg.attach(part)
        server.sendmail(fromAddress, to + cc + bcc, msg.as_string())

        server.quit()
    except Exception as e:
        getLogger('logger').error('Failed to send email, error: %s' % e)


class SDMLogger(object):

    def __init__(self, script):
        global _handlers, _level, _emailTo
        logger = logging.getLogger(script)
        logger.setLevel(_level)
        if _handlers is None or len(_handlers) == 0:
            stdOutHandler = logging.StreamHandler(sys.stderr)
            stdOutHandler.setLevel(_level)
            stdOutHandler.setFormatter(
                Formatter('%(name)s - %(levelname)s - %(entities)s - %(message)s', '%m/%d/%Y %I:%M:%S %p'))
            _handlers.append(stdOutHandler)
        logger.handlers = _handlers
        self.logger = logger
        self.entities = {}
        self.script = script
        self.emailTo = _emailTo
        self.entityStr = ''

    def setEntities(self, **entities):
        self.entities.update(entities)
        self.updateEntityStr()

    def removeEntities(self, *entities):
        for entity in entities:
            if entity in self.entities:
                del self.entities[entity]
        self.updateEntityStr()

    def clearEntities(self):
        self.entities.clear()
        self.entityStr = ''

    def updateEntityStr(self):
        ret = []
        for entity in self.entities:
            ret.append('%s:%s' % (entity, self.entities[entity]))
        self.entityStr = ', '.join(ret)

    def __calllogger(self, function, *args, **kwargs):
        extras = {'entities': self.entityStr}
        if 'extra' in kwargs:
            kwargs['extra'].update(extras)
        else:
            kwargs['extra'] = extras
        function(*args, **kwargs)

    def debug(self, *args, **kwargs):
        self.__calllogger(self.logger.debug, *args, **kwargs)

    def info(self, *args, **kwargs):
        self.__calllogger(self.logger.info, *args, **kwargs)

    def warning(self, *args, **kwargs):
        self.__calllogger(self.logger.warning, *args, **kwargs)

    def error(self, *args, **kwargs):
        self.__calllogger(self.logger.error, *args, **kwargs)

    def exception(self, *args, **kwargs):
        self.logger.exception(*args, **kwargs)

    def critical(self, *args, **kwargs):
        self.__calllogger(self.logger.critical, *args, **kwargs)
        exc_type, exc_value, exc_traceback = sys.exc_info()
        formatted_lines = traceback.format_exc()
        self.__calllogger(self.logger.critical, formatted_lines)
        if self.emailTo is not None:
            sendEmail(self.emailTo, 'Critical Error has occured in script %s' % self.script, formatted_lines)

    def finish(self):
        self.info('finished running script')


def catchall(logger=None, follow=True):
    def func(function):
        def inner(*args, **kwargs):
            try:
                logger.info('running method: %s with args: %s' % (function.__name__, str(args) + str(kwargs)))
                ret = function(*args, **kwargs)
                logger.info('ran method: %s sucessfully. Returned %s' % (function.__name__, str(ret)))
                return ret
            except Exception as e:  # noqa: F841
                if logger is not None:
                    logger.critical('Method: %s threw unexpected exception:' % (function.__name__))
                if follow:
                    raise

        return inner

    return func
