import logging
import traceback
import smtplib
import sys
import os
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import COMMASPACE
from email.encoders import encode_base64
from logging.handlers import RotatingFileHandler
from logging.handlers import TimedRotatingFileHandler
from logging import Formatter

### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
from past.builtins import basestring
from future import standard_library
standard_library.install_aliases()
### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup

_handlers = {True: [], False: []}
_loggers = {}
_level = {True: logging.NOTSET, False: logging.NOTSET}
_emailTo = {True: None, False: None}
_curlObj = {True: None, False: None}
_logFieldName = {True: 'source', False: 'entities'}


def config(file, level=logging.DEBUG, curl=None, verbose=False, emailTo=None, isQueryLog=False):
    global _handlers, _curlObj, _level, _emailTo, _logFieldName
    _emailTo[isQueryLog] = emailTo
    _handlers[isQueryLog] = []
    _curlObj[isQueryLog] = curl
    _level[isQueryLog] = level
    if isQueryLog:
        fileHandler = TimedRotatingFileHandler(file, when="W6", interval=1, backupCount=52, encoding="utf-8")
    else:
        fileHandler = RotatingFileHandler(file, maxBytes=1024 * 1024 * 5, backupCount=10)
    fileHandler.setFormatter(
        Formatter('%(asctime)s - %(name)s - %(levelname)s - %({})s - %(message)s'.format(_logFieldName[isQueryLog]), '%m/%d/%Y %I:%M:%S %p'))
    fileHandler.setLevel(level)
    _handlers[isQueryLog].append(fileHandler)
    if verbose:
        stdOutHandler = logging.StreamHandler(sys.stderr)
        stdOutHandler.setLevel(level)
        stdOutHandler.setFormatter(
            Formatter('%(name)s - %(levelname)s - %({})s - %(message)s'.format(_logFieldName[isQueryLog]), '%m/%d/%Y %I:%M:%S %p'))
        _handlers[isQueryLog].append(stdOutHandler)


def configQueryLog(file, level=logging.DEBUG, curl=None, verbose=False, emailTo=None):
    config(file, level, curl, verbose, emailTo, True)


def setEmail(emails, isQueryLog=False):
    global _emailTo
    _emailTo[isQueryLog] = emails


def getLogger(name, level=None, isQueryLog=False):
    global _loggers
    if name in _loggers:
        # reset the log level
        if level is None:
            level = _level[isQueryLog]
        _loggers[name].logger.setLevel(level)
    else:
        _loggers[name] = SDMLogger(name, level, isQueryLog)
    return _loggers[name]


def getQueryLogger(name, level=None):
    return getLogger(name, level, True)


def sendEmail(to, subject, body, fromAddress='sdm@localhost', host='localhost', attachments=[], replyTo=None, cc=[],
              bcc=[], mime='plain'):
    ### PYTHON2_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    if isinstance(to, basestring):
        to = [to]
    if isinstance(cc, basestring):
        cc = [cc]
    if isinstance(bcc, basestring):
        bcc = [bcc]
    ### PYTHON2_END ###  # noqa: E266 - to be removed after migration cleanup
    ### PYTHON3_BEGIN ###  # noqa: E266 - to be removed after migration cleanup
    # TODO: uncomment code below during cleanup
    # if isinstance(to, str):
    #     to = [to]
    # if isinstance(cc, str):
    #     cc = [cc]
    # if isinstance(bcc, str):
    #     bcc = [bcc]
    ### PYTHON3_END ###  # noqa: E266 - to be removed after migration cleanup
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

        msg.attach(MIMEText(body, mime, 'utf-8'))
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
    except Exception:
        getLogger('logger').error('Failed to send email')


class SDMLogger(object):

    def __init__(self, script, level=None, isQueryLog=False):
        global _handlers, _level, _emailTo, _logFieldName
        logger = logging.getLogger(script)
        if level:
            logger.setLevel(level)
        else:
            logger.setLevel(_level[isQueryLog])
        if _handlers is None or isQueryLog not in _handlers or len(_handlers[isQueryLog]) == 0:
            stdOutHandler = logging.StreamHandler(sys.stderr)
            stdOutHandler.setLevel(_level[isQueryLog])
            stdOutHandler.setFormatter(
                Formatter('%(name)s - %(levelname)s - %({})s - %(message)s'.format(_logFieldName[isQueryLog]), '%m/%d/%Y %I:%M:%S %p'))
            _handlers[isQueryLog].append(stdOutHandler)
        logger.handlers = _handlers[isQueryLog]
        self.logger = logger
        self.entities = {}
        self.script = script
        self.emailTo = _emailTo[isQueryLog]
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
            sendEmail(self.emailTo, 'Critical Error has occurred in script %s' % self.script, formatted_lines)

    def finish(self):
        self.info('finished running script')


# TODO: Is this being used?
def catchall(logger=None, follow=True):
    def func(function):
        def inner(*args, **kwargs):
            try:
                logger.info('running method: %s with args: %s' % (function.__name__, str(args) + str(kwargs)))
                ret = function(*args, **kwargs)
                logger.info('ran method: %s successfully. Returned %s' % (function.__name__, str(ret)))
                return ret
            except Exception as e:  # noqa: F841
                if logger is not None:
                    logger.critical('Method: %s threw unexpected exception:' % (function.__name__))
                if follow:
                    raise

        return inner

    return func
