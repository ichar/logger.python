# -*- coding: cp1251 -*-

#from __future__ import _pout_function

import datetime
import sys
import os
import time
import threading

from functools import wraps

import win32serviceutil
import win32service
import win32event
import servicemanager
import socket
import logging

from watchdog.observers import Observer

sys.path.append('C:/apps/LoggerService')

from config import (
     basedir, IsDebug, IsDeepDebug, IsTrace, IsDisableOutput,
     default_unicode, default_encoding, cr,
     LOCAL_FULL_TIMESTAMP, UTC_FULL_TIMESTAMP, DATE_STAMP, 
     isIterable, print_to, print_exception, setErrorlog, getErrorlog
     )

#from app.worker import Logger
from app.utils import normpath, getToday, getTime, getDate, getDateOnly, checkDate, spent_time

from app.sources import BaseEmitter, AbstractSource, LogProducer, LogConsumer
from app.sources.bankperso import Source as Bankperso
from app.sources.sdc import Source as SDC
from app.sources.exchange import Source as Exchange

is_v3 = sys.version_info[0] > 2 and True or False

version = '1.0 with cp1251 (Python3)'

_config = {}

##  =========================================================  ##

def delay(delay=0.):
    """
        Decorator delaying the execution of a function for a while
    """
    def wrap(f):
        @wraps(f)
        def delayed(*args, **kwargs):
            timer = threading.Timer(delay, f, args=args, kwargs=kwargs)
            timer.start()
        return delayed
    return wrap

def _pout(s, **kw):
    if not is_v3:
        print(s, end='end' in kw and kw.get('end') or None)
        if 'flush' in kw and kw['flush'] == True:
            sys.stdout.flush()
    else:
        print(s, **kw)

##  =========================================================  ##

class Logger(object):
    """
        Helper logging class
    """
    def __init__(self, service):
        self._service = service
    
    def out(self, line):
        self._service._out(line)


class LoggerWindowsService(win32serviceutil.ServiceFramework):
    """
        BankPerso Logger Windows Service Class
    """
    _svc_name_ = "logger.config"
    _svc_display_name_ = "Default Log Service"
    _svc_description_ = "Default Service Description"
    
    def __init__(self, args):
        self.__class__._svc_name_ = args[0]
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.stop_event = win32event.CreateEvent(None, 0, 0, None)
        #socket.setdefaulttimeout(60)

        # Just `_svc_name_` class attribute !!!
        self.config_source = normpath(os.path.join(basedir, args[0]))
        # Stop flag
        self.stop_requested = False

        self._started = None
        self._finished = None

        # Application logger instance
        self._logger = None

        # Application config, can be updated(freshed) every time on start
        self._update_config()

        # Service Events log (just console stdout)
        logging.basicConfig(
            filename=os.path.join(basedir, 'service.%(ctype)s.%(alias)s.log' % self._config),
            level=logging.DEBUG, 
            format='%(asctime)s %(levelname)-7.7s %(message)s',
            datefmt=UTC_FULL_TIMESTAMP
        )

        self._out('config: %s' % self.config_source)

    def _start_logger(self):
        self._logger = Logger(self)

    def _stop_logger(self):
        del self._logger

    def _out(self, line):
        if self._config.get('disableoutput'):
            return
        logging.info(line)

    def SvcStop(self):
        #self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        # ====
        # Stop
        # ====
        self.stop_requested = True

        self._finished = getToday()
        self._out('==> Stopping service at %s' % getDate(self._finished, UTC_FULL_TIMESTAMP))
        self._out('==> Spent time: %s sec' % spent_time(self._started, self._finished))

        win32event.SetEvent(self.stop_event)
        self.ReportServiceStatus(win32service.SERVICE_STOPPED)

    def refreshService(self):
        self._update_config()
        self._set_errorlog()

    def _set_errorlog(self):
        setErrorlog((self._config.get('errorlog') % self._config).lower())

    def _update_config(self):
        self._config = make_config(self.config_source)

    def SvcDoRun(self):
        servicemanager.LogMsg(
            servicemanager.EVENTLOG_INFORMATION_TYPE,
            servicemanager.PYS_SERVICE_STARTED,
            (self._svc_name_,'')
        )
        # =====
        # Start
        # =====
        self._started = getToday()
        self._out('==> Starting service at %s' % getDate(self._started, UTC_FULL_TIMESTAMP))

        self.stop_requested = False
        #
        # Read app config
        #
        self._update_config()
        self._set_errorlog()
        self._out('errorlog: %s' % getErrorlog())
        #
        # Try to start the service
        #
        self.main()

    def create_app(self):
        config = self._config
        logger = self._logger

        ctype = config['ctype'].lower()

        if not ctype:
            app = AbstractSource(config, logger)
        elif ctype == 'bankperso':
            app = Bankperso(config, logger)
        elif ctype == 'sdc':
            app = SDC(config, logger)
        elif ctype == 'exchange':
            app = Exchange(config, logger)
        else:
            app = Bankperso(config, logger)
        
        return app

    def main(self, **kw):
        self._processed = 0
        self._found = {}

        self.ReportServiceStatus(win32service.SERVICE_RUNNING)
        self._start_logger()

        if IsTrace and not IsDisableOutput:
            self._out('root: %s' % self._config.get('root'))

        date_from = None

        try:
            app = self.create_app()
            app._init_state(date_from=date_from, callback=self)

            self.run(app)
            self.start_observer(app)

            app._term()
        except:
            print_exception()

        self._stop_logger()

    def start_observer(self, app):
        lock = threading.Lock()

        source = app._observer_source()
        app._beforeObserve()

        producer = LogProducer(app, lock, source=source, logger=self._logger)
        consumer = LogConsumer(args=(app, producer, lock, self._logger,))
        consumer.start()

        try:
            observer = Observer(timeout=0.5)
            observer.schedule(producer, source, recursive=True)
            observer.start()

            observer_found = {}

            while not self.stop_requested:
                time.sleep(5)

            observer.stop()
            observer_found = consumer.stop()

            if observer_found:
                self._found.update(observer_found)

        except:
            observer = None
            consumer.stop()
            raise

        finally:
            if observer is not None:
                observer.join()
            consumer.join()

    def run(self, app):
        emitter = self._config.get('emitter') or False
        limit = self._config.get('limit') or 0

        print_to(None, '>>> Logger Started[%s], root: %s' % ( \
            self._config['ctype'],
            self._config['root'],
        ))

        base = BaseEmitter(args=(app, emitter, limit, self._logger,))
        
        try:
            base.start()

            while not (self.stop_requested or base.is_finished()):
                time.sleep(5)

            if self.stop_requested:
                base.should_be_stop()

            self._processed, self._found = base.stop()

        finally:
            base.join()

        if not IsDisableOutput:
            self._out('>>> New messages found: %d' % (sum([self._found[x] for x in self._found]) or 0))
            self._out('>>> Total processed: %d orders' % self._processed)
            self._out('>>> Unresolved: %d lines' % app._unresolved_lines())


def make_config(source, encoding=default_encoding):
    with open(source, 'r', encoding=encoding) as fin:
        for line in fin:
            s = line
            if line.startswith(';') or line.startswith('#'):
                continue
            x = line.split('::')
            if len(x) < 2:
                continue

            key = x[0].strip()
            value = x[1].strip()

            if key == 'suppressed':
                value = list(filter(None, value.split(':')))
            elif '|' in value:
                value = value.split('|')
            elif value.lower() in 'false:true':
                value = True if value.lower() == 'true' else False
            elif value.isdigit():
                value = int(value)
            elif key in ':console:seen:':
                value = normpath(os.path.join(basedir, value))

            _config[key] = value

    _config['now'] = getDate(getToday(), format=DATE_STAMP)

    return _config

def isStandAlone(argv):
    return len(argv) > 0 and argv[0].endswith('.exe')

def base(argv):
    return isStandAlone(argv) and os.path.abspath(os.path.dirname(servicemanager.__file__)) or basedir

def setup(name, source):
    title, description = _config.get('service_name')

    if name:
        LoggerWindowsService._svc_name_ = name
    if title:
        LoggerWindowsService._svc_display_name_ = title
    if description:
        LoggerWindowsService._svc_description_ = '%s, basedir: %s. Version %s' % (description, basedir, version)
    
    win32serviceutil.HandleCommandLine(LoggerWindowsService, customInstallOptions='s:')


if __name__ == '__main__':
    argv = sys.argv

    if len(argv) == 1 and isStandAlone(argv):
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(LoggerWindowsService)
        servicemanager.Initialize(LoggerWindowsService._svc_name_, os.path.abspath(servicemanager.__file__))
        servicemanager.StartServiceCtrlDispatcher()

    elif len(argv) == 1 or argv[1].lower() in ('/h', '/help', '-h', 'help', '--help', '/?'):
        _pout('--> Rosan Finance Inc.')
        _pout('--> DB Log System observer Windows service.')
        _pout('--> ')
        _pout('--> Format: service.py [-s <config>] [options] install|update|remove|start [...]|stop|restart [...]|debug [...]')
        _pout('--> ')
        _pout('--> Parameters:')
        _pout('--> ')
        _pout('-->   <config>              :')
        _pout('-->         Name of the script config-file from current directory, by default: `logger.config`,')
        _pout('-->         where should be defined `service_name` parameter')
        _pout('-->         such as <Service Name|Description>:')
        _pout('--> ')
        _pout('-->         BankPerso Default Logger|BankPerso Default Log Service Description')
        _pout('--> ')
        _pout('--> Service options:')
        _pout('--> ')
        _pout('-->   --username <name>     : ')
        _pout('-->         Domain and Username, name: `domain\\username` the service is to run under')
        _pout('--> ')
        _pout('-->   --password <password> :')
        _pout('-->         The password for the username')
        _pout('--> ')
        _pout('-->   --startup <mode>      :')
        _pout('-->         How the service starts, mode: [manual|auto|disabled|delayed], default = manual')
        _pout('--> ')
        _pout('-->   --interactive         :')
        _pout('-->         Allow the service to interact with the desktop')
        _pout('--> ')
        _pout('-->   --perfmonini <file>   :')
        _pout('-->         Path to .ini file to use for registering performance monitor data')
        _pout('--> ')
        _pout('-->   --perfmondll <file>   :')
        _pout('-->         Path to .dll file to use when querying the service for')
        _pout('-->         performance data, default = perfmondata.dll')
        _pout('--> ')
        _pout('--> Options for `start` and `stop` commands only:')
        _pout('--> ')
        _pout('-->   --wait <seconds>      :')
        _pout('-->         Wait for the service to actually start or stop.')
        _pout('-->         If you specify `--wait` with the `stop` option, the service')
        _pout('-->         and all dependent services will be stopped, each waiting')
        _pout('-->         the specified period.')
        _pout('--> ')
        _pout('--> Version:%s' % version)

    elif len(argv) > 1 and argv[1]:

        # -----------
        # Config path
        # -----------
        
        is_default_config = len(argv) < 4 and argv[1] != '-s'
        config_file = 'logger.config' if is_default_config else argv[2]
        
        assert config_file, "Config file name is not present!"

        # --------------
        # Make `_config`
        # --------------

        make_config(config_file)

        # -------------
        # Source folder
        # -------------
        
        source = normpath(_config.get('root') or './')

        # ===============
        # Install Service
        # ===============

        setup(config_file, source)
