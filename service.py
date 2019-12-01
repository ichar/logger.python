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

from app.settings import *
from app.utils import normpath, getToday, getTime, getDate, getDateOnly, checkDate, spent_time

from app.sources import BaseEmitter, AbstractSource, LogProducer, LogConsumer
from app.sources.bankperso import Source as Bankperso
from app.sources.sdc import Source as SDC
from app.sources.exchange import Source as Exchange

is_v3 = sys.version_info[0] > 2 and True or False

_DEFAULT_OBSERVER_TIMEOUT = 1
_DEFAULT_CONSUMER_SLEEP = 1

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
        # Restart flag
        self.restart_requested = False

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

    def _out(self, line, force=None, is_error=False):
        if self._config.get('disableoutput'):
            return
        if is_error:
            logging.error(line)
        elif self._config.get('trace') or force:
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

    @property
    def observer_timeout(self):
        return float(self._config.get('timeout') or _DEFAULT_OBSERVER_TIMEOUT)

    def watch_everything(self):
        return self._config.get('watch_everything')

    @property
    def consumer_sleep(self):
        return float(self._config.get('sleep') or _DEFAULT_CONSUMER_SLEEP)

    def _set_errorlog(self):
        setErrorlog((self._config.get('errorlog') % self._config).lower())

    def _update_config(self):
        self._config = make_config(self.config_source)

    def refreshService(self):
        self._update_config()
        self._set_errorlog()
        # =======
        # Restart
        # =======
        if not self._config.get('refresh'):
            return
        #
        # Stop the service and restart in delay
        #
        self.stop_requested = True
        restart()

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

        self.stop_requested = False

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

            del app
        
        except:
            print_exception()
            self._out('main thread exception (look at traceback.log)', is_error=True)

        self._stop_logger()

    def start_observer(self, app):
        restart_timeout = self._config.get('restart')
        
        lock = threading.Lock()

        source = app._observer_source()
        app._beforeObserve()

        producer = consumer = observer = None

        def _default_except_handler(timeout=None):
            if consumer is not None and consumer.is_alive():
                consumer.stop()
            observer = None

            if timeout:
                time.sleep(timeout)

        observer_found = None

        while True:
            self.restart_requested = False

            producer = LogProducer(app, lock, source=source, logger=self._logger, watch_everything=self.watch_everything)
            consumer = LogConsumer(args=(app, producer, lock, self._logger, self.consumer_sleep))
            consumer.start()

            try:
                observer = Observer(timeout=self.observer_timeout)
                observer.schedule(producer, source, recursive=True)
                observer.start()

                observer_found = {}

                while not self.stop_requested:
                    if restart_timeout and producer.timestamp is not None:
                        timestamp = getToday() - producer.timestamp
                        if timestamp.total_seconds() > restart_timeout:
                            self.restart_requested = True
                            break

                    time.sleep(5)

                    if not observer.is_alive():
                        self._out('observer is lifeless', is_error=True)
                        break
                    if not consumer.is_alive():
                        self._out('cunsumer is lifeless', is_error=True)
                        break

            except FileNotFoundError as ex:
                self.restart_requested = True
                self._out('!!! Observer Exception: %s' % ex, is_error=True)

                _default_except_handler(15)

            except:
                _default_except_handler()
                raise

            else:
                self._out('==> Stop, stop_requested: %s' % self.stop_requested)

            finally:
                if consumer is not None and consumer.is_alive():
                    observer_found = consumer.stop()
                    consumer.join()
                if observer is not None and observer.is_alive():
                    producer.stop()
                    observer.stop()

                if observer_found:
                    self._found.update(observer_found)

                if observer is not None:
                    observer.join()

            consumer = None
            producer = None
            observer = None

            if not self.restart_requested or self.stop_requested:
                break

            self._out('==> Restart...')

        self._out('==> Finish')

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


@delay(30.0)
def restart():
    service = LoggerWindowsService._svc_name_
    logging.info('==> Restarting service[%s]' % service)
    win32serviceutil.RestartService(service)

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

            if not key:
                continue
            elif key == 'suppressed':
                value = list(filter(None, value.split(':')))
            elif key == 'delta_datefrom':
                value = list(filter(None, value.split(':')))
                value = [int(x) for x in value]
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
        LoggerWindowsService._svc_description_ = '%s, basedir: %s. Version %s' % (description, basedir, product_version)
    
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
        _pout('--> Version %s' % product_version)

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
