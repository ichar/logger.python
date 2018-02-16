# -*- coding: utf-8 -*-

#from __future__ import _pout_function

import datetime
import codecs
import sys
import os
import re
import time
import threading

from watchdog.observers import Observer

from config import (
     CONNECTION, IsDebug, IsDeepDebug, IsTrace, IsDisableOutput, print_to, print_exception,
     default_unicode, default_encoding, default_iso, cr,
     LOCAL_EASY_DATESTAMP, UTC_FULL_TIMESTAMP, DATE_STAMP, DATE_FROM_DELTA,
     setErrorlog
     )

from app.worker import Logger, setup_console
from app.utils import normpath, getToday, getDate, getDateOnly, checkDate, spent_time, daydelta

from app.sources import AbstractSource, LogProducer, LogConsumer
from app.sources.bankperso import Source as Bankperso
from app.sources.sdc import Source as SDC
from app.sources.exchange import Source as Exchange

is_v3 = sys.version_info[0] > 2 and True or False

version = '1.0 with cp1251 (Python3)'

config = {}

EOL = '\n'

_processed = 0
_found = {}

##  =========================================================  ##

def _imports():
    for name, val in globals().items():
        if isinstance(val, types.ModuleType):
            yield val.__name__

def show_imported_modules():
    return [x for x in _imports()]

def _pout(s, **kw):
    if not is_v3:
        print(s, end='end' in kw and kw.get('end') or None)
        if 'flush' in kw and kw['flush'] == True:
            sys.stdout.flush()
    else:
        print(s, **kw)

##  =========================================================  ##

logger = Logger(False, encoding=default_encoding)

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

            config[key] = value

            if IsTrace and not IsDisableOutput:
                logger.out('config: %s -> %s' % (key, value))

    config['now'] = getDate(getToday(), format=DATE_STAMP)

def start_observer(app, **kw):
    global _found

    lock = threading.Lock()

    source = app._observer_source()

    app._beforeObserve()

    producer = LogProducer(app, lock, source=source, logger=logger)
    
    consumer = LogConsumer(args=(app, producer, lock, logger,))
    consumer.start()

    try:
        observer = Observer(timeout=0.5)
        observer.schedule(producer, source, recursive=True)
        observer.start()

        observer_found = {}

        try:
            while True:
                time.sleep(1)

        except KeyboardInterrupt:
            observer.stop()
            observer_found = consumer.stop()

        if observer_found:
            _found.update(observer_found)

    except:
        observer = None
        consumer.stop()
        raise

    finally:
        if observer is not None:
            observer.join()
        consumer.join()

def run(**kw):
    global _processed
    global _found

    ctype = config['ctype'].lower()
    emitter = config.get('emitter') or False
    limit = config.get('limit') or 0

    try:
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

        app._init_state(**kw)

        print_to(None, '>>> Logger Started[%s], date_from: %s, root: %s' % ( \
            config['ctype'],
            kw.get('date_from'),
            config['root'],
        ))

        if app.is_ready():
            if emitter:
                _processed, _found = app.emitter(limit=limit)
            else:
                _processed, _found = app(limit=limit)

        if not IsDisableOutput:
            _pout('>>> New messages found: %d' % (sum([_found[x] for x in _found]) or 0))
            _pout('>>> Total processed: %d orders' % _processed)
            _pout('>>> Unresolved: %d lines' % app._unresolved_lines())

        start_observer(app, **kw)

        app._term()

        print_to(None, '%s>>> Logger Finished[%s]%s' % ( \
            cr,
            config['ctype'],
            cr,
        ))

    except:
        print_exception()


if __name__ == "__main__":
    argv = sys.argv

    setup_console(default_encoding)

    if len(argv) == 1 or argv[1].lower() in ('/h', '/help', '-h', 'help', '--help', '/?'):
        _pout('--> Rosan Finance Inc.')
        _pout('--> DB Log System observer.')
        _pout('--> ')
        _pout('--> Format: logger.py [[<config>] [YYYYMMDD] [<source>]]')
        _pout('--> ')
        _pout('--> Parameters:')
        _pout('--> ')
        _pout('-->   <config>      : path to the script config-file, by default: `logger.config`')
        _pout('-->   YYYYMMDD      : patch name as `date_from`')
        _pout('-->   <source>      : source folder, may present in `config`')
        _pout('--> ')
        _pout('--> Version:%s' % version)

    elif len(argv) > 1 and argv[1]:

        # -----------
        # Config path
        # -----------
        
        is_default_config = argv[1].isdigit()
        config_path = 'logger.config' if is_default_config else argv[1]
        
        assert config_path, "Config file name is not present!"

        if is_default_config:
            date_from = argv[1] or None
            source = len(argv) > 2 and argv[2] or ''
        else:
            date_from = len(argv) > 2 and argv[2] or None
            source = len(argv) > 3 and argv[3] or ''

        # -------------------------------------
        # DateFrom as a destination root folder
        # -------------------------------------

        if not date_from:
            date_from = getDateOnly(daydelta(getToday(), DATE_FROM_DELTA))
        else:
            date_from = checkDate(date_from, DATE_STAMP) and getDate(date_from, format=DATE_STAMP, is_date=True) or None

        assert date_from, "Date YYYYMMDD is invalid!"

        date_from = getDate(date_from, LOCAL_EASY_DATESTAMP)    # It's a string (YYYY-MM-DD)

        # --------------
        # Make `_config`
        # --------------

        make_config(config_path)

        # --------------------
        # Set local `errorlog`
        # --------------------

        setErrorlog((config.get('errorlog') % config).lower())

        # -------------
        # Source folder
        # -------------
        
        source = normpath(source or config.get('root') or './')

        # =====
        # Start
        # =====

        if IsTrace and not IsDisableOutput:
            logger.out('config: %s, date_from: %s, source: %s' % ( \
                config_path,
                str(date_from),
                source,
            ))

        start = getToday()

        run(source=source, date_from=date_from, encoding=default_unicode)

        if IsTrace and not IsDisableOutput:
            finish = getToday()
            t = finish - start
            logger.out('Started at %s' % getDate(start, UTC_FULL_TIMESTAMP))
            logger.out('Finished at %s' % getDate(finish, UTC_FULL_TIMESTAMP))
            logger.out('Spent time: %s sec' % spent_time(start, finish))

        logger.close()
