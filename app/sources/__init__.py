# -*- coding: utf-8 -*-

import sys
import os
import time
import threading

from config import (
     CONNECTION, IsDebug, IsDeepDebug, IsExistsTrace, IsTrace, IsDisableOutput, IsObserverTrace,
     default_unicode, default_encoding, default_iso, cr,
     LOCAL_EASY_DATESTAMP, UTC_FULL_TIMESTAMP, DATE_STAMP,
     print_to, print_exception
     )

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, RegexMatchingEventHandler

from functools import wraps

from ..settings import *
from ..database import database_config, BankPersoEngine
from ..worker import checkfile, lines_emitter
from ..utils import normpath, getToday, getTime, getDate, getDateOnly, checkDate, isIterable

engines = {}

args = {
    'bank'      : 'ClientID', 
    'type'      : 'FileTypeID', 
    'status'    : 'FileStatusID', 
    'batchtype' : 'BatchTypeID', 
    'date_from' : 'StatusDate', 
    'date_to'   : 'StatusDate', 
    'id'        : 'FileID', 
    # --- extra ---
    'client'    : 'BankName',
    'complete'  : 'FileStatusID',
    'orderdate' : 'RegisterDate', 
}

# Public constants
filename_splitter = '/'
info_splitter = '::'
point = '.'
observer_prefix = '***'

# Local constants
_INACTIVE = 'inactive'
_REFRESHED = '_refreshed'
_MIN_MESSAGE_SIZE = 20

# Logger DB: OrderLog Database connection name
_database = 'orderlog'

##  -----------------
##  Public Decorators
##  -----------------

def before(name):
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kw):
            global engines
            engines[name] = BankPersoEngine(None, connection=CONNECTION[name])
            return f(*args, **kw)
        return wrapper
    return decorator

def after(name):
    def decorator(f):
        @wraps(f)
        def wrapper(*args):
            if engines[name] is not None:
                engines[name].close()
            return f(*args)
        return wrapper
    return decorator

def set_globals(config):
    global IsDebug, IsDeepDebug, IsTrace, IsExistsTrace, IsDisableOutput, IsObserverTrace
    IsDebug, IsDeepDebug, IsTrace, IsExistsTrace, IsDisableOutput, IsObserverTrace = \
        config.get('debug') or 0, \
        config.get('deepdebug') or 0, \
        config.get('trace') or 0, \
        config.get('existstrace') or 0, \
        config.get('disableoutput') or 0, \
        config.get('observertrace') or 0

##  ============================
##  Abstract Source Logger Class
##  ============================

class BaseEmitter(threading.Thread):
    
    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs=None, daemon=None):
        threading.Thread.__init__(self, group=group, target=target, name=name,
                                  daemon=daemon)

        self._consumer = len(args) > 0 and args[0] or kwargs and kwargs.get('consumer')
        self._emitter = len(args) > 1 and args[1] or kwargs and kwargs.get('emitter') or False
        self._limit = len(args) > 2 and args[2] or kwargs and kwargs.get('limit') or 0
        self._logger = len(args) > 3 and args[3] or kwargs and kwargs.get('logger')

        if IsDebug:
            self._logger.out('emitter init')

        self._processed = 0
        self._found = {}

    def stop(self):
        if IsDebug:
            self._logger.out('emitter stop')

        return self._processed, self._found

    def should_be_stop(self):
        self._consumer.should_be_stop()

    def is_finished(self):
        return self._consumer.is_finished()

    def run(self):
        if IsDebug:
            self._logger.out('emitter run[%s]' % self.ident)

        app = self._consumer
        limit = self._limit

        if app is not None and app.is_ready():
            if self._emitter:
                self._processed, self._found = app.emitter(limit=limit)
            else:
                self._processed, self._found = app(limit=limit)


class AbstractSource(object):

    def __init__(self, config, logger):
        self.config = config
        self.logger = logger

        self.params = {}

        self.source_id = None
        self.module_id = None
        self.log_id = None
        self.message_id = None
        self.status = None
        self.count = 0

        self._module_splitter = ''
        self._engine = None

        self._filename = None
        self._orders = {}
        self._files = {}
        self._lines = []
        self._message = ''
        self._seen = None
        self._callback = None

        self.finished = False
        self.stop = False

    @before(_database)
    def _init_state(self, **kw):
        set_globals(self.config)

        if IsDebug:
            self.logger.out('_init_state: %s -> %s' % (_database, engines[_database].connection))
        
        # --------------------------------
        # Scenario Parameters List (:SPL:)
        # --------------------------------
    
        self._read_seen()

        self.params['date_from'] = getDate(self._seen, format=LOCAL_EASY_DATESTAMP) or kw.get('date_from')
        self.params['client'] = self.config.get('client')
        self.params['complete'] = self.config.get('complete') or COMPLETE_STATUSES

        self._callback = kw.get('callback')

        self.stop = False

    @property
    def debug(self):
        return IsDebug

    @property
    def deepdebug(self):
        return IsDeepDebug

    @property
    def trace(self):
        return IsTrace

    @property
    def disableoutput(self):
        return IsDisableOutput

    def __call__(self, engine=None, limit=None):
        """
            Run the instance.

            Arguments:
                engine -- Database: DB Bankperso engine to get `Orders`
                limit  -- int: number of Order items to perform
        """
        assert engine is not None, "Engine is None!"

        self._engine = engine

        return self._execute(limit)

    def is_ready(self):
        return engines and True or False

    def is_finished(self):
        return self.finished

    def should_be_stop(self):
        self.stop = True

    @after(_database)
    def _term(self):
        pass

    def _evolute_date(self, date_from):
        """
            Check&Change current date.

            Arguments:
                date_from -- datetime: current date
                
            Class parameters:
                _seen     -- datetime: last seen date
        """
        if not date_from:
            return

        if date_from == getDate(self.params['date_from'], format=LOCAL_EASY_DATESTAMP, is_date=True):
            return

        # ----------------------------------
        # Delete the past observed Log-files
        # ----------------------------------

        for filename in [x for x in sorted(self._files.keys())]:
            if not self._is_matched_filename(filename):
                del self._files[filename]

        # ------------
        # Set new date
        # ------------

        if date_from != self._seen:
            self._refresh_seen(date_from)

        self.params['date_from'] = getDate(date_from, format=LOCAL_EASY_DATESTAMP)
        self.config['now'] = getDate(date_from, format=DATE_STAMP)

        # --------------------------
        # Assign up to date errorlog
        # --------------------------

        if self._callback is None:
            return

        self._callback.refreshService()

    def _beforeObserve(self, date_from=None):
        """
            Sets FSO initial Log-file pointers
        """
        self._files = {}
        self._lines = []
        self._message = ''

        if IsDebug:
            self.logger.out('seen: %s' % getDate(self._seen, format=DATE_STAMP))

    def _refresh_seen(self, seen):
        self._seen = seen

        # ---------------
        # Output new seen
        # ---------------

        self._update_seen()

    def _read_seen(self):
        """
            Get last seen date from `seen`-file
        """
        seen = self.config.get('seen')
        if not seen:
            return

        try:
            fo = open(seen, mode='rb')
            line = fo.readline().decode(default_encoding).strip()
            self._seen = getDate(line, format=DATE_STAMP, is_date=True)
            fo.close()
        except:
            pass

    def _update_seen(self):
        """
            Output last seen date into `seen`-file
        """
        seen = self.config.get('seen')
        if not (seen and self._seen):
            return

        try:
            fo = open(seen, mode='wb')
            fo.write(getDate(self._seen, format=DATE_STAMP).encode(default_encoding))
            fo.close()
        except:
            pass

    def _unresolved_lines(self, force=False):
        if IsDeepDebug or force:
            items = []

            # ----------------------------
            # Overstocked Lines collection
            # ----------------------------

            lines = len(self._lines)
            items.append('%s!!! unregistered lines (overstock): %d' % (cr, lines))

            fname = ''
            for filename, line in sorted(self._lines):
                if filename != fname:
                    items.append('--> file: %s' % filename)
                    fname = filename
                items.append(line)

            # -----------------
            # Orders collection
            # -----------------

            if lines > 0:
                items.append('%s!!! orders: %d' % (cr, len(self._orders)))
                columns = ('FileID', _INACTIVE, _REFRESHED, 'FileStatusID', 'id', 'BankName', 'aliases', 'date_from', 'FName', 'keys',)

                for n, id in enumerate(sorted(self._orders.keys())):
                    order = self._orders[id]
                    items.append('%03d: {%s}' % ( \
                        n, 
                        ', '.join(['%s: %s' % (x, str(order.get(x))) for x in columns]),
                    ))

            items.append(cr)

            print_to(None, items)

        return len(self._lines)

    ##  -------------
    ##  Basic Members
    ##  -------------

    def _execute(self, limit, func=None, **kw):
        """
            General Logger Scenario.

            Synopsis:
                FSO : File System Object

            Class parameters:
                date_from   -- datetime[**kw] or string [self.params]: running from the given date (YYYY-MM-DD)

            Arguments:
                limit       -- int: number of Order items to perform
                func        -- callable: function to execute to pick up messages from Logs

            Keyword arguments:
                filename    -- string: path to Log-file passed by observer event

            Returns:
                _processed  -- int: number of Order items successfully performed
                _found      -- dict: number of Log-messages found by order
        """
        _processed = 0
        _found = {}

        self.finished = False

        date_from = kw.get('date_from') or getDate(self.params['date_from'], format=LOCAL_EASY_DATESTAMP, is_date=True)

        # -----------------------------------------
        # Get Orders for given Logger config params
        # -----------------------------------------

        self.getOrders(date_from=date_from)

        orders = [x for x in self._orders.keys() if not self._is_inactive_order(x)]

        for n, id in enumerate(sorted(orders)):
            order = self._orders[id]

            # ---------------------------------------------------------
            # Pick up an Order Log-messages from the given `Source` FSO
            # ---------------------------------------------------------

            if func is not None:

                # ---------------------------------
                # For a Observer's event run `func`
                # ---------------------------------

                done = func(order, id, **kw)
                if done:
                    _found[id] = done

                    # ------------------------------------------------
                    # Finish when the first matched item will be found
                    # ------------------------------------------------

                    break
            else:

                # ----------------------------------
                # Another way, walk the `Source` FSO
                # ----------------------------------

                _found[id] = self.pickupLogs(order, id)

            _processed += 1

            if limit and _processed > limit or self.stop:
                break

        # ---------------
        # Finish Scenario
        # ---------------

        self.finished = True

        return _processed, _found

    def _check_completed(self, date_from, func=None, **kw):
        """
            Explore `lines` for overstock.
            Check all lines for finalized orders.
        """
        if func is None:
            return
        
        orders = self._orders
        lines = self._lines

        self._orders = {}
        self.getOrders(date_from=date_from, finalized=True)

        overstock = []

        for n, id in enumerate(sorted(self._orders.keys())):
            order = self._orders[id]

            # ---------------------------------------
            # Check every line for the selected order
            # ---------------------------------------

            for i, line in enumerate(lines):
                if i in overstock:
                    continue

                self._lines = [line]

                # ----------------------------------------
                # If matched, remove it (just overstocked)
                # ----------------------------------------

                if func(order, id, **kw):
                    overstock.append(i)

        if overstock:
            lines = [lines[i] for i in range(len(lines)) if i not in sorted(overstock)]

        self._lines = lines
        self._orders = orders

    def _formatted_dump(self, ob, title):
        dump = '%s%s%s%s' % ('-'*19, cr, title, cr)

        columns = self.formated_columns

        if ob is not None:
            info = []

            for n, column in enumerate(columns):
                if not (column and column in ob):
                    continue
                elif column in ('Date', 'Module', 'Code',):
                    dump += '%s%s' % (ob[column], cr)
                elif column == 'Message':
                    dump += '%s%s' % (self.getLogMessage(ob), cr)
                else:
                    v = str(ob[column])
                    info.append('%s:%s' % (column, v.strip()))

            dump += ', '.join(info)+cr

            info = []
            info.append('%s:%s' % ('source_id', self.source_id))
            info.append('%s:%s' % ('module_id', self.module_id))
            info.append('%s:%s' % ('log_id', self.log_id))
            info.append('%s:%s' % ('message_id', self.message_id))
            info.append('%s:%s' % ('status', self.status))
            info.append('%s:%s' % ('count', self.count))

            dump += ', '.join(info)+cr

        return dump

    def _processed_log_item(self, ob, current_filename):
        """
            Run scenario for a given Log-item
        """
        filename = current_filename

        is_logged = False

        # If Log-filename changed
        if ob['filename'] != filename or 'Module' in ob:
            filename = ob['filename']

            if IsTrace:
                print_to(None, filename+':')

            # Check Module
            # ~~~~~~~~~~~~
            self.module_id = None
            cname, cpath = self.getModuleInfo(filename, ob, as_list=True)
            self.check_module(source_id=self.source_id, cname=cname, cpath=cpath)

            # Check Log
            # ~~~~~~~~~
            self.log_id = None
            cname = self.getLogInfo(filename, ob, as_list=True)
            self.check_log(source_id=self.source_id, module_id=self.module_id, cname=cname)

        # Check existing & Register Log item
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        self.message_id = None
        self.registerLogItem(filename, ob)

        if not self.status:
            title = '!!! no status'

        elif self.status in 'SMLB':
            title = 'Error: DB-status is invalid[%s]' % self.status
            
        elif self.message_id is not None:
            title = ''
            
            if self.status and self.status.startswith('ID:'):
                title = 'New message[%s%s]' % (self.status, self._message)

                is_logged = True

            elif IsExistsTrace:
                title = self.status

        else:
            title = 'Not registered! MessageID is null'

        if IsTrace and title:
            print_to(None, self._formatted_dump(ob, title))

            if IsDebug and not IsDisableOutput:
                self.logger.out(title)

        return is_logged, filename

    def _pickup_logs(self, logs, **kw):
        filename = kw.get('filename') or ''

        done = 0

        for ob in logs:
            if 'exception' in ob:
                if IsTrace:
                    print_to(None, 'Exception: %s' % ob['exception'])
                continue

            is_logged, filename = self._processed_log_item(ob, filename)

            if is_logged:
                done += 1

        return done

    def _after_launch(self, logs, order_params):
        client, file_id, file_name = order_params

        for ob in logs:
            ob.update({
                'bp_fileid'   : file_id,
                'bp_filename' : file_name,
                'client'      : client,
            })

    def _observer_source(self):
        root = self._log_config()['root']
        return '%s%s' % (self.config.get('root'), root and '/'+root or '')

    def _log_config(self):
        """Override this method to populate `Source` Log-config"""
        return None

    def _log_mask(self, log_config, mode):
        """Override this method to populate `Source` Log-config folder/file mask"""
        return '|'.join(['([\\\\/]+%s)' % x for x in log_config[mode] if x]) or ''

    def _log_regexes(self):
        """Override this method to populate `Observer` file masks"""
        log_config = self._log_config()
        if log_config:
            masks = { \
                'root' : log_config['root'],
                'dir'  : self._log_mask(log_config, 'dir'),
                'file' : self._log_mask(log_config, 'file'),
            }
            regexes = [r'.*%(root)s%(dir)s%(file)s(?si)' % masks]
        else:
            regexes = [r'.*']

        if IsDebug:
            self.logger.out('regexes: %s' % regexes)

        return regexes

    def _is_inactive_order(self, id):
        order = self._orders[id]
        return _INACTIVE in order and order.get(_INACTIVE) or False

    def _is_matched_filename(self, filename):
        """Override this method to check given Log-filename"""
        return True

    def _is_line_valid(self, line):
        columns = line.split(self.split_by)
        return line and len(columns) >= len(self.columns) and len(columns[-1]) > _MIN_MESSAGE_SIZE or False

    def _is_suspended(self, filename, config):
        suspend = config.get('suspend') or []
        for x in suspend:
            if x and x in filename:
                return True
        suppressed = [x.lower() for x in self.config.get('suppressed') or []]
        for x in suppressed:
            if x and x in filename:
                return True
        return False

    def _parse_datefrom(self, filename):
        return None

    def _get_order_param(self, name, as_dict=False):
        if as_dict:
            return dict(zip(('name', 'value'), (args[name], self.params.get(name))))
        return args[name], self.params.get(name)

    def _update_batch(self, row, keys):
        def _update_key(name):
            value = str(row[name])
            if not value in keys:
                keys.append(value)

        _update_key('TID')
        _update_key('TZ')

    def check_source(self, **kw):
        """
            DB-Check if `source` exists.
            
            Keyword arguments:
                root          -- root folder for observe
                ip            -- server IP
                ctype         -- system type as 'bankperso, sdc, exchange...'

            Returns:
                If OK: `SourceID`
        """
        cursor = engines[_database].runProcedure('orderlog-check-source', **kw)
        self.source_id = cursor[0][0] if cursor else None

    def check_module(self, **kw):
        """
            DB-Check if `module` exists.
            
            Keyword arguments:
                source_id     -- SourceID
                cname         -- module name
                cpath         -- path to the log-file folder

            Returns:
                If OK: `ModuleID`
        """
        cursor = engines[_database].runProcedure('orderlog-check-module', **kw)
        self.module_id = cursor[0][0] if cursor else None

    def check_log(self, **kw):
        """
            DB-Check if `log` exists.
            
            Keyword arguments:
                source_id     -- SourceID
                module_id     -- ModuleID
                cname         -- log-file name

            Returns:
                If OK: `LogID`
        """
        cursor = engines[_database].runProcedure('orderlog-check-log', **kw)
        self.log_id = cursor[0][0] if cursor else None

    def register_log_message(self, args=None, **kw):
        """
            Check if exists & Register Log-message.
            
            Keyword arguments:
                [source_id]   -- int: SourceID
                [module_id]   -- int: ModuleID
                [log_id]      -- int: ModuleID
                
                [source_info] -- string: <root:ip:ctype>
                [module_info] -- string: <cname:cpath>
                [log_info]    -- string: <filename>

                fileid        -- int: BP_FileID
                filename      -- string: BP_FileName
                [batchid]     -- int: BP_BatchID
                code          -- message level: INFO, ERROR, OK...
                count         -- int: count of message at the current second
                message       -- string: text of message
                event_date    -- string: event datetime
                rd            -- string: now

            Returns:
                Stored procedure response.
                If OK `LogID`
        """
        self.status = ''
        cursor = engines[_database].runProcedure('orderlog-register-log-message', args, **kw)
        if cursor:
            self.message_id = cursor[0][0]
            self.status = cursor[0][1]
        else:
            if IsDebug:
                self.logger.out('!!! register_log_message, no cursor: %s' % str(args))

    def make_filter(self, date_from=None, finalized=False):
        """
            Make filter `where` for orders selection SQL query.
            Parameters are defined in :SPL:

            Arguments:
                date_from  -- datetime: timestamp of an order
                finalized  -- boolean: build query for completed orders
        """
        where = ''

        items = []

        if self.config.get('check_datefrom') or date_from:
            datefrom = self._get_order_param('date_from', as_dict=True)
            if date_from:
                value = getDate(date_from, format=LOCAL_EASY_DATESTAMP)
            else:
                value = datefrom['value']

            if checkDate(value, LOCAL_EASY_DATESTAMP):
                complete = self._get_order_param('complete', as_dict=True)

                # ===========================
                # Check finalized orders only
                # ===========================

                if finalized:

                    # ---------------------------------------
                    # Date of Status earlier then `date_from`
                    # ---------------------------------------

                    items.append("(%s <= '%s 00:00' and %s in (%s))" % ( \
                            datefrom['name'], 
                            value,
                            complete['name'], 
                            ','.join(['%s' % x for x in complete['value'] if x])
                        ))

                # ==========================================
                # Check orders in progress from a given date
                # ==========================================

                else:

                    # -------------------------------------
                    # Date of Status later then `date_from`
                    # -------------------------------------

                    items.append("(%s >= '%s 00:00' or %s not in (%s))" % ( \
                            datefrom['name'], 
                            value,
                            complete['name'], 
                            ','.join(['%s' % x for x in complete['value'] if x])
                        ))

                    # -------------------------------------------
                    # Order RegisterDate earlier then `date_from`
                    # -------------------------------------------

                    name, x = self._get_order_param('orderdate')
                    items.append("%s <= '%s 23:59'" % (name, value))

        # ------------------------------
        # Orders for a given client only
        # ------------------------------

        name, value = self._get_order_param('client')
        if value and value != '*':
            items.append("%s='%s'" % (name, value))

        if items:
            where += ' and '.join(items)

        return where

    def getLogMessage(self, ob):
        if 'Message' not in ob:
            return ''
        message = ob['Message']
        return message.strip()

    def getSourceInfo(self, as_list=False):
        root, ip, ctype = self.config.get('root'), self.config.get('ip'), self.config.get('ctype')
        if as_list:
            return root, ip, ctype
        return info_splitter.join([root, ip, ctype])

    def getModuleInfo(self, filename, ob=None, as_list=False):
        log_name = filename.split(filename_splitter)[-1]
        cpath = re.sub(r'/%s' % log_name, '', filename)
        cname = cpath.split(self._module_splitter)[1]
        if as_list:
            return cname, cpath
        return info_splitter.join([cname, cpath])

    def getLogInfo(self, filename, ob=None, as_list=False):
        cname = filename.split(filename_splitter)[-1]
        if as_list:
            return cname
        return '%s' % cname

    def getEventDate(self, ob):
        return ob.get('Date')

    def getMessageCount(self, ob):
        self.count = 1
        return self.count

    def registerLogItem(self, filename, ob):
        return self.register_log_message((
            self.source_id,
            self.module_id,
            self.log_id,
            self.getSourceInfo(),
            self.getModuleInfo(filename, ob),
            self.getLogInfo(filename, ob),
            ob.get('bp_fileid'),
            None,
            ob.get('client'),
            ob.get('bp_filename'),
            ob.get('Code'),
            self.getMessageCount(ob),
            self.getLogMessage(ob),
            self.getEventDate(ob),
            getDate(getToday(), format=UTC_FULL_TIMESTAMP)
        ))

    def getOrders(self, date_from=None, finalized=False):
        """
            Get Bankperso Orders list.
            
            Class properties:
                _engine    -- connected engine to BankDB
                _orders    -- selected orders list: FileID, FName...

            Arguments:
                date_from  -- datetime: current date for orders sellections
                finalized  -- boolean: build query for completed orders

            Returns length of `active` orders (int).
        """
        engine = self._engine

        orders = {}

        order = 'FileID'
        where = self.make_filter(date_from=date_from, finalized=finalized)

        columns = ('FileID', 'FName', 'BankName', 'FileStatusID',)

        active = []

        cursor = engine.runQuery('orders', columns=columns, where=where, order=order, as_dict=True,
                                 encode_columns=('BankName',),
                                 distinct=True)
        if cursor:
            for n, row in enumerate(cursor):
                id = row['id'] = row['FileID']
                row['date_from'] = date_from

                if not id:
                    continue
                elif not id in self._orders:
                    orders[id] = row
                elif row['FileStatusID'] != self._orders[id]['FileStatusID'] and self._orders[id].get(_REFRESHED):
                    self._orders[id][_REFRESHED] = False

                active.append(id)

        # ----------------------------------------
        # Update orders state in class collections
        # ----------------------------------------

        self._orders.update(orders)

        for id in self._orders:
            self._orders[id][_INACTIVE] = id not in active

        return len(active)

    def pickupLogs(self, order, id):
        """
            Pick up Order Log-messages.

            Arguments:
                order  -- dict: DB Bankperso order
                id     -- int: FileOrder ID

            Returns:
                _found -- int: number of Log-messages found
        """
        logs = self.getLogs(order, date_format=UTC_FULL_TIMESTAMP, case_insensitive=True, no_span=True)

        if IsTrace and len(logs) > 0:
            print_to(None, '%sID:%s LOGS[%s]:%s' % (cr, id, len(logs), cr))

        return self._pickup_logs(logs)

    def emitter(self, engine, limit):
        """
            Lines Emitter Scenario.

            Config parameters:
                check_filename -- flag: match Log filename with client name or alias
                decoder_trace  -- flag: prints decoder errors
        """
        _processed = 0
        _found = {}

        self._engine = engine

        date_from = getDate(self.params['date_from'], format=LOCAL_EASY_DATESTAMP, is_date=True)
        check_filename = self.config.get('check_filename') or False
        decoder_trace = self.config.get('decoder_trace') or False

        client = self.config.get('client')

        keys = []
        if client and client != '*':
            keys = [self.config.get(x).lower() for x in ('client', 'alias')]

        suppressed = [x.lower() for x in self.config.get('suppressed') or []]

        self.finished = False

        # -------------------------
        # Set Logs-files collection
        # -------------------------

        self._beforeObserve(date_from=date_from)

        # -----------------------------------------
        # Get Orders for given Logger config params
        # -----------------------------------------

        self.getOrders(date_from=date_from)

        # -----------------
        # Observe Log-files
        # -----------------

        is_break = False

        for n, filename in enumerate(sorted(self._files.keys())):
            if is_break or self.stop:
                break

            # ---------------------------------------------------------------
            # Skip filename if supressed or not matched with given client key
            # ---------------------------------------------------------------

            fname = filename.lower()

            if check_filename and keys:
                found = False
                for x in keys:
                    if x and x in fname:
                        found = True
                        break
                if not found:
                    if IsDebug:
                        self.logger.out('skipped: %s' % filename)
                    continue

            if suppressed:
                found = False
                for key in suppressed:
                    if key and key in fname:
                        found = True
                        break
                if found:
                    if IsDebug:
                        self.logger.out('suppressed: %s' % filename)
                    continue

            self._message = ':%d-%d' % (len(self._files), n+1,)

            # -----------------------------------
            # Remember name of processed Log-file
            # -----------------------------------

            self._filename = filename

            # ----------------------------------------------
            # Set Logs pointers at the beginning of the file
            # ----------------------------------------------

            if IsDebug:
                self.logger.out('file: %s [%d]' % (filename, self._files[filename]))

            self._files[filename] = 0

            # -------------------------
            # Refresh Orders collection
            # -------------------------

            if self.getOrders(date_from=self._parse_datefrom(filename)) == 0:
                if IsDebug:
                    self.logger.out('inactive: %s' % filename)
                continue

            # ---------------------------------------
            # Generate Log-lines stream from the file
            # ---------------------------------------

            for l, line in enumerate(lines_emitter(filename, 'rb', default_unicode, 'EMITTER', 
                                                   decoder_trace=decoder_trace,
                                                   files=self._files,
                                                   globals=self.config,
                                                   )):
                if IsDeepDebug:
                    self.logger.out('line %d: [%s]' % (l, len(line)))

                # -----------------------------------
                # Skip invalid (broken) Log-line data
                # -----------------------------------

                if not self._is_line_valid(line):
                    continue

                if IsTrace and IsDeepDebug:
                    print_to(None, '%s' % line)

                # ------------------------------------------
                # Add Log-line to launching lines-collection
                # Never lade emitter by recurring lines!!!
                # ------------------------------------------

                self._lines = [(filename, line,)] #self._lines.append((filename, line,))

                # --------------------------
                # Check Order via given line
                # --------------------------

                orders = [x for x in self._orders.keys() if not self._is_inactive_order(x)]

                for i, id in enumerate(sorted(orders)):
                    order = self._orders[id]

                    if not id in _found:
                        _found[id] = 0

                    # ----------------------------
                    # Match Log-line with an Order
                    # ----------------------------

                    logged = self.launchEvent(order, id, case_insensitive=True, no_span=True)

                    if not logged:
                        continue

                    _found[id] += logged
                    _processed += 1

                    # ---------------------------------------------
                    # If Log-line done, break and take the next one
                    # ---------------------------------------------

                    break

                # -----------------
                # Check lines limit
                # -----------------

                if limit and _processed > limit:
                    is_break = True

                    break

        self._lines = []

        # ---------------
        # Finish Scenario
        # ---------------

        self.finished = True

        return _processed, _found

    ##  ------------------------
    ##  Observer Consume Members
    ##  ------------------------

    def watch(self, event):
        """
            Observer supplies FSO path for Log-file which logged some new messages.
            We should read that new Log-lines, sort its by `Order` and register in DB in case of success.
            
            It's laded by recurring lines.

            Class items:
                self._files -- dict: FSO pointers for launched Log-files
                self._lines -- list: new Log-lines detected on respond of Observer event

            Arguments:
                event -- FileSystemEvent: observer handler event
        """
        filename = normpath(event.src_path)

        if not (filename and self._is_matched_filename(filename)):
            self._filename = None

            if IsTrace and IsDebug:
                print_to(None, '!!! no matched filename[%s]: %s' % (filename, repr(event)))

            return

        self._filename = filename

        decoder_trace = self.config.get('decoder_trace') or False
        stack_events = self.config.get('stack_events') or False

        # -----------------------------------------
        # Get lines from the file last seen pointer
        # -----------------------------------------

        if not stack_events:
            self._lines = []

        checkfile(filename, 'rb', default_unicode, None, [], None, 'OBSERVER', decoder_trace=decoder_trace,
                  files=self._files, 
                  lines=self._lines,
                  globals=self.config,
                  )

        if IsDebug:
            self.logger.out('*** Log event: %s, lines: %d [%d]' % (self._filename, len(self._lines), self._files[filename]))

    def onFileCreated(self, event):
        """
            A new file under `root` folder was created.
            We should check it and register a new type of Logs-events as `self._files` item.

            Class items:
                self._files -- dict: FSO pointers for launched Log-files

            Arguments:
                event -- FileSystemEvent: observer handler event
        """
        filename = normpath(event.src_path)

        if not self._is_matched_filename(filename):
            return

        self._files[filename] = 0

        if IsDebug:
            self.logger.out('>>> file created: %s' % filename)

    def onFileDeleted(self, event):
        """
            Some file under `root` folder was deleted.
            We should unregister corresponding Logs-events from `self._files` collection.

            Class items:
                self._files -- dict: FSO pointers for launched Log-files

            Arguments:
                event -- FileSystemEvent: observer handler event
        """
        filename = normpath(event.src_path)

        if not filename in self._files:
            return

        del self._files[filename]

        if IsDebug:
            self.logger.out('>>> file deleted: %s' % filename)

    def onFileMoved(self, event):
        """
            Some file under `root` folder was moved.
            We should update corresponding Logs-events in `self._files` collection.

            Class items:
                self._files -- dict: FSO pointers for launched Log-files

            Arguments:
                event -- FileSystemEvent: observer handler event (`dest_path` - new location)
        """
        filename = normpath(event.src_path)
        new = normpath(event.dest_path)

        if not filename in self._files or not new:
            return

        del self._files[filename]
        self._files[new] = 0

        if IsDebug:
            self.logger.out('>>> file moved from: %s to: %s' % (filename, new))

    def launchObserverEvent(self):
        """
            Consume the watched Observer event.

            Returns:
                logged -- int: count of Log-messages performed successfully
        """
        if not (self._filename and self._lines):

            if IsTrace and IsDebug:
                print_to(None, '!!! no lanched: %s, lines: [%d]' % (self._filename, len(self._lines)))

            return 0

        date_from = self._parse_datefrom(self._filename)

        # ----------------------------
        # Check Current Date evolution
        # ----------------------------

        self._evolute_date(date_from)

        # ------------
        # Launch Event
        # ------------

        _processed, _found = self._execute(0, func=self.launchEvent, date_from=date_from,
                                           date_format=UTC_FULL_TIMESTAMP, case_insensitive=True, no_span=True, 
                                           observer=True)

        logged = _found and sum([v for k,v in _found.items()]) or 0

        if IsDebug:
            self.logger.out('*** Logged: %d' % logged)

        n = len(self._lines)
        force = False

        if n > MAX_UNRESOLVED_LINES[1]:
            force = True #IsDeepDebug and True or False

        elif n > MAX_UNRESOLVED_LINES[0] and n%MAX_UNRESOLVED_LINES[2] == 0:
            self._check_completed(date_from, func=self.launchEvent,
                                  date_format=UTC_FULL_TIMESTAMP, case_insensitive=True, no_span=True,
                                  no_pickup=False)

            if IsDebug:
                self.logger.out('*** Overstock: %d' % len(self._lines))

        self._unresolved_lines(force=force)

        if force:
            self._lines = []

            if IsDebug:
                self.logger.out('*** Overstock: reset')

        return logged 


##  ================
##  Observer Classes
##  ================

def observer_trace(message, lock, event=None):
    print_to(None, '%s %s %s%s, locked: %s' % ( \
             getTime(format=UTC_FULL_TIMESTAMP), 
             observer_prefix,
             message, 
             event and (' %s' % repr(event)) or '',
             lock.locked(),
             ))


class LogConsumer(threading.Thread):
    
    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs=None, daemon=None):
        threading.Thread.__init__(self, group=group, target=target, name=name,
                                  daemon=daemon)
        self._should_be_run = True
        self._consumer = len(args) > 0 and args[0] or kwargs and kwargs.get('consumer')
        self._producer = len(args) > 1 and args[1] or kwargs and kwargs.get('producer')
        self._lock = len(args) > 2 and args[2] or kwargs and kwargs.get('lock')
        self._logger = len(args) > 3 and args[3] or kwargs and kwargs.get('logger')

        if IsDebug:
            self._logger.out('observer init')

        self._found = {}

    def stop(self):
        if IsDebug:
            self._logger.out('observer stop')

        self._should_be_run = False
        return self._found

    def run(self):
        if IsDebug:
            self._logger.out('observer run[%s]' % self.ident)

        while self._should_be_run:
            time.sleep(1)

            if IsObserverTrace and IsDeepDebug:
                observer_trace('observer attempts to get event', self._lock)

            event = None

            try:
                with self._lock:
                    if not self._producer.is_empty():
                        event = self._producer.next_event()
                        self._consumer.watch(event)
                    else:
                        continue

                if IsObserverTrace:
                    observer_trace('extracted event', self._lock, event=event)

                logged = self._consumer.launchObserverEvent()

                with self._lock:
                    done_event = self._producer.pop()

            except:
                print_exception()

            if event.key != done_event.key:
                self._logger.out('!!! check observer: %s' % repr(event))

            if logged > 0:
                key = event.src_path
                if key not in self._found:
                    self._found[key] = 0
                self._found[key] += logged

        if IsDebug:
            self._logger.out('observer finish')


class LogProducer(RegexMatchingEventHandler):

    def __init__(self, consumer, lock, **kw):
        self._consumer = consumer

        regexes = consumer._log_regexes()
        super(LogProducer, self).__init__(regexes=regexes)

        self._lock = lock
        self._source = kw.get('source')
        self._logger = kw.get('logger')

        self._stack = []

        if IsDebug:
            self._logger.out('LogProducer[%s] activated' % self._source)

    def exists(self, event):
        return False #event.key in [e.key for e in self._stack]

    def is_empty(self):
        return True if not self._stack or len(self._stack) == 0 else False

    def push(self, event):
        """
            Register given event in the Producer queue.

            Arguments:
                event        -- FileSystemEvent: observer handler event

            Event atributes:
                event_type   -- string: The type of the event as a string: 'moved:deleted:created:modified'
                src_path     -- string: FSO path of the file system object that triggered this event
                is_directory -- bool: True if event was emitted for a directory, False otherwise

            Properies:
                key          -- tuple: (self.event_type, self.src_path, self.is_directory)
        """
        if not event:
            return

        with self._lock:
            if not self.exists(event):
                self._stack.append(event)

                if IsObserverTrace:
                    observer_trace('registered a new event', self._lock, event=event)

    def is_file(self, event):
        return 0 if event.is_directory else 1

    def on_moved(self, event):
        super(LogProducer, self).on_moved(event)

        if not self.is_file(event):
            return

        with self._lock:
            self._consumer.onFileMoved(event)

        if IsObserverTrace:
            observer_trace('on-moved event', self._lock, event=event)

    def on_created(self, event):
        super(LogProducer, self).on_created(event)

        if not self.is_file(event):
            return

        with self._lock:
            self._consumer.onFileCreated(event)

        if IsObserverTrace:
            observer_trace('on-created event', self._lock, event=event)

    def on_deleted(self, event):
        super(LogProducer, self).on_deleted(event)

        if not self.is_file(event):
            return

        with self._lock:
            self._consumer.onFileDeleted(event)

        if IsObserverTrace:
            observer_trace('on-deleted event', self._lock, event=event)

    def on_modified(self, event):
        super(LogProducer, self).on_modified(event)

        if not self.is_file(event):
            return

        self.push(event)

    def next_event(self):
        """
            Return event which is next in the queue, but not extract it
        """
        return self._stack[0]

    def pop(self):
        """
            Extract the first event from the Producer queue (FIFO)
        """
        return self._stack.pop(0)
