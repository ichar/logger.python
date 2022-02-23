# -*- coding: utf-8 -*-

from . import *
from ..worker import sdc_log_config, check_sdc_log, getSDCLogInfo

# Source DB: BankDB Database connection name
_database = 'bankperso'

# Local constant
_RE_FILEDATE = re.compile(r'.*_(\d{2}\.\d{2}\.\d{4}).*')

##  =======================
##  SDC Source Logger Class
##  =======================

class Source(AbstractSource):

    def __init__(self, config, logger): 
        super(Source, self).__init__(config, logger)

        self._module_splitter = 'sdc_'

    @before(_database)
    def _init_state(self, **kw):
        super(Source, self)._init_state(**kw)

        if self.trace and not self.disableoutput:
            self.logger.out('_init_state: %s -> %s' % (_database, engines[_database].connection))

        self.view = database_config['sdclog']
        self.columns = self.view['export']
        self.formated_columns = list(self.columns) + ['bp_fileid', 'bp_filename', 'client']
        self.split_by = '\t'
        
        # ------------
        # Check Source
        # ------------

        root, ip, ctype = self.getSourceInfo(as_list=True)
        self.check_source(root=root, ip=ip, ctype=ctype)

    @after(_database)
    def _term(self):
        super(Source, self)._term()

    def __call__(self, engine=None, limit=None):
        return super(Source, self).__call__(engine=engines[_database], limit=limit)

    def is_dead(self, force=None):
        engine = engines[_database]

        try:
            engine = check_engine(engine, force=force)
        except:
            engine = None

        return engine is None or super(Source, self).is_dead(force=force)

    def emitter(self, engine=None, limit=None):
        return super(Source, self).emitter(engines[_database], limit)

    ##  ------------
    ##  Overridences
    ##  ------------

    def _beforeObserve(self, date_from=None):
        """
            Sets FSO initial Log-files state
        """
        super(Source, self)._beforeObserve()

        seen = getDateOnly(date_from or getToday())
        dates = (seen, None,)

        config = self._make_logger_config()
        client = self.config.get('client')

        aliases = self._get_client_aliases(client)

        getSDCLogInfo(dates=dates, client=client, aliases=aliases,
                      fmt=DEFAULT_DATETIME_SDCLOG_FORMAT, 
                      date_format=None,
                      config=config,
                      files=self._files,
                      pointers=True,
                      globals=self.config,
                      )

        if self.debug:
            self.logger.out('%s sdc files found: %d' % (observer_prefix, len(self._files)))

        self._refresh_seen(seen)

    def _log_config(self):
        """
            Populates `Source` Log-config from `worker.py` module
        """
        return sdc_log_config

    def _log_regexes(self):
        """
            Populates `Observer` file masks
        """
        log_config = self._log_config()
        if log_config:
            masks = { \
                'root' : log_config['root'],
                'dir'  : '(.*)?',
                'file' : self._log_mask(log_config, 'file'),
            }
            regexes = [r'.*%(root)s%(dir)s%(file)s(?si)' % masks]
        else:
            regexes = [r'.*']

        if self.debug:
            self.logger.out('regexes: %s' % regexes)

        return regexes

    def _is_matched_filename(self, filename):
        """
            Checks if given Log-file matched with current date or any custom circumstances
        """
        return True if not self._is_suspended(filename, sdc_log_config) and \
            getTime(format=DEFAULT_DATETIME_SDCLOG_FORMAT[0]) in filename else False

    def _parse_datefrom(self, filename):
        m = _RE_FILEDATE.match(filename.split('/')[-1])
        date_from = m and m.group(1)
        format = DEFAULT_DATETIME_SDCLOG_FORMAT[0]
        return date_from and checkDate(date_from, format) and getDate(date_from, format=format, is_date=True) or None

    def _make_logger_config(self):
        root = self.config.get('root')
        encoding = self.config.get('encoding') or default_encoding
        filemask = self.config.get('filemask')
        options = self.config.get('options')

        # Config should locate at `root` folder with the given `encoding`
        # with used extra params (see `worker.py`):
        #   `filemask` and `options`
        # If `root` is empty, default config.BP_ROOT will be used

        config = (encoding, root, filemask, options) if root else None
        return config

    def _make_logger_params(self, order, **kw):
        """
            Makes `worker.py` parameters
        """
        config = self._make_logger_config()

        # Check parameters of the `order`

        file_id = order.get('FileID')
        file_name = order.get('FName')
        client = order.get('BankName')

        refreshed = order.get(ORDER_REFRESHED, False)
        if kw.get('observer'):
            refreshed = refreshed if not self.config.get('forced_refresh') else False

        date_from = getDateOnly(order.get('date_from') or getToday())
        date_to = getToday()

        columns = self.columns
        split_by = self.split_by

        # -----------------------
        # Get SDC Order Log Items
        # -----------------------

        engine = engines[_database]

        keys = []
        aliases = []

        if file_id is not None and not refreshed:
            keys.append(str(file_id))

            if file_name:
                keys.append(file_name)
                if point in file_name:
                    keys.append(file_name.split(point)[0])

            where = 'FileID = %s' % file_id

            # -------------------
            # Get Batches/TZ info
            # -------------------

            cursor = engine.runQuery('batches', where='FileID=%s' % file_id, order='TID', as_dict=True)
            if cursor:
                for n, row in enumerate(cursor):
                    self._update_batch(row, keys)

                order[ORDER_REFRESHED] = len(cursor) > 0

            dates = (date_from, date_to,)

        else:
            keys = order.get('keys') or []
            dates = order.get('dates') or (date_from, date_to,)

        if not 'aliases' in order:
            pass #aliases = self._get_client_aliases(client)
        else:
            aliases = order.get('aliases') or []

        if not refreshed:
            order['keys'] = keys
            order['aliases'] = aliases

        return config, (client, file_id, file_name,), (keys, columns, dates, aliases, split_by,)

    ##  -------------------
    ##  Log-item properties
    ##  -------------------

    def getModuleInfo(self, filename, ob=None, as_list=False):
        log_name = filename.split(filename_splitter)[-1]
        cpath = re.sub(r'/%s' % log_name, '', filename)

        # Get `Module` cname via `config.filemask`
        filemask = self.config.get('filemask')
        rfile = re.compile(filemask)
        m = rfile.search(log_name)
        cname = m and m.group(1) or ''

        if as_list:
            return cname, cpath
        return info_splitter.join([cname, cpath])

    ##  ------
    ##  Public
    ##  ------

    def refreshOrder(self, order):
        self._make_logger_params(order)
        order[ORDER_REFRESHED] = True

    def getLogs(self, order, **kw):
        """
            Picks up the given Order Log items from the `Source` FSO root folder.
            
            Arguments:
                order   -- dict: Order item

            Keyword arguments:
                date_format      -- format of date for `walk` (DEFAULT_DATETIME_FORMAT by default)
                case_insensitive -- boolean: use case-insensitive keys check
                no_span          -- boolean: make `span` tag or not

            Returns:
                logs    -- list: picked log-items collection
        """
        config, order_params, log_params = self._make_logger_params(order)

        client, file_id, file_name = order_params
        keys, columns, dates, aliases, split_by = log_params

        logs = getSDCLogInfo(keys=keys, split_by=split_by, columns=columns, dates=dates, client=client, aliases=aliases,
                             fmt=DEFAULT_DATETIME_SDCLOG_FORMAT, 
                             date_format=UTC_FULL_TIMESTAMP,
                             case_insensitive=kw.get('case_insensitive'),
                             no_span=kw.get('no_span'),
                             config=config,
                             globals=self.config,
                             )

        self._after_launch(logs, order_params)

        return logs

    def launchEvent(self, order, id, **kw):
        """
            Tries to match Log's FSO event with the given Order.

            Arguments:
                order            -- dict: Order item
                id               -- int: FileOrder ID

            Keyword arguments:
                filename         -- string: path to Log-file passed by observer event
                date_format      -- format of date for `walk` (DEFAULT_DATETIME_FORMAT by default)
                case_insensitive -- boolean: use case-insensitive keys check
                no_span          -- boolean: make `span` tag or not
                no_pickup        -- boolean: don't pickup lines (for Overstock only)

            Makes `logs` with checked Log-info.
            Log-file pointer is kept only if messages for given `filename` and `order` were matched and registered successfully.
        """
        no_pickup = kw.get('no_pickup') or False
        config, order_params, log_params = self._make_logger_params(order, **kw)

        client, file_id, file_name = order_params
        keys, columns, dates, aliases, split_by = log_params

        encoding, root, filemask, options = config

        logs = []

        check_sdc_log(logs, '', encoding, 
                      keys=keys,
                      split_by=split_by,
                      columns=columns,
                      fmt=DEFAULT_DATETIME_SDCLOG_FORMAT, 
                      date_format=UTC_FULL_TIMESTAMP,
                      case_insensitive=kw.get('case_insensitive'),
                      unresolved=kw.get('unresolved'),
                      no_span=kw.get('no_span'),
                      options=options,
                      lines=self._lines,
                      globals=self.config,
                      )

        if logs and no_pickup:
            return 1

        self._after_launch(logs, order_params)

        if self.trace and len(logs) > 0:
            print_to(None, '%sID:%s LOGS[%s]:%s' % (cr, id, len(logs), cr))

        logged = self._pickup_logs(logs, **kw)

        return logged
