# -*- coding: utf-8 -*-

from copy import deepcopy

from . import *
from ..worker import perso_log_config, check_perso_log, getPersoLogInfo

# Source DB: BankDB Database connection name
_database = 'bankperso'

# Local constant
_RE_FILEDATE = re.compile(r'(\d{8})_.*')
_MIN_MESSAGE_SIZE = 20

##  =============================
##  Bankperso Source Logger Class
##  =============================

class Source(AbstractSource):

    def __init__(self, config, logger): 
        super(Source, self).__init__(config, logger)

        self._module_splitter = 'Log_'

    @before(_database)
    def _init_state(self, **kw):
        super(Source, self)._init_state(**kw)

        if self.trace and not self.disableoutput:
            self.logger.out('_init_state: %s -> %s' % (_database, engines[_database].connection))

        self.view = database_config['persolog']
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

        getPersoLogInfo(dates=dates,
                        client=client,
                        fmt=DEFAULT_DATETIME_PERSOLOG_FORMAT, 
                        date_format=None,
                        config=config,
                        files=self._files,
                        pointers=True,
                        globals=self.config,
                        )

        if self.debug:
            self.logger.out('%s bankperso files found: %d' % (observer_prefix, len(self._files)))

        self._refresh_seen(seen)

    def _log_config(self):
        """
            Populates `Source` Log-config from `worker.py` module
        """
        log_root = self.config.get('log_root')
        if log_root:
            config = deepcopy(perso_log_config)
            config['root'] = log_root
            return config
        return perso_log_config

    def _is_matched_filename(self, filename):
        """
            Checks if given Log-file matched with current date or any custom circumstances
        """
        return True if not self._is_suspended(filename, perso_log_config) and \
            getTime(format=DEFAULT_DATETIME_PERSOLOG_FORMAT[0]) in filename else False

    def _parse_datefrom(self, filename):
        m = _RE_FILEDATE.match(filename.split('/')[-1])
        date_from = m and m.group(1)
        format = DEFAULT_DATETIME_PERSOLOG_FORMAT[0]
        return date_from and checkDate(date_from, format) and getDate(date_from, format=format, is_date=True) or None

    def _make_logger_config(self):
        root = self.config.get('root')
        encoding = self.config.get('encoding') or default_encoding

        # Config should locate at `root` folder with the given `encoding`
        # Can be used extra params (see `worker.py`):
        #   `filemask` and `options`
        # If `root` is empty, default config.BP_ROOT will be used

        config = (encoding, root) if root else None
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

        # -----------------------------
        # Get Bankperso Order Log Items
        # -----------------------------

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
            pass
        else:
            aliases = order.get('aliases') or []

        if not refreshed:
            order['keys'] = keys
            order['aliases'] = aliases

        return config, (client, file_id, file_name,), (keys, columns, dates, aliases, split_by,)

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

        logs = getPersoLogInfo(keys=keys, split_by=split_by, columns=columns, dates=dates, client=client,
                               fmt=DEFAULT_DATETIME_PERSOLOG_FORMAT, 
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

        encoding, root = config

        logs = []

        check_perso_log(logs, '', encoding, 
                        keys=keys,
                        split_by=split_by,
                        columns=columns,
                        fmt=DEFAULT_DATETIME_PERSOLOG_FORMAT, 
                        date_format=UTC_FULL_TIMESTAMP,
                        case_insensitive=kw.get('case_insensitive'),
                        unresolved=kw.get('unresolved'),
                        no_span=kw.get('no_span'),
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

