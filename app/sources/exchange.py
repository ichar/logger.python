# -*- coding: utf-8 -*-

from . import *
from ..worker import exchange_log_config, check_exchange_log, getExchangeLogInfo

# Source DB: BankDB Database connection name
_database = 'bankperso'

# Local constant
_RE_FILEDATE = re.compile(r'.*_(\d{2}\.\d{2}\.\d{4}).*')
_MIN_MESSAGE_SIZE = 30

##  ============================
##  Exchange Source Logger Class
##  ============================

class Source(AbstractSource):

    def __init__(self, config, logger): 
        super(Source, self).__init__(config, logger)

        self._module_splitter = '_logfile_'

    @before(_database)
    def _init_state(self, **kw):
        super(Source, self)._init_state(**kw)

        if self.trace and not self.disableoutput:
            self.logger.out('_init_state: %s -> %s' % (_database, engines[_database].connection))

        self.view = database_config['exchangelog']
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

        getExchangeLogInfo(dates=dates,
                           client=client,
                           fmt=DEFAULT_DATETIME_EXCHANGELOG_FORMAT, 
                           date_format=None,
                           config=config,
                           files=self._files,
                           pointers=True,
                           globals=self.config,
                           )

        if self.debug:
            self.logger.out('%s exchange files found: %d' % (observer_prefix, len(self._files)))

        self._refresh_seen(seen)

    def _log_config(self):
        """
            Populates `Source` Log-config from `worker.py` module
        """
        return exchange_log_config

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
        return True if not self._is_suspended(filename, exchange_log_config) and \
            getTime(format=DEFAULT_DATETIME_EXCHANGELOG_FORMAT[0]) in filename else False

    def _is_line_valid(self, line):
        if not line:
            return False

        options = self.config.get('options')
        is_jzdo = 'jzdo' in options and 'jzdo' in self._filename.lower()

        if self.split_by in line:
            columns = line.split(self.split_by)
            n = len(self.columns)
        else:
            v = line.split()
            n = len(self.columns) - (is_jzdo and 1 or 0)

            if len(v) > n:
                v[n-1] = ' '.join([x.strip() for x in v[n-1:]])
                columns = v[0:n]
            else:
                columns = v

        def _smb_valid(value):
            for s in value:
                x = s.encode(default_encoding)
                if not (x <= b'\x7f' or x >= b'\xc0'):
                    return False
            return True

        date = columns[1] if is_jzdo else columns[0]
        """
        x1 = date[0].isdigit()
        x2 = checkDate(date, DEFAULT_DATETIME_EXCHANGELOG_FORMAT[0])
        x3 = len(columns) == n
        s = _smb_valid(columns[-1])
        x4 = not s
        x5 = len(columns[-1]) > _MIN_MESSAGE_SIZE
        return x1 and x2 and x3 and x4 and x5 or False
        """
        return date[0].isdigit() and checkDate(date, DEFAULT_DATETIME_EXCHANGELOG_FORMAT[0]) and \
               len(columns) == n and _smb_valid(columns[-1]) and \
               len(columns[-1]) > _MIN_MESSAGE_SIZE or \
               False

    def _parse_datefrom(self, filename):
        m = _RE_FILEDATE.match(filename.split('/')[-1])
        date_from = m and m.group(1)
        format = DEFAULT_DATETIME_EXCHANGELOG_FORMAT[0]
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

        refreshed = order.get('_refreshed', False)
        if kw.get('observer'):
            refreshed = refreshed if not self.config.get('forced_refresh') else False

        date_from = getDateOnly(order.get('date_from') or getToday())
        date_to = getToday()

        columns = self.columns
        split_by = self.split_by

        # ----------------------------
        # Get Exchange Order Log Items
        # ----------------------------

        engine = engines[_database]

        keys = []
        aliases = []

        if file_id is not None and not refreshed:
            keys.append(str(file_id))

            if file_name:
                keys.append(file_name)
                if point in file_name:
                    keys.append(file_name.split(point)[0])

                order['_refreshed'] = True

            dates = (date_from, date_to,)

        else:
            keys = order.get('keys') or []
            dates = order.get('dates') or (date_from, date_to,)

        if not 'aliases' in order:
            if client:
                aliases.append(client)

                # -----------------------
                # Get Client Aliases list
                # -----------------------

                where = "Aliases like '%" + client +"%' or Name='" + client + "'"
    
                cursor = engine.runQuery('orderstate-aliases', where=where, as_dict=True)
                if cursor:
                    for n, row in enumerate(cursor):
                        if row['Aliases'] is not None and len(row['Aliases']):
                            aliases.extend(row['Aliases'].split(':'))

            if file_name:
                if file_name.startswith('OCG') or file_name.startswith('PPCARD'):
                    aliases += 'JZDO'
                    split_by = ' '

            if len(aliases):
                aliases = list(set(aliases))

        else:
            aliases = order.get('aliases') or []

        order['keys'] = keys
        order['aliases'] = aliases

        return config, (client, file_id, file_name,), (keys, columns, dates, aliases, split_by,)

    ##  -------
    ##  Private
    ##  -------

    def _split_module_name(self, cname):
        rmodule = re.compile(r'(.*)(\[([\d]+)\])+?')
        m = rmodule.search(cname)
        if m:
            module = m.group(1)
            count = len(m.groups()) == 3 and int(m.group(3)) or 0
        else:
            module = cname
            count = 1
        return module, count

    ##  -------------------
    ##  Log-item properties
    ##  -------------------

    def getModuleInfo(self, filename, ob=None, as_list=False):
        log_name = filename.split(filename_splitter)[-1]
        cpath = re.sub(r'/%s' % log_name, '', filename)
        cname = self._split_module_name(ob.get('Module') or '')[0]
        if as_list:
            return cname, cpath
        return info_splitter.join([cname, cpath])

    def getLogInfo(self, filename, ob=None, as_list=False):
        cname = filename.split(filename_splitter)[-1]
        if as_list:
            return cname
        return '%s' % cname

    def getEventDate(self, ob):
        return re.sub(r'\.[\d]+', '', ob.get('Date'))

    def getMessageCount(self, ob):
        self.count = self._split_module_name(ob.get('Module') or '')[1]
        return self.count

    ##  ------
    ##  Public
    ##  ------

    def getLogs(self, order, **kw):
        """
            Picks up the given Order Log items from the `source` filesystem root folder.
            
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

        logs = getExchangeLogInfo(keys=keys, split_by=split_by, columns=columns, dates=dates, client=client, aliases=aliases,
                                  fmt=DEFAULT_DATETIME_EXCHANGELOG_FORMAT, 
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

        check_exchange_log(logs, '', encoding, 
                           keys=keys,
                           split_by=split_by,
                           columns=columns,
                           fmt=DEFAULT_DATETIME_EXCHANGELOG_FORMAT, 
                           date_format=UTC_FULL_TIMESTAMP,
                           case_insensitive=kw.get('case_insensitive'),
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
