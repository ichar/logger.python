# -*- coding: utf-8 -*-

import os
import sys
import datetime
import traceback
import re

from collections import Iterable

basedir = \
    os.path.split(sys.executable)[1] == 'service.exe' and 'C:/apps/LoggerService' or \
    os.path.abspath(os.path.dirname(__file__))

# ----------------------------
# Global application constants
# ----------------------------

IsDebug                = 1  # Debug[stdout]: prints general info
IsDeepDebug            = 0  # Debug[stdout]: prints detailed info, replicate to console
IsTrace                = 1  # Trace[errorlog]: output execution trace
IsLogTrace             = 1  # Trace[errorlog]: output detailed trace for Log-actions
IsObserverTrace        = 1  # Trace[errorlog]: output detailed trace for Log Events Observer
IsExistsTrace          = 1  # Flag: prints DB-status for existent Log-items
IsDisableOutput        = 0  # Flag: disabled stdout
IsPrintExceptions      = 1  # Flag: sets printing of exceptions
IsNoEmail              = 1  # Flag: don't send email

LOCAL_FULL_TIMESTAMP   = '%d-%m-%Y %H:%M:%S'
LOCAL_EXCEL_TIMESTAMP  = '%d.%m.%Y %H:%M:%S'
LOCAL_EASY_TIMESTAMP   = '%d-%m-%Y %H:%M'
LOCAL_EASY_DATESTAMP   = '%Y-%m-%d'
LOCAL_EXPORT_TIMESTAMP = '%Y%m%d%H%M%S'
UTC_FULL_TIMESTAMP     = '%Y-%m-%d %H:%M:%S'
UTC_EASY_TIMESTAMP     = '%Y-%m-%d %H:%M'
DATE_TIMESTAMP         = '%d/%m'
DATE_STAMP             = '%Y%m%d'

DATE_FROM_DELTA        = 0

default_print_encoding = 'cp866'
default_unicode        = 'utf-8'
default_encoding       = 'cp1251'
default_iso            = 'ISO-8859-1'

CONNECTION = {
    'bankperso'    : { 'server':'localhost', 'user':'sa', 'password':'*****', 'database':'BankDB', 'timeout':15 },
    'orderstate'   : { 'server':'localhost', 'user':'sa', 'password':'*****', 'database':'OrderState', 'timeout':15 },
    'configurator' : { 'server':'localhost', 'user':'sa', 'password':'*****', 'database':'BankDB', 'timeout':15  },
    'orderlog'     : { 'server':'localhost', 'user':'sa', 'password':'*****', 'database':'OrderLog', 'timeout':15  },
}

smtphost1 = {
    'host'         : '172.9.9.9', 
    'port'         : 25,
    'connect'      : None,
    'tls'          : 0,
    'method'       : 1,
    'from'         : 'mailrobot@company.ru',
}

smtphost2 = {
    'host'         : 'smtp-mail.outlook.com', 
    'port'         : 587,
    'connect'      : {'login' : "support@company.ru", 'password' : "Rof86788"},
    'tls'          : 1,
    'method'       : 2,
    'from'         : 'support@company.ru',
}

smtphosts = (smtphost1, smtphost2)

email_address_list = {
    'adminbd'      : 'admin_bd@company.ru',     
    'support'      : 'support@company.ru',
    'mailrobot'    : 'mailrobot@company.ru',
}

image_encoding = {
    'default'      : (default_encoding, default_unicode, default_iso,),
}

BP_ROOT = { 
    'default'      : (default_unicode, 'Z:/bankperso/default',), #'//persotest/bankperso'
    'VTB24'        : (default_unicode, 'Z:/bankperso/VTB24',),
    'CITI_BANK'    : (default_unicode, 'Z:/bankperso/CITI',),
}

INFOEXCHANGE_ROOT = {
    'default'      : (default_unicode, 'Z:/#Save/infoexchange',),
}

SDC_ROOT = {
    'default'      : (default_unicode, 'Z:/SDC/default', 'sdc_(.*)_(\d{2}\.\d{2}\.\d{4}).*', 'with_aliases',),
    'VTB24'        : (default_unicode, 'Z:/SDC/VTB24', 'sdc_(.*)_(\d{2}\.\d{2}\.\d{4}).*', ''),
    'CITI_BANK'    : (default_unicode, 'Z:/SDC/CITI', 'sdc_(.*)_(\d{2}\.\d{2}\.\d{4}).*', ''),
}

EXCHANGE_ROOT = {
    'default'      : (default_unicode, 'Z:/exchange/11.21', '(.*)_(\d{2}\.\d{2}\.\d{4}).*', 'with_aliases:jzdo:unique:count',),
    'CITI_BANK'    : (default_unicode, 'Z:/exchange/11.18', '(.*)_(\d{2}\.\d{2}\.\d{4}).*', '*',),
}

MAX_UNRESOLVED_LINES = (9, 99, 3)
COMPLETE_STATUSES = (62, 64, 98, 197, 198, 201, 202, 203, 255,) # 

ansi = not sys.platform.startswith("win")

path_splitter = '/'
n_a = 'n/a'
cr = '\n'

_config = None

def isIterable(v):
    return not isinstance(v, str) and isinstance(v, Iterable)

def normpath(p):
    if p.startswith('//'):
        return '//%s' % re.sub(r'\\', '/', os.path.normpath(p[2:]))
    return re.sub(r'\\', '/', os.path.normpath(p))

def mkdir(name):
    try:
        os.mkdir(name)
    except:
        raise OSError('Error while create a folder')

def checkPathExists(source, filename):
    folders = normpath(filename).split(path_splitter)
    source = normpath(source).lower()

    def _is_exist(p):
        return os.path.exists(p) and os.path.isdir(p)

    disk = folder = ''

    for name in folders:
        if not name or filename.endswith('%s%s' % (path_splitter, name)):
            continue

        if not folder and ':' in name:
            disk = name
            folder = '%s%s' % (disk, path_splitter)
        else:
            folder = os.path.join(folder, name)

        if not normpath(folder).lower() in source:
            return False
        if _is_exist(folder):
            continue

        mkdir(folder)

    return True

##  --------------------------------------- ##

class Config(object):
    
    def __init__(self):
        self._errorlog = ''
    """
    def _get_errorlog(self):
        return self._errorlog or errorlog
    def _set_errorlog(self, value):
        self._errorlog = value
    errorlog = property(_get_errorlog, _set_errorlog)
    """
    @property
    def errorlog(self): return self._errorlog
    @errorlog.setter
    def errorlog(self, value): self._errorlog = value

##  --------------------------------------- ##

def print_to(f, v, mode='ab', request=None, encoding=default_encoding):
    items = not isIterable(v) and [v] or v
    if not f:
        f = getErrorlog()
    fo = open(f, mode=mode)
    def _out(s):
        if not isinstance(s, bytes):
            fo.write(s.encode(encoding, 'ignore'))
        else:
            fo.write(s)
        fo.write(cr.encode())
    for text in items:
        try:
            if IsDeepDebug:
                print(text)
            if request:
                _out('%s>>> %s [%s]' % (cr, datetime.datetime.now().strftime(UTC_FULL_TIMESTAMP), request.url))
            _out(text)
        except Exception as e:
            pass
    fo.close()

def print_exception(stack=None):
    print_to(errorlog, '%s>>> %s:%s' % (cr, datetime.datetime.now().strftime(LOCAL_FULL_TIMESTAMP), cr))
    traceback.print_exc(file=open(errorlog, 'a'))
    if stack is not None:
        print_to(errorlog, '%s>>> Traceback stack:%s' % (cr, cr))
        traceback.print_stack(file=open(errorlog, 'a'))

def setErrorlog(s):
    _config.errorlog = s and normpath(os.path.join(basedir, s))

def getErrorlog():
    return _config.errorlog

errorlog = normpath(os.path.join(basedir, 'traceback.log'))

if _config is None: _config = Config()
