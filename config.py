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
    'bankperso'    : { 'server':'localhost', 'user':'sa', 'password':'***', 'database':'BankDB',     'timeout':15 },
    'cards'        : { 'server':'localhost', 'user':'sa', 'password':'***', 'database':'Cards',      'timeout':15 },
    'orderstate'   : { 'server':'localhost', 'user':'sa', 'password':'***', 'database':'OrderState', 'timeout':15 },
    'preload'      : { 'server':'localhost', 'user':'sa', 'password':'***', 'database':'BankDB',     'timeout':15 },
    'configurator' : { 'server':'localhost', 'user':'sa', 'password':'***', 'database':'BankDB',     'timeout':15 },
    'orderlog'     : { 'server':'localhost', 'user':'sa', 'password':'***', 'database':'OrderLog',   'timeout':15 },
}

smtphost = {
    'host' : 'mail2.company.ru', 
    'port' : 25
}

email_address_list = {
    'adminbd'      : 'admin@company.ru',     
    'support'      : 'support@company.ru',
    'warehouse'    : 'user@company.ru',
}

image_encoding = {
    'default'      : (default_encoding, default_unicode, default_iso,),
    'CITI_BANK'    : (default_print_encoding, default_encoding,),
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
    'default'      : (default_unicode, 'Z:/exchange/11.01', '(.*)_(\d{2}\.\d{2}\.\d{4}).*', 'with_aliases:jzdo:unique:count',),
    'CITI_BANK'    : (default_unicode, 'Z:/exchange/11.02', '(.*)_(\d{2}\.\d{2}\.\d{4}).*', '*',),
}

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

def print_to(f, v, mode='ab', request=None):
    if not f:
        f = getErrorlog()
    if not checkPathExists(basedir, f):
        return
    items = not isIterable(v) and [v] or v
    fo = open(f, mode=mode)
    def _out(s):
        fo.write(s.encode(default_encoding, 'ignore'))
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

def print_exception():
    print_to(errorlog, '>>> %s:%s' % (datetime.datetime.now().strftime(LOCAL_FULL_TIMESTAMP), cr))
    traceback.print_exc(file=open(errorlog, 'a'))

def setErrorlog(s):
    _config.errorlog = s and normpath(os.path.join(basedir, s))

def getErrorlog():
    return _config.errorlog

errorlog = normpath(os.path.join(basedir, 'traceback.log'))

if _config is None: _config = Config()
