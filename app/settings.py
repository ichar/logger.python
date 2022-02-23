# -*- coding: utf-8 -*-

import re
from datetime import datetime

product_version = '1.75, 2020-12-11 with cp1251 (Python3)'

#########################################################################################

#   -------------
#   Default types
#   -------------

DEFAULT_LANGUAGE = 'ru'
DEFAULT_LOG_MODE = 1
DEFAULT_PER_PAGE = 10
DEFAULT_ADMIN_PER_PAGE = 20
DEFAULT_PAGE = 1
DEFAULT_UNDEFINED = '---'
DEFAULT_DATE_FORMAT = ('%d/%m/%Y', '%Y-%m-%d',)
DEFAULT_DATETIME_FORMAT = '<nobr>%Y-%m-%d</nobr><br><nobr>%H:%M:%S</nobr>'
DEFAULT_DATETIME_INLINE_FORMAT = '<nobr>%Y-%m-%d&nbsp;%H:%M:%S</nobr>'
DEFAULT_DATETIME_PERSOLOG_FORMAT = ('%Y%m%d', '%Y-%m-%d %H:%M:%S',)
DEFAULT_DATETIME_SDCLOG_FORMAT = ('%d.%m.%Y', '%d.%m.%Y %H:%M:%S,%f',)
DEFAULT_DATETIME_EXCHANGELOG_FORMAT = ('%d.%m.%Y', '%d.%m.%Y %H:%M:%S,%f',)
DEFAULT_HTML_SPLITTER = ':'

USE_FULL_MENU = True

MAX_TITLE_WORD_LEN = 50
MAX_XML_BODY_LEN = 1024*100
MAX_XML_TREE_NODES = 100
MAX_LOGS_LEN = 999
MAX_CARDHOLDER_ITEMS = 9999
EMPTY_VALUE = '...'
