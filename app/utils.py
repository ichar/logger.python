# -*- coding: utf-8 -*-

import os
import datetime
from datetime import timedelta
import time
import re
from collections import Iterable
import io
import xlwt
from sortedcontainers import SortedDict
import base64
import codecs
import zipfile

from config import (
     IsDebug, IsDeepDebug, default_print_encoding, default_unicode, default_encoding, default_iso, cr,
     LOCAL_FULL_TIMESTAMP, LOCAL_EASY_TIMESTAMP, UTC_EASY_TIMESTAMP, DATE_STAMP,
     print_to, print_exception
     )

from .settings import DEFAULT_DATETIME_FORMAT, EMPTY_VALUE, DEFAULT_HTML_SPLITTER, MAX_TITLE_WORD_LEN

empty_value = '...'

def getBOM(encoding):
    return codecs.BOM_UTF8.decode(encoding)

def _imports():
    for name, val in globals().items():
        if isinstance(val, types.ModuleType):
            yield val.__name__

def show_imported_modules():
    return [x for x in _imports()]

def os_exec(command, message):
    try:
        code = os.system(command)
        if code:
            raise Exception('%s, command: %s, code: %s' % (message, command, code))
    except:
        raise

def mkdir(name):
    os_exec(os.name == 'posix' and 
        'mkdir "'+name+'"' or 
        'mkdir "'+name+'"', 
        'Error while create a folder: %s' % name)

create_folder = mkdir

def rmdir(name):
    os_exec(os.name == 'posix' and 
        'rm -rf "'+name+'"' or 
        'rmdir /s/q "'+name+'"', 
        'Error while remove a folder: %s' % name)

remove_folder = rmdir

def del_file(name, check=True):
    if check and not os.path.exists(name):
        return
    os_exec(os.name == 'posix' and 
        'rm `%s`' % name or 
        'del /f "%s"' % os.path.normpath(name), 
        'Error while remove a file: %s' % name)

remove_file = del_file

def mv_file(path_from, path_to):
    os_exec(os.name == 'posix' and 
        'mv '+path_from+' '+path_to or 
        'move /Y "'+os.path.normpath(path_from)+'" "'+os.path.normpath(path_to)+'"', 
        'Error while moving a file from: %s to: %s' % (path_from, path_to))

move_file = mv_file

def cp_file(path_from, path_to):
    os_exec(os.name == 'posix' and 
        'cp '+path_from+' '+ path_to or 
        'copy /Y /V "'+os.path.normpath(path_from)+'" "'+os.path.normpath(path_to)+'"', 
        'Error while copying a file from: %s to: %s' % (path_from, path_to))

copy_file = cp_file

def normpath(p, share=None):
    if p.startswith('//'):
        return (not share and '//%s' or r'\\%s') % re.sub(r'\\', '/', os.path.normpath(p[2:]))
    return re.sub(r'\\', '/', os.path.normpath(p))

def check_folder_exists(destination, root):
    folders = destination.replace(root, '').split('/')
    folder = root
    while len(folders) > 0:
        name = folders.pop(0)
        if not name:
            continue
        folder = normpath(os.path.join(folder, name), 1)
        if not (os.path.exists(folder) and os.path.isdir(folder)):
            mkdir(folder)

def unzip(src, destination=None, is_relative=False):
    if not os.path.exists(src):
        return
    if not destination:
        destination = os.path.split(src)[0]
    elif is_relative:
        destination = os.path.join(os.path.split(src)[0], destination)
    x = zipfile.ZipFile(src, 'r')
    x.extractall(destination)
    x.close()

def getToday():
    return datetime.datetime.now()

def getTime(format=UTC_EASY_TIMESTAMP):
    return getToday().strftime(format)

def getDate(value, format=DEFAULT_DATETIME_FORMAT, is_date=False):
    if is_date:
        try:
            return value and datetime.datetime.strptime(value, format) or None
        except:
            return None
    else:
        return value and value.strftime(format) or ''

def getDateOnly(value, format=DATE_STAMP):
    return datetime.datetime.strptime(value.strftime(format), format)

def getId(id):
    return DEFAULT_HTML_SPLITTER in id and id.split(DEFAULT_HTML_SPLITTER)[1] or id

def getMaskedPAN(value):
    return value and re.sub(r'(\d{4})\d{8}(\d{4})', r'\1********\2', re.sub(r'(\d)\s(\d)', r'\1\2', value)) or ''

def checkDate(value, format=DEFAULT_DATETIME_FORMAT):
    try:
        v = datetime.datetime.strptime(value, format)
        if not (v and v.year > 2010):
            v = None
    except:
        v = None
    return v and True or False

def spent_time(start, finish=None):
    if not finish:
        finish = datetime.datetime.now()
    t = finish - start
    return (t.seconds*1000000+t.microseconds)/1000000

def out(x):
    return x.encode(default_print_encoding, 'ignore')

def cid(ob):
    return ob and ob.id is not None and str(ob.id) or EMPTY_VALUE

def cdate(date, fmt=LOCAL_FULL_TIMESTAMP):
    if date:
        return str(date.strftime(fmt))
    return empty_value

def clean(value):
    return value and re.sub(r'[\n\'\"\;\%\*]', '', value).strip() or ''

def cleanHtml(value):
    return value and re.sub(r'\s+', ' ', re.sub(r'<.*?>', '', value)) or ''

def xsplit(value, keys):
    out = [value]
    for s in keys:
        l = range(len(out))
        for n in l:
            v = out.pop(0)
            out += [x.strip() for x in v.split(s)]
    return list(set(out))

def usplitter(values, keys):
    items = []
    for x in values:
        items += xsplit(x, keys)
    return list(set(items))

def splitter(value, length=None, comma=','):
    if value and len(value) > (length or MAX_TITLE_WORD_LEN):
        changed, v = worder(value, length=length, comma=comma in value and comma)
        return v
    return value

def worder(value, length=None, comma=None):
    max_len = (not length or length < 0) and MAX_TITLE_WORD_LEN or length
    words = value.split()
    s = ''
    changed = 0
    while len(words):
        word = words.pop(0).strip()
        if s:
            s += ' '
        if len(word) <= max_len:
            s += word
        else:
            w = word[max_len:]
            if comma and comma in w:
                words = ['%s%s%s' % (x.strip(), comma, ' ') for x in w.split(comma)] + words
            else:
                words.insert(0, w)
            s += word[:max_len]
            changed = 1
    s = s.strip()
    if comma and s.endswith(comma):
        s = s[:-1]
    return changed, s

default_indent = ' '*2

def indentXMLTree(node, level=1, is_edge=False, count=0, limit=1000):
    if node is None:
        return
    if limit and count > limit:
        return

    new_line = cr + default_indent * level
    tail = cr + default_indent * (level-1)
    edge = cr + default_indent * (level-2)

    node.tail = tail
    l = len(node)

    if l:
        if not (node.text and node.text.strip()):
            node.text = new_line
        for n, ob in enumerate(list(node)):
            indentXMLTree(ob, level+1, n+1==l and True or False, count=count+1, limit=limit)
        node.tail = tail
    if is_edge:
        node.tail = edge

def isIterable(v):
    return not isinstance(v, str) and not isinstance(v, dict) and isinstance(v, Iterable)

def makeCSVContent(rows, title, IsHeaders, **kw):
    encoding = kw.get('encoding') or default_encoding
    output = io.BytesIO()
    
    crlf = '\r\n'
    eol = crlf.encode()

    for i, row in enumerate(rows):
        line = b''
        for j, column in enumerate(row):
            line += str(column).encode(encoding) + b';'
        output.write(line+eol)

    output.seek(0)

    return output.read()

def makeXLSContent(rows, title, IsHeaders, **kw):
    output = io.BytesIO()
    wb = xlwt.Workbook()

    font0 = xlwt.Font()
    font0.name = 'Verdana'
    font0.height = 0x00C8  # Size: 0x00A0-8 0x00B4-9 0x00C8-10
    font0.bold = True

    style0 = xlwt.XFStyle()  # Headers style
    style0.font = font0

    font1 = xlwt.Font()
    font1.name = 'Verdana'
    font1.bold = False

    style1 = xlwt.XFStyle()  # Cells style (default)
    style1.font = font1

    alignment = xlwt.Alignment()
    alignment.horz = xlwt.Formatting.Alignment.HORZ_RIGHT

    style2 = xlwt.XFStyle()  # Cells style (alignment right)
    style2.font = font1
    style2.alignment = alignment

    ws = wb.add_sheet(title[:31])
    for i, row in enumerate(rows):
        for j, column in enumerate(row):
            if i == 0:
                style = style0
            elif isinstance(column, str) and '[b]' in column:
                column = column.replace('[b]', '')
                style = style0
            else:
                style = style1
            ws.write(i, j, column, style)

    wb.save(output)
    output.seek(0)

    return output.read()

def getParamsByKeys(params, keys):
    output = {}
    for param in params:
        if param['PName'] in keys:
            i = keys.index(param['PName'])
            key = keys.pop(i)
            output[key] = param
    return output

def getWhereFilter(filter, key, with_and=True):
    return '%s' % (
        filter and ("%s%s like '%%%s%%'" % (with_and and ' and ' or '', key, filter)) or '')

def makeIDList(ids):
    return ','.join([str(x) for x in sorted(ids)])

def checkPaginationRange(n, page, pages):
    return n < 3 or (n > page-3 and n < page+3) or n > pages-1

def decoder(data, encodings, info='', is_trace=False, limit=None, level=0, **kw):
    image = ''
    encoding = None

    if data is None and level == 0 and 'source' in kw:
        with open(kw['source'], 'rb') as fi:
            data = fi.read()

    if data is not None and encodings:
        n = -1
        is_error = False

        while len(data) > 0:
            if limit and len(image) > limit:
                break

            n = n < len(encodings) - 1 and n + 1 or 0
            encoding = encodings[n]
            errors = encoding == default_iso and 'surrogateescape' or 'strict'

            try:
                image += data.decode(encoding, errors=errors)
                break
            except UnicodeError as ex:
                if is_trace:
                    print_to(None, '>>> decoder error:%s encoding:%s [%s:%s] reason:%s [%s]' % (
                        info, encoding, ex.start, ex.end, ex.reason, 
                        data[ex.start and ex.start-1 or 0:ex.end]), 
                        )
                if is_error and n == len(encodings) - 1:
                    break

                i, e = decoder(data[ex.start:ex.end], encodings[n+1:], info=info, is_trace=is_trace, limit=limit, level=level+1)
                if i:
                    image += data[:ex.start].decode(encoding)
                    image += i
                    data = data[ex.end:]
                    n -= 1
                else:
                    is_error = True
                    p = 'continuation' in ex.reason and ex.start > 0 and ex.start - 1 or ex.start
                    image += data[:p].decode(encoding)
                    data = data[p:]

        if is_trace:
            print_to(None, '>>> decoder done %serrors: [%s]' % (is_error and 'with ' or 'without ', encoding))

    if level == 0:
        data = None
        del data

    if is_trace:
        print_to(None, '>>> decoder image:%s' % len(image))

    return image, encoding

def pickupKeyInLine(line, key, span=''):
    if not (line and key):
        return line

    if span is None:
        pass
    elif not span:
        span = '<span class="log-key">%s</span>' % key

    return re.sub(r'%s(?si)' % re.sub(r'([$])', r'\\\1', key), span, line)

def monthdelta(date, delta):
    m, y = (date.month+delta) % 12, date.year + ((date.month)+delta-1) // 12
    if not m: m = 12
    d = min(date.day, [31,29 if y%4==0 and not y%400==0 else 28,31,30,31,30,31,31,30,31,30,31][m-1])
    return date.replace(day=d,month=m, year=y)

def daydelta(date, delta):
    return delta and date + timedelta(days=delta) or date

def hourdelta(date, delta):
    return delta and date + timedelta(hours=delta) or date

def minutedelta(date, delta):
    return delta and date + timedelta(minutes=delta) or date

def Capitalize(s):
    return (s and len(s) > 1 and s[0].upper() + s[1:].lower()) or (len(s) == 1 and s.upper()) or ''

def unCapitalize(s):
    return (s and len(s) > 1 and s[0].lower() + s[1:]) or (len(s) == 1 and s.lower()) or ''

def sortedDict(dic=None):
    return SortedDict(dic)

def reprSortedDict(dic, is_sort=False):
    items = []
    for key in (is_sort and sorted(dic) or dic):
        if dic[key]:
            items.append('%s=%s' % (key, dic[key]))
    return '{%s}' % ','.join(items)

def makeSqlWhere(query):
    """
        Makes SQL Query `WHERE` string.
        
        Attributes:
            query   -- Dict, {'name' : 'value', ...}
        
        Returns:
            where   -- String, such as: LinkID=10 AND CName='XXX' ...
    """
    where = ['%s=%s' % (name, isinstance(value, str) and ('\'%s\'' % value) or str(value)) for name, value in query.items() if value]
    return ' AND '.join(where)

def sint(value):
    if not value:
        return None
    if isinstance(value, int):
        return value
    return value and value.isdigit() and int(value)

def image_base64(src, image_type):
    with open(src, 'rb') as fi:
        encoded = base64.b64encode(fi.read())
    return 'data:image/%s;base64,%s' % (image_type, encoded.decode())

def rfind(s, sub, start=0):
    for n in range(-1, -len(s)-1, -1):
        x = s[start + n]
        if x == sub:
            return n
    return 0
