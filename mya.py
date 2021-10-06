#!/usr/bin/env python3
#
# mya.py - python module to facilitate lookup and fetching
#          of EPICS variable data from the MYA archiver,
#          using a proxy server to bypass the acc firewall.
#
# author: richard.t.jones at uconn.edu
# version: november 28, 2018
#
# sample usage:
#   import mya
#   descr = mya.lookup("IBCAD00CRCUR6")
#   value, time = mya.fetch(descr, 425325135239577600, 22200)
#   for i in range(0, len(value)):
#      print(mya.time_epics_to_string(time[i]), value[i])

import math
import datetime
import pytz
import MySQLdb
import array
import ROOT
import re
import bisect

dbname = "archive"
dbproxy = "gluey.phys.uconn.edu"

port = {'opsmya0': 63306,
        'opsmya1': 63307,
        'opsmya2': 63308,
        'opsmya3': 63309,
        'opsmya4': 63310,
        'opsmya5': 63311,
        'opsmya6': 63312,
        'opsmya7': 63313,
        'opsmya8': 63314,
        'opsmya9': 63315,
        'opsmya10': 63316,
        'opsmya11': 63317,
        'opsmya12': 63318,
        'hstmya1': 63319,
       }

db = {}

tzlocal = pytz.timezone("America/New_York")

opsmya_start = 0x5a8e08000000000
epics_second = (1 << 28)
epics_cache = {'mya descriptors': {}}

def lookup(varname, deployment="ops"):
   """
   Fetches the following information from the EPICS archive directory
   and returns it in a hash with the following keys defined.
      chan_id (unique int) - data are contained in table_<chan_id>
      name (string) - equals varname
      type (string=1, short=2, float=3, enum=4, char=5, double=6)
      adel (float or None)
      size (array length, or 1 for scalar)
      clip (NULL as far as I can tell)
      active (0 if discontinued, 1 otherwise) 
      request (1, use unknown)
      alert (flag when out of bounds, criteria unknown)
      host (string) - hostname of mysql server hosting table_<chan_id>
      backup (long int) - timestamp
   """
   if varname in epics_cache['mya descriptors']:
      return epics_cache['mya descriptors'][varname]
   if deployment == "ops":
      host = "opsmya0"
   elif deployment == "history":
      host = "hstmya1"
   else:
      print("mya.lookup warning: unknown archive deployment {0},"
            "cannot continue!".format(deployment))
      return {}
   if not host in db:
      connect(host)
   cur = db[host].cursor()
   cur.execute("select * from channels where name = '{0}'".format(varname))
   heads = cur.description
   descr = {}
   for row in cur.fetchall():
      for i in range(0, len(heads)):
         descr[heads[i][0]] = row[i]
   epics_cache['mya descriptors'][varname] = descr
   return descr

def fetch(descr, t0, dt=0, cond=None):
   """
   Fetch data for the EPICS variable described in descr starting at
   time t0 (EPICS time) and going forward dt seconds. The values are
   returned in 2 arrays, the first being the value and the second being
   the time that value was recorded. If dt is not specified then it defaults
   to zero, and just one value is returned, together with its time. Note
   that the returned time will be less than or equal to the requested t0.
   A query condition may be provided as a logical expression in input
   variable cond, in which case only time periods that satisfy the
   logical condition are included in the output.
   """
   if isinstance(t0, str):
      t0 = time_string_to_epics(t0)
   t1 = int(t0 + dt * epics_second)
   archive = "ops"
   if isinstance(descr, dict):
      host = descr['host']
   else:
      if t0 < opsmya_start:
         ops = "history"
         descriptor = lookup(descr, deployment="history")
      else:
         ops = "ops"
         descriptor = lookup(descr)
      if 'host' in descriptor:
         descr = descriptor
         host = descr['host']
      else:
         print("mya.fetch warning: epics variable {0} not found".format(descr))
         return [], []
   key = descr['name']
   if key in epics_cache:
      if t0 >= epics_cache[key][1][0] and t1 <= epics_cache[key][1][-1]:
         i0 = bisect.bisect_left(epics_cache[key][1], t0)
         i1 = bisect.bisect_right(epics_cache[key][1], t1)
         values = [v for v in epics_cache[key][0][i0:i1]]
         times = [t for t in epics_cache[key][1][i0:i1]]
         if len(times) == 0 or times[0] > t0:
            values.insert(0, epics_cache[key][0][i0-1])
            times.insert(0, t0)
         times[-1] = t1
         return values, times
   print("mya.fetch info: mya cache miss on", key)
   if archive != "ops":
      print("mya.fetch warning: going back into {0} archive,".format(archive),
               "this may take some time...")
   table = "table_{0}".format(descr['chan_id'])
   cur = db[host].cursor()
   cur.execute("select time from {0} where time < {1}".format(table, t0) +
               " order by time desc limit 1")
   row = cur.fetchone()
   if not row:
      print("mya.fetch warning: no entries found in archive for {0}"
            "on or before {1}".format(descr['name'], time_epics_to_string(t0)))
      return [], []
   tini = row[0] - 1
   tfin = row[0] + (dt * epics_second) + 1
   cur.execute("select time,val1 from {0}".format(table) +
               " where time > {0} and time < {1}".format(tini, tfin))
   times = []
   values = []
   for row in cur.fetchall():
      times.append(int(row[0]))
      values.append(float(row[1]))
   epics_cache[key] = values, times
   print("mya.fetch info: {0} archive lookup returns {1} entries"
         .format(archive, len(times)))
   return filter_cond(values, times, cond)

def plot(descr, t0, dt=0, cond=None):
   """
   Same as fetch, but return the results as a TGraph of values vs time.
   A query condition may be provided as a logical expression in input
   variable cond, in which case only time periods that satisfy the
   logical condition are included in the plot.
   """
   values, times = fetch(descr, t0, dt, cond)
   if len(times) == 0:
      print("mya.fetch warning: no data found for {0} during the requested"
            "run period".format(descr))
      return 0
   times = [(t - times[0]) / epics_second for t in times]
   graph = ROOT.TGraph(len(times), array.array('d', times),
                                   array.array('d', values))
   try:
      graph.SetTitle(descr['name'])
   except:
      graph.SetTitle(descr)
   graph.GetXaxis().SetTitle("epoch time/s")
   graph.Draw('AL')
   c1 = ROOT.gROOT.FindObject("c1")
   c1.Update()
   return graph

def filter_cond(values, times, cond):
   """
   Filter the time series in values,times by the logical condition in cond.
   The filter removes unwanted elements from the time series and sometimes
   moves the time value forward, but never inserts new elements.
   """
   if not cond or len(times) == 0:
      return values, times
   fvalues = []
   ftimes = []
   i = 0
   for r in search_ranges(cond, times[0], times[-1]):
      while i < len(times) and times[i] < r[0]:
         i += 1
      while i < len(times) and times[i] <= r[1]:
         fvalues.append(values[i])
         ftimes.append(times[i])
         i += 1
      if i == len(times):
         break
   return fvalues, ftimes

def search_ranges(query, t0, t1):
   """
   Searches the archive during the time interval between t0 and t1 for
   time ranges that satisfy the logical condition contained in the
   query string. Query is a logical expression consisting of any
   combination of arithmetic and logical operators between epics
   variables and constant expressions. The following operators are
   supported, where e1 and e2 are any two epics variables or valid
   arithmetic expressions, and l1 and l2 are any two valid logical
   expressions.
     e3 = e1 + e2 : arithmetic addition
     e3 = e1 - e2 : arithmetic subtraction
     e3 = e1 * e2 : arithmetic multiplication
     e3 = e1 / e2 : arithmetic division
     e3 = e1 // e2 : arithmetic division, integer part
     e3 = e1 % e2 : arithmetic division, remainder
     e3 = e1 ** e2 : arithmetic power
     e2 = -e1 : arithmetic negation
     e2 = +e1 : copies e1 to e2
     l3 = e1 > e2 : arithmetic greater
     l3 = e1 >= e2 : arithmetic greater or equal
     l3 = e1 == e2 : arithmetic equal
     l3 = e1 != e2 : arithmetic not equal
     l3 = e1 < e2 : arithmetic lesser
     l3 = e1 <= e2 : arithmetic lesser or equal
     l3 = l1 && l2 : logical and
     l3 = l1 || l2 : logical or
     l2 = !l1 : logical not (parentheses required)
   Parentheses are supported at all levels to change the default
   precedence (python language standard) of operations. The result
   of the query must be a logical value, or the query is invalid.
   The order of variable lookups to the archive proceeds left-to-
   right across the expression, so putting the most restrictive
   or coarse-grained condition variables first will greatly
   increase the speed of the algorithm.
   """
   if isinstance(t0, str):
      t0 = time_string_to_epics(t0)
   if isinstance(t1, str):
      t1 = time_string_to_epics(t1)
   if re.match(r"[+\-\.0-9eE]+$", query):
      yield (t0, t1, float(query))
      return
   pquery = parenthesise(query)
   if not pquery:
      return
   elif pquery[0] == ' ':
      yield from search_ranges(pquery[1:], t0, t1)
   elif pquery[0] == '!':
      for r in search_ranges(pquery[1:], t0, t1):
         yield (r[0], r[1], not r[2])
      return
   elif pquery[0] == '-':
      for r in search_ranges(pquery[1:], t0, t1):
         yield (r[0], r[1], -r[2])
      return
   elif pquery[0] == '+':
      for r in search_ranges(pquery[1:], t0, t1):
         yield (r[0], r[1], +r[2])
      return
   elif pquery[0] == '(':
      level = 1
      for i in range(1, len(pquery)):
         if pquery[i] == '(':
            level += 1
         elif pquery[i] == ')':
            level -= 1
         if level == 0:
            subj_ranges = [r for r in search_ranges(pquery[1:i], t0, t1)]
            break
      if level != 0:
         print("mya.search_ranges error: unbalanced parentheses,"
               "cannot parse query!")
         return
      expr = re.match(r" *([<>=!\+\-\*/\&\|]+) *(.+)", pquery[i+1:])
      if expr:
         verb = expr.group(1)
         pred = expr.group(2)
      elif subj:
         subj = subj.group(1)
         verb = None
         pred = None
      else:
         print("mya.search_ranges error: bad query:", query)
         return
   else:
      subj = re.match(r"([.A-Za-z:_0-9]+)", pquery)
      expr = re.match(r"([.A-Za-z:_0-9]+) *([<>=!\+\-\*/\&\|]+) *(.+)", pquery)
      if expr:
         subj = expr.group(1)
         verb = expr.group(2)
         pred = expr.group(3)
      elif subj:
         subj = subj.group(1)
         verb = None
         pred = None
      else:
         print("mya.search_ranges error: bad query:", query)
         return
      subj_ranges = []
      values, times = fetch(subj, t0, (t1 - t0) / epics_second)
      for i in range(0, len(times) - 1):
         subj_ranges.append((times[i], times[i+1], values[i]))
      if len(times) > 0 and times[-1] < t1:
         subj_ranges.append((times[-1], t1, values[-1]))
   if len(subj_ranges) == 0:
      return
   elif verb == None:
      for r in subj_ranges:
         yield (r[0], r[1], r[2])
      return
   count = 0
   for r1 in subj_ranges:
      count += 1
      for r2 in search_ranges(pred, r1[0], r1[1]):
         if verb == "+":
            value = r1[2] + r2[2]
         elif verb == "-":
            value = r1[2] - r2[2]
         elif verb == "*":
            value = r1[2] * r2[2]
         elif verb == "/":
            value = r1[2] / r2[2]
         elif verb == "//":
            value = r1[2] // r2[2]
         elif verb == "%":
            value = r1[2] % r2[2]
         elif verb == "**":
            value = r1[2] ** r2[2]
         elif verb == ">":
            value = r1[2] > r2[2]
         elif verb == ">=":
            value = r1[2] >= r2[2]
         elif verb == "!=":
            value = r1[2] != r2[2]
         elif verb == "<":
            value = r1[2] < r2[2]
         elif verb == "<=":
            value = r1[2] <= r2[2]
         elif verb == "&&":
            value = r1[2] and r2[2]
         elif verb == "||":
            value = r1[2] or r2[2]
         else:
            print("mya.search_ranges error: unsupported operator!")
            return
         yield (r2[0], r2[1], value)

def find_ranges(query, t0, t1):
   """
   Runs search_ranges(query, t0, t1) and then compresses the output
   to merge successive intervals with unchanged values, and eliminate
   intervals where the query condition evaluates to False. For details
   regarding the query syntax, see function search_ranges().
   """
   rsaved = 0
   for r in search_ranges(query, t0, t1):
      if r[2] == False:
         continue
      elif rsaved and r[0] == rsaved[1]:
         rsaved[1] = r[1]
      elif rsaved:
         yield (rsaved[0], rsaved[1], rsaved[2])
      rsaved = [r[0], r[1], r[2]]
   if rsaved:
      yield (rsaved[0], rsaved[1], rsaved[2])

def parenthesise(query):
   """
   Parse an algebraic expression and add parentheses to express the
   correct operator precedence for python operations, in order from
   highest to lowest precedence. Multiple operations with the same
   precedence are processed left-to-right.
     1. f(args...)           : call to function f()
     2. +, -                 : unary +, -
     3. **                   : exponentiation
     4. *, /, //, %          : multiplication and division
     5. +, -                 : addition and subtraction
     6 ==, !=, <, <=, >, >=  : comparison operators
     7. &&                   : logical and
     8. ||                   : logical or
   Support for user-callable functions (precedence 1) is included
   here for future use, although the query expression parser does
   not currently recognize any user functions.
   """
   bioperators = {'**': 6, '*': 5, '/': 5, '//':5, '%':5,
                  '+': 4, '-': 4, '==': 3, '!=': 3, '<': 3,
                  '<=': 3, '>': 3, '>=': 3, '&&': 2, 
                  '||': 1, ',': 0}
   pquery = ["",]
   preced = [0, ]
   i = -1
   while i+1 < len(query):
      i += 1
      if query[i] == ' ':
         continue
      elif query[i] == '(':
         i0 = i+1
         level = 1
         while i+1 < len(query):
            i += 1
            if query[i] == '(':
               level += 1
            elif query[i] == ')':
               level -= 1
            if level == 0:
               pquery[-1] += '(' + parenthesise(query[i0:i]) + ')'
               break
         if level != 0:
            print("mya.parenthesise error: parenthesis mismatch,"
                  "cannot parse query!")
            return ""
         continue
      expr = re.match(r"([+\-!]*[.A-Za-z:_0-9]+) *([+\-%/\*\&|=!<>,]+)", query[i:])
      evar = re.match(r"([+\-!]*[.A-Za-z:_0-9]+) *$", query[i:])
      efun = re.match(r"[A-Za-z][.A-Za-z:_0-9]*\(", query[i:])
      if efun:
         va = '(' + efun.group(0)
         i += len(efun.group(0))
         i0 = i
         level = 1
         while i+1 < len(query):
            i += 1
            if query[i] == '(':
               level += 1
            elif query[i] == ')':
               level -= 1
            if level == 0:
               va += parenthesise(query[i0:i]) + '))'
               break
         if level != 0:
            print("mya.parenthesise error: parenthesis mismatch,"
                  "cannot parse query!")
            return ""
         expr = re.match(r" *([+\-%/\*\&|=!<>,]*)", query[i+1:])
         op = expr.group(1)
         i += len(op)
      elif expr:
         va = expr.group(1)
         op = expr.group(2)
      elif evar:
         va = evar.group(1)
         op = ""
      if va and op:
         if op in bioperators:
            while bioperators[op] < preced[-1]:
               preced.pop()
               pexpr = pquery.pop()
               va = '(' + pexpr + va + ')'
            if bioperators[op] > preced[-1]:
               preced.append(bioperators[op])
               pquery.append(va + op)
            elif bioperators[op] == preced[-1]:
               if preced[-1] > 0:
                  pquery[-1] = '(' + pquery[-1] + va + ')' + op
               else:
                  pquery[-1] += va + op
         else:
            print("mya.parenthesise error: operator {0} not supported,",
                  "cannot parse query!".format(expr.group(2)))
            return ""
         i += len(expr.group(0)) - 1
         continue
      elif va:
         pquery[-1] += va
         break
      print("mya.parenthesise error: query parsing error #2,",
            "cannot continue!")
      return
   while len(pquery) > 1:
      preced.pop()
      pexpr = pquery.pop()
      pquery[-1] = '(' + pquery[-1] + '(' + pexpr + '))'
   while len(pquery[0]) > 0 and pquery[0][0] == '(':
      level = 1
      for i in range(1, len(pquery[0])):
         if pquery[0][i] == '(':
            level += 1
         elif pquery[0][i] == ')':
            level -= 1
         if level == 0:
            if i+1 == len(pquery[0]):
               pquery[0] = pquery[0][1:-1]
            else:
               level = -1
               break
      if level < 0:
         break
   return pquery[0]

def connect(host):
   """
   Open a connection to mysql server host.
   """
   dbuser = "myapi"
   dbpasswd = "MYA"
   db[host] = MySQLdb.connect(host=dbproxy, port=port[host],
                              user=dbuser, passwd=dbpasswd,
                              db=dbname)

def time_string_to_epics(datetime_string, gmt=False):
   """
   Converts a date+time string in format yyyy-mm-dd HH:MM[:SS[+0.X]]
   into an EPICS timestamp value (long int).
   """
   tsec = datetime.datetime.strptime(datetime_string, "%Y-%m-%d %H:%M:%S")
   epoch = datetime.datetime(1970,1,1)
   epoch = pytz.utc.localize(epoch, is_dst=None)
   if gmt:
      tsec = pytz.utc.localize(tsec, is_dst=None)
   else:
      tsec_local = tzlocal.localize(tsec, is_dst=None)
      tsec = tsec_local.astimezone(pytz.utc)
   plus = datetime_string.find("+") + 1
   tfrac = 0
   if plus:
      tfrac = float(datetime_string[plus:])
   tepoch = tsec - epoch
   tepoch = tepoch.days * 24 * 3600 + tepoch.seconds
   return int(math.floor((tepoch + tfrac) * epics_second))

def time_epics_to_string(epics_time, fraction=0, gmt=False):
   """
   Converts an EPICS timestamp value (long int) into a string in the
   format "yyyy-mm-dd HH:MM:SS", or "yyy-mm-dd HH:MM:SS+0.X" if fraction
   argument is non-zero, where X is fraction of a second.
   """
   tepoch = epics_time // epics_second
   tfrac = (epics_time % epics_second) / float(epics_second)
   epoch = datetime.datetime(1970,1,1)
   ttime = epoch + datetime.timedelta(seconds=tepoch)
   epoch = pytz.utc.localize(epoch, is_dst=None)
   ttime_utc = pytz.utc.localize(ttime, is_dst=None)
   if gmt:
      ttime = ttime_utc
   else:
      ttime = ttime_utc.astimezone(tzlocal)
   tstring = ttime.strftime("%Y-%m-%d %H:%M:%S")
   if fraction:
      tstring += '+' + str(tfrac)
   return tstring
