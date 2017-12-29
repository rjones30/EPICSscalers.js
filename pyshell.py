#!/usr/bin/python
import sys

while True:
    req = raw_input()
    open("pyshell.log", "a").write(req + '\n')
    try:
        print eval(req, {__builtins__:None}, {})
    except:
       print "PROGRAM ERROR"
    sys.stdout.flush()
    
