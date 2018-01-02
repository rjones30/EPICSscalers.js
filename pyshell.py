#!/usr/bin/python
import sys
import re

def iterate(expr, index0, index1):
    f = eval(expr)
    dim = len(index0)
    names = {'none': 0}
    index = [index0[i] for i in range(0, dim)]
    nameTable = tabulate(f, dim-1, index, index0, index1, names)
    json = '{"names":['
    for key, value in sorted(names.iteritems(), key=lambda (k,v): (v,k)):
        if value > 1:
            json += ',"' + key + '"'
        elif value == 1:
            json += '"' + key + '"'
    json += '],"nameTable":' + str(nameTable) + '}'
    json += '\n'
    return json

def tabulate(f, dim, index, index0, index1, names):
    newrow = [0] * index1[dim]
    if dim > 0:
        for i in range (index0[dim], index1[dim]):
            index[dim] = i
            newrow[i] = tabulate(f, dim-1, index, index0, index1, names)
    else:
        for i in range (index0[0], index1[0]):
            index[0] = i
            name = f(*index)
            if not name in names:
                names[name] = len(names)
            newrow[i] = names[name]
    return newrow

while True:
    req = raw_input()
    open("pyshell.log", "a").write(req + '\n')
    try:
        if re.search(r"__", req):
            raise ValueError("Unsafe request detected, rejecting request!")
        else:
            print eval(req, {__builtins__:None}, {'iterate': iterate})
    except:
        print "PROGRAM ERROR"
    sys.stdout.flush()
    model = False
