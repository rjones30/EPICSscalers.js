# EPICSscalers.js - web client/server for display and animation of EPICS scalers

## Author

* Richard Jones, University of Connecticut, Storrs, CT

## Description

EPICSscalers.js a general client/server application based on 
angular.js and node.js that provides a variety of graphical views
of EPICS variables with animation in a modern web browser. It
is written for the GlueX experiment, but the basic functionality
would work in any EPICS environment.

The scalers for the GlueX experiment are stored in the EPICS archiver
operated by the Jefferson Lab accelerator group. This information is
housed in mysql servers located behind a double firewall at the lab,
and accessed through a low-level C++ API known as MYA. A tool called
myaviewer provided by the accelerator group is capable of producing
some useful plots, but its functionality is limited and the gui is 
extremely slow to use from off-site because it is X11-based. The
EPICSscalers.js project was created to provide a web browser-based
interface to the scalers that is fast and responsive. Users can
easily extend it provide views and queries to meet their needs.

It is designed in a client/server architecture. The client side is
built using the angular.js application framework, which makes it
fast and responsive. The backend is built using node.js, which
makes it able to handle asynchronous requests efficiently. It is
easy to deploy in userspace without needing root access
or special permissions. Any user can start the backend as a
node.js process listening on a non-privileged port on some server
in the counting house, and then access that server port from any
machine in the counting house.

## History

The initial release of this package was written in December 2017.
The backend process communicates with mysql servers running on the
opsmya0...9 servers operated by the accelerator group, and with the
hallddb server which provides access to the rcdb database. The number
and location of these servers may change with time.

## Release history

See VERSIONS file in the project directory.

## Usage synopsis

The server is designed to accept requests directed to a non-privileged
port on some computer in the Hall D counting house. By default, that
port is 8080. This computer must have the node.js rpm package installed.
Start the server from within the directory where EPICSscalers.js was
installed by the "git clone" command, using the following command.

  $ node scalers.js

Once this is running, direct a web browser to the chosen port on the
host machine, (eg. http://gluon03.jlab.org:8080). The interactive page
displayed should be self-explantory.

## Dependencies

The server host must have the node.js package installed. It must also
have direct network access to the opsmya0... servers in the acc.jlab.org
domain, and the firewall must allow outside access to the server port,
eg. 8080. Any modern browser should be capable of supporting the
angular.js client.

## Bugs

None known at this time.

## How to contribute

## Contact the authors

Write to richard.t.jones at uconn.edu.
