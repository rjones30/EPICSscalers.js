# EPICSscalers.js - web client for display and animation of EPICS scalers

## Author

* Richard Jones, University of Connecticut, Storrs, CT

## Description

The scalers for the GlueX experiment are stored in the EPICS archiver
operated by the Jefferson Lab accelerator group. This information is
housed in mysql servers located behind a double firewall at the lab,
which makes it a bear to access from off-site. Not only that, but the
lookup and display tools for examining scalers are rudimentary and
limited in functionality. This project was created to build a web
browser-based interface to the scalers that users can easily extend
to provide views and queries to meet their needs. It is designed in
a client/server architecture. The client side is built using the
angular.js application framework, which makes it fast and responsive.
The backend is built using node.js, which makes it easy to deploy in
userspace without needing root access or special permissions. Any
user can start the backend as a node.js process listening on a 
non-privileged port on some server in the counting house, and then
access that server port over a ssh tunnel from anywhere on the web.

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
