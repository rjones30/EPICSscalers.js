// 
// scalers.js - a node.js server for the EPICSscalers.js web client
//
// author: richard.t.jones at uconn.edu
// version: december 26, 2017
//

const fs = require('fs');
const url = require('url');
const http = require('http');
const mysql = require('mysql');
const Promise = require('es6-promise').Promise;
const child_process = require('child_process');

// cache of EPICS variables kept in memory
// consisting of parallel arrays indexed by chan_id,
// augmented with map from variable name to chan_id.

var varcache = {"name"    : [],      // map : chan_id -> name
                "type"    : [],      // map : chan_id -> type
                "size"    : [],      // map : chan_id -> size
                "host_id" : [],      // map : chan_id -> host_id
                "chan_id" : {}};     // map : name -> chan_id

var myahost_id = {"opsmya0" : 0, 
                  "opsmya1" : 1,
                  "opsmya2" : 2,
                  "opsmya3" : 3,
                  "opsmya4" : 4,
                  "opsmya5" : 5,
                  "opsmya6" : 6,
                  "opsmya7" : 7,
                  "opsmya8" : 8};

var myahost = [{"name":"opsmya0.acc.jlab.org", 
                  "proxy":"gluey.phys.uconn.edu", "port": 63306},
               {"name":"opsmya1.acc.jlab.org",
                  "proxy":"gluey.phys.uconn.edu", "port": 63307},
               {"name":"opsmya2.acc.jlab.org",
                  "proxy":"gluey.phys.uconn.edu", "port": 63308},
               {"name":"opsmya3.acc.jlab.org",
                  "proxy":"gluey.phys.uconn.edu", "port": 63309},
               {"name":"opsmya4.acc.jlab.org",
                  "proxy":"gluey.phys.uconn.edu", "port": 63310},
               {"name":"opsmya5.acc.jlab.org",
                  "proxy":"gluey.phys.uconn.edu", "port": 63311},
               {"name":"opsmya6.acc.jlab.org",
                  "proxy":"gluey.phys.uconn.edu", "port": 63312},
               {"name":"opsmya7.acc.jlab.org",
                  "proxy":"gluey.phys.uconn.edu", "port": 63313},
               {"name":"opsmya8.acc.jlab.org",
                  "proxy":"gluey.phys.uconn.edu", "port": 63314}];

var last_varcache_refresh = 0;
var varcache_timeout = 21600000; // 6 hours

// create a connection pool to the mya backend servers

var mya_pool = [];
for (var i=0; i < 9; ++i) {
    var pool = mysql.createPool({ connectionLimit: 10, //important
                                  host: myahost[i].proxy,
                                  port: myahost[i].port,
                                  user: "myapi",
                                  password: "<redacted>",
                                  database: "archive"});
    mya_pool.push(pool);
}

// create a connection pool to the rcdb server

var rcdb_pool = mysql.createPool({ connectionLimit: 10, //important
                                   host: "hallddb.jlab.org",
                                   port: 3306,
                                   user: "rcdb",
                                   database: "rcdb"});

// spawn a python shell to interpret python expressions

var pyshell = child_process.spawn('./pyshell.py');
pyshell.stdout.on('data', pyshell_output_listener);
pyshell.stdout.on('error', pyshell_fault_listener);
pyshell.stdout.on('close', pyshell_close_listener);
pyshell.stderr.on('data', pyshell_error_listener);
pyshell.stderr.on('error', pyshell_fault_listener);
pyshell.stderr.on('close', pyshell_close_listener);
var pyshell_queue = [];

function pyshell_query(message) {
    // Send message to the python shell as a command, and return
    // a promise for the response. All communication with the
    // python shell should take place through this function
    // properly manage the serialization.

    return new Promise(function(resolve, reject) {
        pyshell_queue.push({message: message,
                            response: "",
                            resolve: resolve,
                            reject: reject});
        if (pyshell_queue.length == 1) {
            pyshell.stdin.write(message + '\n');
        }
    });
}

function pyshell_output_listener(data) {
    // Event listener for stdout responses from the python shell,
    // passes the response to the first resolve in the queue.
    // Responses are left open until reception of a nl termination
    // character. When a response terminates the original promise
    // is resolved and the next message in the queue is sent, if any.
 
    if (pyshell_queue.length == 0) {
        console.log("unexpected message from pyshell: " + data);
        return;
    }
    pyshell_queue[0].response += data;
    if (data[data.length - 1] == 0x0a) {
        pyshell_queue[0].resolve(pyshell_queue[0].response);
        pyshell_queue.shift();
        if (pyshell_queue.length > 0) {
            pyshell.stdin.write(pyshell_queue[0].message + '\n');
        }
    }
}

function pyshell_error_listener(data) {
    // Event listener for stderr responses from the python shell,
    // passes the response to the first resolve in the queue.
    // Responses are left open until reception of a nl termination
    // character. When a response terminates the original promise
    // is resolved and the next message in the queue is sent, if any.

    if (pyshell_queue.length == 0) {
        console.log("unexpected message from pyshell: " + data);
        return;
    }
    pyshell_queue[0].response += data;
    if (data[data.length - 1] == 0x0a) {
        pyshell_queue[0].reject(pyshell_queue[0].response);
        pyshell_queue.shift();
        if (pyshell_queue.length > 0) {
            pyshell.stdin.write(pyshell_queue[0].message + '\n');
        }
    }
}

function pyshell_fault_listener(data) {
    // Event listener for communications faults with the python shell,
    // hangs the connection. Best to let the system crash and do a
    // most-mortem than try to recover automatically and let a bad
    // client overwhelm us.

    console.log("unexpected fault from pyshell: " + data);
}

function pyshell_close_listener(data) {
    // Event listener for communications faults with the python shell,
    // hangs the connection. Best to let the system crash and do a
    // most-mortem than try to recover automatically and let a bad
    // client overwhelm us.

    console.log("unexpected close from pyshell: " + data);
}

function do_groups_list(req_obj) {
    // Query EPICS the database for a list of all groups found.
    // If there is a connection error or some problem fetching 
    // results, report that as a http error but resolve the
    // promise, do not fail over to reject.

    return new Promise(function(resolve, reject) {
        mya_pool[0].getConnection(function(err, con) {
            if (err) {
                resolve({code: 500, content: err.message, type: 'plain'});
                return;
            }   
            sql = "select group_id, name from groups" +
                  " where name like 'HD_%' or name = 'bpm'" +
                  " or name like 'bcm%'" +
                  " or name like 'HallD%'" +
                  " or name like 'COLLIMATOR%'" +
                  " or name like 'PROFILER%'" +
                  " or name like 'tag%'" +
                  " or name like 'Tag%'" +
                  " or name like 'TAG%'" +
                  " or name like 'magnets%'" +
                  " or name = 'utilityMeters';"
            con.query(sql, function(err, result, fields) {
                con.release();
                if (err) {
                    resolve({code: 500, content: err.message, type: 'plain'});
                    return;
                }   
                var json = "{";
                for (var i in result) {
                    if (i > 0)
                        json += ',';
                    json += '"' + result[i]['name'] + '"';
                    json += ':' + result[i]['group_id'];
                }
                json += "}";
                resolve({code: 200, content: json, type: 'json'});
            });
        });
    });
}

function do_channels_group(req_obj) {
    // Query the EPICS database for a list of all variables
    // belonging to the specified group encoded in the url.
    // If there is a connection error or some problem fetching 
    // results, report that as a http error but resolve the
    // promise, do not fail over to reject.

    return new Promise(function(resolve, reject) {
        mya_pool[0].getConnection(function(err, con) {
            if (err) {
                resolve({code: 500, content: err.message, type: 'plain'});
                return;
            }   
            sql = "select members.chan_id, name, type, size, host" +
                  " from members join channels" +
                  " on members.chan_id = channels.chan_id" +
                  " where members.group_id = " + 
                  req_obj.query.group.toString() +
                  " order by members.chan_id;";
            con.query(sql, function(err, result, fields) {
                con.release();
                if (err) {
                    resolve({code: 500, content: err.message, type: 'plain'});
                    return;
                }   
                var json = "[";
                for (var i in result) {
                    if (i > 0)
                        json += ',{';
                    else
                        json += '{';
                    json += '"name":"' + result[i]['name'] + '",';
                    json += '"type":' + result[i]['type'] + ',';
                    json += '"size":' + result[i]['size'] + ',';
                    json += '"host":"' + result[i]['host'] + '",';
                    json += '"chan":"' + result[i]['chan_id'] + '"}';
                }
                json += "]";
                resolve({code: 200, content: json, type: 'json'});
            });
        });
    });
}

function do_run_times(req_obj) {
    // Query the GlueX rcdb database for the start and end times
    // for a particular run. If there is a connection error or
    // some problem fetching results, report that as a http error
    // but resolve the promise, do not fail over to reject.

    return new Promise(function(resolve, reject) {
        rcdb_pool.getConnection(function(err, con) {
            if (err) {
                resolve({code: 500, content: err.message, type: 'plain'});
                return;
            }   
            sql = "select number as run," +
                  " UNIX_TIMESTAMP(started) as started," +
                  " UNIX_TIMESTAMP(finished) as finished" +
                  " from runs where number <= " + 
                  req_obj.query.run.toString() +
                  " order by number desc limit 1;";
            con.query(sql, function(err, result, fields) {
                con.release();
                if (err || result.length==0) {
                    console.log("bad rcdb query: " + err.message);
                    resolve({code: 500, content: err.message, type: 'plain'});
                    return;
                }   
                var json = '{"run":' + result[0]['run'] + 
                           ',"starttime":' + result[0]['started'] + 
                           ',"endtime":' + result[0]['finished'] +
                           '}';
                resolve({code: 200, content: json, type: 'json'});
            });
        });
    });
}

function do_test_mapstring(req_obj) {
    // Pass the test mapstring to the pyshell and return the result.

    const i = (req_obj.query.i)? req_obj.query.i.toString() : '0';
    const j = (req_obj.query.j)? req_obj.query.j.toString() : '0';
    const k = (req_obj.query.k)? req_obj.query.k.toString() : '0';
    var mapstring = req_obj.query.mapstring;
    var expr = '(lambda i,j,k:' + mapstring + ')' +
               '(' + i + ',' + j + ',' + k + ')';
    return pyshell_query(expr).then(function(result) {
        return {code: 200, content: result, type: 'json'};
    }).catch(function(err) {
        return {code: 500, content: err, type: 'plain'};
    });
}

function do_eval_namestring(req_obj) {
    // Pass the namestring to the pyshell and return the result.
 
    const i0 = (req_obj.query.i0)? req_obj.query.i0.toString() : '0';
    const i1 = (req_obj.query.i1)? req_obj.query.i1.toString() : '0';
    const j0 = (req_obj.query.j0)? req_obj.query.j0.toString() : '0';
    const j1 = (req_obj.query.j1)? req_obj.query.j1.toString() : '0';
    const k0 = (req_obj.query.k0)? req_obj.query.k0.toString() : '0';
    const k1 = (req_obj.query.k1)? req_obj.query.k1.toString() : '0';
    var namestring = req_obj.query.namestring.replace(/"/g, '\\"')
                                             .replace(/'/g, "\\'");
    var prog;
    if (i1 > i0)
        prog = 'iterate("lambda i:' + namestring + '",' +
               '[' + i0 + '], [' + i1 + '])';
    else if (j1 > j0)
        prog = 'iterate("lambda j:' + namestring + '",' +
               '[' + j0 + '], [' + j1 + '])';
    else if (k1 > k0)
        prog = 'iterate("lambda k:' + namestring + '",' +
               '[' + k0 + '], [' + k1 + '])';
    return pyshell_query(prog).then(function(result) {
        json = JSON.parse(result);
        json.chan_id = [];
        for (var i in json.names) {
            var name = json.names[i]
            var name_valid = (name in varcache.chan_id);
            if (!name_valid) {
                var m1 = name.match(/\[([0-9]+)\]$/);
                if (m1) {
                    name = name.replace(/\[[0-9]+\]$/, '');
                    if (name in varcache.chan_id) {
                       var index = parseInt(m1[1]);
                       var chan_id = varcache.chan_id[name];
                       name_valid = (index < varcache.size[chan_id]);
                    }
                }
            }
            if (name_valid)
                json.chan_id.push(varcache.chan_id[name]);
            else
                json.chan_id.push(null);
        }
        return {code: 200, content: JSON.stringify(json), type: 'json'};
    }).catch(function(err) {
        console.log("Error - " + err);
        return {code: 500, content: err, type: 'plain'};
    });
}

function do_eval_mapstring(req_obj) {
    // Pass the eval mapstring to the pyshell and return the result.
 
    const i0 = (req_obj.query.i0)? req_obj.query.i0.toString() : '0';
    const i1 = (req_obj.query.i1)? req_obj.query.i1.toString() : '0';
    const j0 = (req_obj.query.j0)? req_obj.query.j0.toString() : '0';
    const j1 = (req_obj.query.j1)? req_obj.query.j1.toString() : '0';
    const k0 = (req_obj.query.k0)? req_obj.query.k0.toString() : '0';
    const k1 = (req_obj.query.k1)? req_obj.query.k1.toString() : '0';
    var mapstring = req_obj.query.mapstring.replace(/"/g, '\\"')
                                           .replace(/'/g, "\\'");
    var prog = 'iterate("lambda i,j,k:' + mapstring + '",' +
               '[' + i0 + ',' + j0 + ',' + k0 + '],' +
               '[' + i1 + ',' + j1 + ',' + k1 + '])';
    return pyshell_query(prog).then(function(result) {
        json = JSON.parse(result);
        json.chan_id = [];
        for (var i in json.names) {
            var name = json.names[i]
            var name_valid = (name in varcache.chan_id);
            if (!name_valid) {
                var m1 = name.match(/\[([0-9]+)\]$/);
                if (m1) {
                    var basename = name.replace(/\[0-9]+\]$/, '');
                    if (basename in varcache.chan_id) {
                       var index = parseInt(m1[1]);
                       var chan_id = varcache.chan_id[basename];
                       name_valid = (index < varcache.size[chan_id]);
                    }
                }
            }
            if (name_valid)
                json.chan_id.push(varcache.chan_id[name]);
            else
                json.chan_id.push(null);
        }
        return {code: 200, content: JSON.stringify(json), type: 'json'};
    }).catch(function(err) {
        console.log("Error - " + err);
        return {code: 500, content: err, type: 'plain'};
    });
}

function readFile(filepath, encoding) {
    // Wrap fs.readFile to make it return a promise.

    return new Promise(function(resolve, reject) {
        fs.readFile(filepath, encoding, function(err, data) {
            if (err) {
                reject(err);
                return;
            }
            resolve(data);
        });
    });
}

function refresh_channels_cache() {
    // Query the EPICS database for a list of all variables
    // and store the complete lookup table in memory. Use a
    // timeout to determine whether to fetch a new copy from
    // the backend or use the existing copy in memory.

    var now = (new Date).getTime();
    if (now < last_varcache_refresh + varcache_timeout) 
        return 0;

    return new Promise(function(resolve, reject) {
        mya_pool[0].getConnection(function(err, con) {
            if (err) {
                console.log("Error connecting to pool 0: " + err.message);
                reject(err);
                return;
            }   
            sql = "select chan_id, name, type, size, host" +
                  " from channels order by chan_id;";
            con.query(sql, function(err, result, fields) {
                con.release();
                if (err) {
                    console.log("Error reading channels table" +
                                " from pool 0: " + err.message);
                    reject(err);
                    return;
                }   
                var chan_id = 0;
                for (var i in result) {
                    while (chan_id < result[i].chan_id) {
                        varcache.name.push(null);
                        varcache.type.push(null);
                        varcache.size.push(null);
                        varcache.host_id.push(null);
                        chan_id++;
                    }
                    var name = result[i].name;
                    var type = result[i].type;
                    var size = result[i].size;
                    var host = result[i].host;
                    varcache.name.push(name);
                    varcache.type.push(type);
                    varcache.size.push(size);
                    varcache.host_id.push(myahost_id[result[i].host]);
                    varcache.chan_id[name] = chan_id;
                    chan_id++;
                }
                resolve();
            });
        });
    });
}

refresh_channels_cache();

http.createServer(function (req, res) {
    // This is the main web server event listener.
    // The query is available in req, and res is
    // the handle through which a response is sent.

    var send_response = function(result) {
        res.writeHead(result.code, {'Content-Type': 'text/' + result.type});
        res.end(result.content);
        console.log("sent response in format " + result.type + 
                    " length " + result.content.length.toString())
    };

    var req_obj = url.parse(req.url, true);
    if (req_obj.query.request) {
        if (req_obj.query.request == "list_groups")
            do_groups_list(req_obj).then(send_response);
        else if (req_obj.query.request == "list_channels")
            do_channels_group(req_obj).then(send_response);
        else if (req_obj.query.request == "run_times")
            do_run_times(req_obj).then(send_response);
        else if (req_obj.query.request == "test_mapping")
            do_test_mapstring(req_obj).then(send_response);
        else if (req_obj.query.request == "eval_names")
            do_eval_namestring(req_obj).then(send_response);
        else if (req_obj.query.request == "eval_mapping")
            do_eval_mapstring(req_obj).then(send_response);
        else
            send_response(400, "400 Bad Request", "plain");
    }
    else if (req_obj.pathname.length > 1) {
        var code;
        var type;
        var content;
        var js = req_obj.pathname.match(/^\/(.*\.js)$/);
        var css = req_obj.pathname.match(/^\/(.*\.css)$/);
        var ico = req_obj.pathname.match(/^\/(.*\.ico)$/);
        if (js) {
            code = 200;
            type = 'javascript';
            content = readFile(js[1], 'utf8');
        }
        else if (css) {
            code = 200;
            type = 'css';
            content = readFile(css[1], 'utf8');
        }
        else if (ico) {
            code = 200;
            type = 'ico';
            content = readFile(ico[1], 'utf8');
        }
        else {
            code = 404;
            type = 'plain';
            content = Promise.resolve("Unsupported filetype");
            console.log("unexpected request received for file " +
                        req_obj.pathname);
        }
        content.then(function(text) {
            send_response({code: code, content: text, type: type});
        }).catch(function(err) {
            send_response({code: 404, content: err, type: 'plain'});
        });
    }
    else {
        readFile('scalers.html', 'utf8').then(function(text) {
            send_response({code: 200, content: text, type: 'html'});
        });
    }

}).listen(8080);
