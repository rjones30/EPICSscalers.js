var http = require('http');
var mysql = require('mysql');
var url = require('url');
var fs = require('fs');

function do_display_webform(callback) {
  fs.readFile('scalers.html', 'utf8', function(err, contents) {
    callback(200, contents, 'html');
  });
};

function do_return_file(pathname, callback) {
  var js = pathname.match(/^\/(.*\.js)$/);
  var css = pathname.match(/^\/(.*\.css)$/);
  var ico = pathname.match(/^\/(.*\.ico)$/);
  if (js) {
    fs.readFile(js[1], 'utf8', function(err, contents) {
      callback(200, contents, 'javascript');
    });
  }
  else if (css) {
    fs.readFile(css[1], 'utf8', function(err, contents) {
      callback(200, contents, 'css');
    });
  }
  else if (ico) {
    fs.readFile(ico[1], 'utf8', function(err, contents) {
      callback(200, contents, 'ico');
    });
  }
  else {
    console.log("unexpected request received for file " + pathname);
  }
};

function do_list_groups(callback) {
  var con = mysql.createConnection({
    host: "gluey.phys.uconn.edu",
    port: 63306,
    user: "myapi",
    password: "MYA",
    database: "archive"
  });
  con.connect();
  sql = "select group_id, name from groups" +
        " where name like 'HD_%' or name = 'bpm'" +
        " or name like 'HallD%' or name = 'utilityMeters';"
  con.query(sql, function (err, result, fields) {
    if (err)
      return callback(500, err.message, 'plain');
    listing = "{";
    for (var i in result) {
      if (i > 0)
        listing += ', ';
      listing += '"' + result[i]['name'] + '"';
      listing += ':' + result[i]['group_id'];
    }
    listing += "}";
    callback(200, listing, 'json');
    con.end();
  });
}

function do_list_channels(group, callback) {
  var con = mysql.createConnection({
    host: "gluey.phys.uconn.edu",
    port: 63306,
    user: "myapi",
    password: "MYA",
    database: "archive"
  });
  con.connect();
  sql = "select members.chan_id, name, type, size, host" +
        " from members join channels" +
        " on members.chan_id = channels.chan_id" +
        " where members.group_id = " + group.toString() +
        " order by members.chan_id;";
  con.query(sql, function (err, result, fields) {
    if (err)
      return callback(500, err.message, 'plain');
    listing = "["
    for (var i in result) {
      if (i == 0)
        listing += '{';
      else
        listing += ', {';
      listing += '"name":"' + result[i]['name'] + '",';
      listing += '"type":' + result[i]['type'] + ',';
      listing += '"size":' + result[i]['size'] + ',';
      listing += '"host":"' + result[i]['host'] + '",';
      listing += '"chan":"' + result[i]['chan_id'] + '"}';
    }
    listing += "]";
    callback(200, listing, 'json');
    con.end();
  });
}

function do_run_times(run, callback) {
  var con = mysql.createConnection({
    host: "hallddb.jlab.org",
    port: 3306,
    user: "rcdb",
    database: "rcdb"
  });
  con.connect();
  sql = "select number as run," +
        " UNIX_TIMESTAMP(started) as started," +
        " UNIX_TIMESTAMP(finished) as finished" +
        " from runs where number <= " + run.toString() +
        " order by number desc limit 1;";
  con.query(sql, function (err, result, fields) {
    if (err || result.length == 0) {
      console.log("bad rcdb query: " + err.message);
      return callback(500, err.message, 'plain');
    }
    listing = '{"run":' + result[0]['run'] + 
              ',"starttime":' + result[0]['started'] + 
              ',"endtime":' + result[0]['finished'] +
              '}';
    callback(200, listing, 'json');
    con.end();
  });
}

http.createServer(function (req, res) {
  var return_result = function(code, message, type) {
    res.writeHead(code, {'Content-Type': 'text/' + type});
    res.end(message);
    console.log("sent message in format " + type + " length " + message.length.toString())
  };
  var response = url.parse(req.url, true);
  var pathname = response.pathname;
  var query = response.query;
  if (query.request) {
    if (query.request == "list_groups")
      do_list_groups(return_result);
    else if (query.request == "list_channels")
      do_list_channels(query.group, return_result);
    else if (query.request == "run_times")
      do_run_times(query.run, return_result);
    else
      return_result(400, "400 Bad Request", "plain");
  }
  else if (pathname.length > 1) {
    do_return_file(pathname, return_result);
  }
  else
    do_display_webform(return_result);
}).listen(8080);
