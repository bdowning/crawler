var fs = require('fs');
var async = require('async');
var sqlite3 = require('sqlite3');

var dbTables = [ {
    name: 'uris',
    columns: [
        'uri TEXT PRIMARY KEY',
        'response_code INTEGER',
        'content_type TEXT'
    ]
}, {
    name: 'refs',
    columns: [
        'from_uri TEXT NOT NULL',
        'type TEXT NOT NULL',
        'to_uri TEXT NOT NULL'
    ]
}, {
    name: 'redirects',
    columns: [
        'from_uri TEXT PRIMARY KEY',
        'to_uri TEXT NOT NULL'
    ]
} ];

function Database(name) {
    this.name = name;
}

Database.prototype.waterfall = function (tasks, cb) {
    async.waterfall(tasks.map(function (f) { return f.bind(this) }, this), cb);
};

Database.prototype.transaction = function (body, cb) {
    this.waterfall([
        function (cb) {
            this.db.run('BEGIN', cb);
        },
        function (cb) {
            body.bind(this)(cb);
        }
    ], (function (err) {
        console.log(err, err ? 'ROLLBACK' : 'COMMIT');
        this.db.run(err ? 'ROLLBACK' : 'COMMIT', function (err) { cb(err); });
    }).bind(this));
};

Database.prototype.create = function (cb) {
    this.waterfall([
        function (cb) {
            if (fs.existsSync(this.name))
                cb(new Error('Database already exists'));
            else
                this.db = new sqlite3.Database(
                    this.name,
                    sqlite3.OPEN_READWRITE | sqlite3.OPEN_CREATE,
                    cb);
        },
        function (cb) {
            this.transaction(function (cb) {
                async.eachSeries(dbTables, this.createTable.bind(this), cb);
            }, cb);
        }
    ], cb);
};

Database.prototype.createTable = function (tableSpec, cb) {
    var stmt =
        'CREATE TABLE ' + tableSpec.name +
        ' (' + tableSpec.columns.join(', ') + ')';
    console.log(stmt);
    this.db.run(stmt, cb);
};

Database.prototype.getPendingUri = function (cb) {
    this.db.get('SELECT uri FROM uris WHERE response_code IS NULL LIMIT 1',
                function (err, row) {
                    if (err) cb(err) else cb(null, row.uri);
                });
};

Database.prototype.addResult = function (result, cb) {
};

var db = new Database('test.db');
db.create(function (err) { console.log('done', err); });
