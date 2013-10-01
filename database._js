var fs = require('fs');
var async = require('async');
var sqlite3 = require('sqlite3');

var dbTables = [ {
    name: 'crawl_prefixes',
    columns: [
        'prefix TEXT PRIMARY KEY'
    ]
}, {
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

Database.prototype.transaction = function (body, _) {
    try {
        this.db.run('BEGIN', _);
        body(_);
        this.db.run('COMMIT', _);
    } catch (e) {
        this.db.run('ROLLBACK', _);
        throw e;
    }
};

function newSqlite3Database(name, flags, cb) {
    var db = new sqlite3.Database(name, flags, function (err) {
        cb(err, db);
    });
}

Database.prototype.create = function (_) {
    if (this.name !== ':memory:' && fs.existsSync(this.name))
        throw new Error('Database already exists');
    this.db = newSqlite3Database(
        this.name,
        sqlite3.OPEN_READWRITE | sqlite3.OPEN_CREATE,
        _);
    this.transaction((function (_) {
        dbTables.forEach_(_, function(_, tableSpec) {
            this.createTable(tableSpec, _);
        }, this);
    }).bind(this), _);
    this.refreshCrawlPrefixes(_);
};

Database.prototype.addCrawlPrefix = function (prefix, _) {
    this.db.run('INSERT OR IGNORE INTO crawl_prefixes (prefix) VALUES (?)',
                prefix, _);
    this.refreshCrawlPrefixes(_);
};

Database.prototype.refreshCrawlPrefixes = function (_) {
    var rows = this.db.all('SELECT prefix FROM crawl_prefixes', _);
    this.crawlPrefixes = rows.map(function (row) {
        return row.prefix;
    });
};

Database.prototype.isUriCrawlable = function (uri) {
    for (var i = 0; i < this.crawlPrefixes.length; ++i) {
        if (uri.indexOf(this.crawlPrefixes[i]) == 0)
            return true;
    }
    return false;
}

Database.prototype.createTable = function (tableSpec, _) {
    var stmt =
        'CREATE TABLE ' + tableSpec.name +
        ' (' + tableSpec.columns.join(', ') + ')';
    console.log(stmt);
    this.db.run(stmt, _);
};

Database.prototype.getPendingUri = function (_) {
    var row = this.db.get(
        'SELECT uri FROM uris WHERE response_code IS NULL LIMIT 1', _);
    return row.uri;
};

Database.prototype.addPending = function (uri, _) {
    this.db.run('INSERT OR IGNORE INTO uris (uri) VALUES (?)',
                uri);
};

Database.prototype.addResult = function (result, _) {
    if (!this.isUriCrawlable(result.uri))
        return;
    var row = this.db.get('SELECT uri, response_code FROM uris WHERE uri = ?',
                          result.uri, _);
    if (row && row.response_code)
        return;
    this.db.run('INSERT OR REPLACE INTO uris ' +
                '(uri, response_code, content_type) ' +
                'VALUES (?, ?, ?)',
                result.uri, result.statusCode, result.contentType, _);

    var prevUri = result.originalUri;
    result.redirects.forEach_(_, function (_, redirect) {
        if (this.isUriCrawlable(prevUri)) {
            this.db.run('INSERT OR REPLACE INTO uris ' +
                        '(uri, response_code) ' +
                        'VALUES (?, ?)',
                        prevUri, redirect.statusCode, _);
            this.db.run('INSERT OR REPLACE INTO redirects ' +
                        '(from_uri, to_uri) ' +
                        'VALUES (?, ?)',
                        prevUri, result.uri, _);
        }
        prevUri = redirect.redirectUri;
    }, this);

    result.refs.forEach_(_, function (_, ref) {
        this.addResultRef(result.uri, ref, _);
    }, this);
};

Database.prototype.addResultRef = function (uri, ref, _) {
    this.db.run('INSERT INTO refs ' +
                '(from_uri, type, to_uri) ' +
                'VALUES (?, ?, ?)',
                uri, ref.type, ref.uri, _);
};

var crawl = require('./crawl');

var db = new Database('test.db');
db.create(_);
db.addCrawlPrefix('https://www.lavos.net/', _);
var result = crawl('http://www.lavos.net/', _);
console.log(result);
db.transaction(function (_) {
    db.addResult(result, _);
}, _);
console.log('wtf?');
