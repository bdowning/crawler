var cheerio = require('cheerio');
var request = require('request');
var url = require('url');

function crawl(uri, cb) {
    console.log('req', uri);
    request.get({
        uri: uri
    }, function (err, response, body) {
        console.log('resp', uri);
        if (err) return cb(err);
        var result = {
            statusCode: response.statusCode,
            contentType: response.headers['content-type'],
            originalUri: uri,
            redirects: response.request.redirects,
            uri: response.request.uri.href,
            refs: [ ]
        };
        var base = response.request.uri.href;
        if (response.statusCode == 200 && result.contentType == 'text/html') {
            var $ = cheerio.load(body);

            $('base').each(function (index, el) {
                console.log('new base', el.attribs.base);
                base = el.attribs.base;
            });

            $('[href], [src]').each(function (index, el) {
                var attr, uri, type;
                if (el.attribs.href)
                    attr = 'href'
                if (el.attribs.src)
                    attr = 'src';
                uri = el.attribs[attr];
                if (el.name == 'link') {
                    el.attribs.rel.split(/\s+/).forEach(function (rel) {
                        result.refs.push({
                            type: 'link rel="' + rel.toLowerCase() + '"',
                            uri: url.resolve(base, uri)
                        });
                    });
                } else {
                    result.refs.push({
                        type: el.name,
                        uri: url.resolve(base, uri)
                    });
                }
            });
        }

        cb(null, result);
    });
}

// crawl('http://www.stephenwolfram.com/', function (err, result) {
//     console.log(err);
//     console.log(result);
// });

module.exports = crawl;
