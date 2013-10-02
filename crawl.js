var cheerio = require('cheerio');
var request = require('request');
var url = require('url');

function crawl(uri, linkCheckOnly, cb) {
    request.head({
        uri: uri,
        followRedirect: false
    }, function (err, response, body) {
        if (err) return cb(err);
        var result = {
            statusCode: response.statusCode,
            contentType: response.headers['content-type'],
            uri: response.request.uri.href,
            refs: [ ]
        };
        var base = response.request.uri.href;
        if (response.statusCode >= 300 && response.statusCode < 400 &&
            response.headers['location']) {
            result.refs.push({
                type: 'redirect',
                uri: url.resolve(base, response.headers['location'])
            });
            cb(null, result);
        } else if (response.statusCode == 200 &&
                   result.contentType.indexOf('text/html') == 0 &&
                   !linkCheckOnly) {
            request.get({
                uri: response.request.uri.href
            }, function (err, response, body) {
                if (err) return cb(err);
                try {
                    parseHtml(base, cheerio.load(body), result);
                } catch (e) {
                    result.htmlParseError = e.toString();
                    // FIXME store error in DB somehow
                }
                cb(null, result);
            });
        } else {
            cb(null, result);
        }
    });
}

function parseHtml(base, $, result) {
    $('base').each(function (index, el) {
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

module.exports = crawl;
