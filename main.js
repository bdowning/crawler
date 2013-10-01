var cheerio = require('cheerio');
var request = require('request');
var url = require('url');

var crawlBase = 'http://www.wolfram.com/';

request({
    uri: crawlBase
}, function (err, response, body) {
    if (err) throw err;
    var base = response.request.uri.href;
    if (response.statusCode == 200) {
        var $ = cheerio.load(body);

        $('base').each(function (index, el) {
            console.log('new base', el.attribs.base);
            base = el.attribs.base;
        });

        $('[href], [src]').each(function (index, el) {
            var attr;
            var uri, resolved;
            if (el.attribs.href)
                attr = 'href'
            if (el.attribs.src)
                attr = 'src';
            uri = el.attribs[attr];
            resolved = url.resolve(base, uri);
            console.log(el.type, el.name, attr, uri);
            console.log('\t', resolved);
        });
    }
})
