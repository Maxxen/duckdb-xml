var duckdb = require('../../duckdb/tools/nodejs');
var assert = require('assert');

describe(`xml extension`, () => {
    let db;
    let conn;
    before((done) => {
        db = new duckdb.Database(':memory:', {"allow_unsigned_extensions":"true"});
        conn = new duckdb.Connection(db);
        conn.exec(`LOAD '${process.env.QUACK_EXTENSION_BINARY_PATH}';`, function (err) {
            if (err) throw err;
            done();
        });
    });

    it('xml function should return expected string', function (done) {
        db.all("SELECT xml('Sam') as value;", function (err, res) {
            if (err) throw err;
            assert.deepEqual(res, [{value: "Xml Sam 🐥"}]);
            done();
        });
    });

    it('xml_openssl_version function should return expected string', function (done) {
        db.all("SELECT xml_openssl_version('Michael') as value;", function (err, res) {
            if (err) throw err;
            assert(res[0].value.startsWith('Xml Michael, my linked OpenSSL version is OpenSSL'));
            done();
        });
    });
});