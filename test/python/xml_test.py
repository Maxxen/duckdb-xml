import duckdb
import os
import pytest

# Get a fresh connection to DuckDB with the xml extension binary loaded
@pytest.fixture
def duckdb_conn():
    extension_binary = os.getenv('QUACK_EXTENSION_BINARY_PATH')
    if (extension_binary == ''):
        raise Exception('Please make sure the `QUACK_EXTENSION_BINARY_PATH` is set to run the python tests')
    conn = duckdb.connect('', config={'allow_unsigned_extensions': 'true'})
    conn.execute(f"load '{extension_binary}'")
    return conn

def test_xml(duckdb_conn):
    duckdb_conn.execute("SELECT xml('Sam') as value;");
    res = duckdb_conn.fetchall()
    assert(res[0][0] == "Xml Sam 🐥");

def test_xml_openssl_version_test(duckdb_conn):
    duckdb_conn.execute("SELECT xml_openssl_version('Michael');");
    res = duckdb_conn.fetchall()
    assert(res[0][0][0:51] == "Xml Michael, my linked OpenSSL version is OpenSSL");