#!/bin/sh

if [ -z $(which initdb) ] ; then
    echo "Postgres not found, you may need to launch like so: PATH=\$PATH:/usr/lib/postgresql/9.3/bin/ $0"
    exit 1
fi

## Thanks to Matwey V. Kornilov ( https://github.com/matwey ) for
## this:

initdb --locale en_US.UTF-8 datadir
cat > datadir/postgresql.conf <<EOF
ssl = on
ssl_ca_file = 'root.crt'
EOF

cp test_data/epgsql.crt datadir/server.crt
cp test_data/epgsql.key datadir/server.key
cp test_data/root.crt datadir/root.crt
cp test_data/root.key datadir/root.key
chmod 0600 datadir/server.key

cat > datadir/pg_hba.conf <<EOF
local   all             $USER                                   trust
host    template1	$USER                   127.0.0.1/32    trust
host    $USER           $USER                   127.0.0.1/32    trust
host    epgsql_test_db1 $USER                   127.0.0.1/32    trust
host    epgsql_test_db1 epgsql_test             127.0.0.1/32    trust
host    epgsql_test_db1 epgsql_test_md5         127.0.0.1/32    md5
host    epgsql_test_db1 epgsql_test_cleartext   127.0.0.1/32    password
hostssl epgsql_test_db1 epgsql_test_cert        127.0.0.1/32    cert clientcert=1
EOF

