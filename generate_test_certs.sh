#!/bin/bash
# https://www.postgresql.org/docs/current/ssl-tcp.html#SSL-CERTIFICATE-CREATION

DATADIR=test/data
ROOT_SUBJ="/CN=epgsql CA"
EPGSQL_SUBJ="/CN=epgsql_test_cert"
set -x

# generate root key
openssl genrsa -out ${DATADIR}/root.key 2048
# generate root cert
openssl req -new -x509 -text -days 3650 -key ${DATADIR}/root.key -out ${DATADIR}/root.crt -subj "$ROOT_SUBJ"

# generate client/server key
openssl genrsa -out ${DATADIR}/epgsql.key 2048
# generate client/server signature request
openssl req -new -key ${DATADIR}/epgsql.key -out ${DATADIR}/epgsql.csr -subj "$EPGSQL_SUBJ"
# create signed client/server cert
openssl x509 -req -text -days 3650 -in ${DATADIR}/epgsql.csr -CA ${DATADIR}/root.crt -CAkey ${DATADIR}/root.key -CAcreateserial -out ${DATADIR}/epgsql.crt

rm ${DATADIR}/*.{csr,srl}
