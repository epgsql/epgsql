#!/bin/bash
# https://www.postgresql.org/docs/current/ssl-tcp.html#SSL-CERTIFICATE-CREATION

DATADIR=test/data
CA_SUBJ="/CN=epgsql CA"
CLIENT_SUBJ="/CN=epgsql_test_cert"
SERVER_SUBJ="/CN=epgsql server"
set -x

# generate root key
openssl genrsa -out ${DATADIR}/root.key 2048
# generate root cert
openssl req -new -x509 -text -days 3650 -key ${DATADIR}/root.key -out ${DATADIR}/root.crt -subj "$CA_SUBJ"

# generate server key
openssl genrsa -out ${DATADIR}/server.key 2048
# generate server signature request
openssl req -new -key ${DATADIR}/server.key -out ${DATADIR}/server.csr -subj "$SERVER_SUBJ"
# create signed server cert
openssl x509 -req -text -days 3650 -in ${DATADIR}/server.csr -CA ${DATADIR}/root.crt -CAkey ${DATADIR}/root.key -CAcreateserial -out ${DATADIR}/server.crt

# generate client key
openssl genrsa -out ${DATADIR}/client.key 2048
# generate client signature request
openssl req -new -key ${DATADIR}/client.key -out ${DATADIR}/client.csr -subj "$CLIENT_SUBJ"
# create signed client cert
openssl x509 -req -text -days 3650 -in ${DATADIR}/client.csr -CA ${DATADIR}/root.crt -CAkey ${DATADIR}/root.key -CAcreateserial -out ${DATADIR}/client.crt

# generate bad client key and cert
openssl genrsa -out ${DATADIR}/bad-client.key 2048
openssl req -new -x509 -text -days 3650 -key ${DATADIR}/bad-client.key -out ${DATADIR}/bad-client.crt -subj "$CLIENT_SUBJ"

rm ${DATADIR}/*.{csr,srl}
