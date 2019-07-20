#!/bin/bash
#
# This script generates a collection of certificate files for use in the
# gestalt unit tests.
#
# CA password is: gestalt
# CA common name: gestalt-ca
# Device common name: 127.0.0.1

# Cleanup from a previous run
rm -f *.csr *.key *.pem *.srl

# Certificate Authority (CA)
# ==========================
# Generate the key and a self-signed certificate that we will use as the
# Certificate Authority (CA). Use the ``subj`` argument to provide all the
# certificate subject material so that the output can be generated immediately
# (e.g without opening an interactive prompt).
#
# CA password is: gestalt
#
echo "Generating root CA key..."
openssl genrsa -aes256 -passout pass:gestalt-ca -out ca.key 4096

echo "Creating and self-signing CA root certificate..."
openssl req \
    -new -x509 -key ca.key -days 365 -out ca.pem -passin pass:'gestalt-ca' \
    -subj "/C=AU/ST=SA/L=Adelaide/O=MyOrg/OU=MyOrgUnit/CN=gestalt-ca"


# Server
# ======
# Generate the key to be used by the server, then generate a Certificate
# Signing Request to be used by the server and finally sign the server
# certificate with the CA key.
# Apparently matching server IP address with CN field of certificate has been
# deprecated for more than 15 years. So we need to pass an extra parameter
# specifying that ``subjectAltName=IP:127.0.0.1``
#
# Server password is: gestalt
# Server common name: 127.0.0.1
#
echo "Generating server key..."
openssl genrsa -out server.key 4096

echo "Generating server certificate signing request (csr)..."
openssl req -new -key server.key -out server.csr \
    -subj "/C=AU/ST=SA/L=Adelaide/O=MyOrg/OU=MyOrgUnit/CN=127.0.0.1"

echo "Signing server csr using root CA certificate..."
openssl x509  \
    -CA ca.pem -CAkey ca.key -CAcreateserial -passin pass:'gestalt-ca' \
    -req -in server.csr \
    -extfile <(printf "subjectAltName=IP:127.0.0.1") \
    -out server.pem -days 365

# This step also creates the *ca.slr* file which is a serial number record
# file. This is used later for signing the client certificate.


# Client
# ======
# Generate a key to be used by a specific client, then generate a Certificate
# Signing Request from the key which will be used by the client and finally
# sign the client certificate with our CA key
#
# Client password is: gestalt
# The common name used on the client does not really matter.
#
echo "Generating client key..."
openssl genrsa -out client.key 4096

echo "Generating client certificate signing request (csr)..."
openssl req -new -key client.key -out client.csr \
        -subj "/C=AU/ST=SA/L=Adelaide/O=MyOrg/OU=MyOrgUnit/CN=127.0.0.1"

echo "Signing client csr using root CA certificate..."
openssl x509 -req \
    -CA ca.pem -CAkey ca.key -CAserial ./ca.srl -passin pass:'gestalt-ca' \
    -in client.csr \
    -out client.pem -days 365 \
    -addtrust clientAuth

# The most important part of the final command is the ``-addtrust clientAuth``
# part. The makes a certificate suitable for use with a client.


# Create another client cert to use in verification testing. This certificate
# will not be signed by our root CA.
echo "Generating client2 key..."
openssl genrsa -out client2.key 4096

echo "Creating and self-signing client2 certificate..."
openssl req -new -x509 -key client2.key -days 365 -out client2.pem \
    -subj "/C=AU/ST=SA/L=Adelaide/O=MyOrg/OU=MyOrgUnit/CN=client2"
