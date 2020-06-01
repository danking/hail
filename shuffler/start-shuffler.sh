#!/bin/bash

set -ex

openssl pkcs12 -export \
        -inkey /ssl-config/shuffler-key.pem \
        -in /ssl-config/shuffler-cert.pem \
        -name shuffler-key-store \
        -out shuffler-key-store.p12 \
        -passout pass:dummypw

keytool -noprompt \
         -import \
         -alias incoming-cert \
         -file /ssl-config/shuffler-incoming.pem \
         -keystore shuffler-trust-store.jks \
         -storepass dummypw

java -cp '/spark-2.4.0-bin-hadoop2.7/jars/*:/hail.jar' is.hail.shuffler.server.ShuffleServer \
     shuffler-key-store.p12 \
     PKCS12 \
     hail \
     shuffler-trust-store.jks \
     JKS \
     hail \
     443
