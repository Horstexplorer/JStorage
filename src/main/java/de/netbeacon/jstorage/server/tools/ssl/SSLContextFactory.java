/*
 *     Copyright 2020 Horstexplorer @ https://www.netbeacon.de
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.netbeacon.jstorage.server.tools.ssl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import java.io.ByteArrayInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;

/**
 * Used to create an SSLContext from certificates and keys
 *
 * @author horstexplorer
 */
public class SSLContextFactory {

    private KeyStore keyStore;
    private String keyStorePass;
    private final Logger logger = LoggerFactory.getLogger(SSLContextFactory.class);

    /**
     * Creates a new SSLContextFactory
     */
    public SSLContextFactory(){
        try{
            keyStore = KeyStore.getInstance("JKS");
            keyStore.load(null);

            SecureRandom random = new SecureRandom();
            keyStorePass = String.valueOf(random.nextLong());
        }catch (Exception e){
            logger.error("Failed To Create SSLContextFactory",e);
        }
    }

    /**
     * Used to add certificates and their private key
     * <p>
     * Requires an Base64 encoded X509 certificate and Base64 encoded RSA key
     *
     * @param alias            preferred alias for this certificate. Something like: some.domain.tld
     * @param pathToCert       path to the certificate. Something like ./dir/to/cert.pem
     * @param pathToPrivateKey path to the key. Something like: ./dir/to/key.pem
     * @return boolean success/failure
     */
    public boolean addCertificate(String alias, String pathToCert, String pathToPrivateKey){
        try{
            byte[] cert = Files.readAllBytes(Path.of(pathToCert));
            String certs = new String(cert);
            certs = certs.substring(certs.indexOf("-----BEGIN CERTIFICATE-----")+27, certs.indexOf("-----END CERTIFICATE-----"));
            certs = certs.replaceAll("\n", "");
            cert = Base64.getDecoder().decode(certs);
            byte[] key = Files.readAllBytes(Path.of(pathToPrivateKey));
            String keys = new String(key);
            keys = keys.substring(keys.indexOf("-----BEGIN PRIVATE KEY-----")+27, keys.indexOf("-----END PRIVATE KEY-----"));
            keys = keys.replaceAll("\n", "");
            key = Base64.getDecoder().decode(keys);

            CertificateFactory certFactory = CertificateFactory.getInstance("X.509");
            X509Certificate certificate = (X509Certificate)certFactory.generateCertificate(new ByteArrayInputStream(cert));

            KeyFactory keyFactory = KeyFactory.getInstance("RSA");
            PrivateKey privateKey = keyFactory.generatePrivate(new PKCS8EncodedKeySpec(key));

            keyStore.setCertificateEntry(alias+"_cert", certificate);
            keyStore.setKeyEntry(alias+"_key", privateKey, keyStorePass.toCharArray(), new Certificate[] {certificate});

            logger.debug("Successfully Add Certificate With Alias "+alias);
            return true;
        }catch (Exception e){
            logger.error("Failed To Add Certificate With Alias "+alias);
            return false;
        }
    }

    /**
     * Used to recive an SSLContext from the keystore
     *
     * @return SSLContext ssl context
     */
    public SSLContext createSSLContext(){
        try{
            SSLContext context = SSLContext.getInstance("TLS");
            KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
            kmf.init(keyStore, keyStorePass.toCharArray());
            KeyManager[] km = kmf.getKeyManagers();

            TrustManagerFactory tmf = TrustManagerFactory.getInstance("X.509");
            tmf.init(keyStore);
            TrustManager[] tm = tmf.getTrustManagers();

            context.init(km, tm, null);

            logger.debug("Successfully Created SSLContext");
            return context;
        }catch (Exception e){
            logger.error("Failed To Create SSLContext", e);
            return null;
        }
    }
}
