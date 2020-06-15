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

package de.netbeacon.jstorage.server.api.socket;

import de.netbeacon.jstorage.server.api.socket.processing.HTTPProcessor;
import de.netbeacon.jstorage.server.tools.ipban.IPBanManager;
import de.netbeacon.jstorage.server.tools.ssl.SSLContextFactory;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Creates the socket for client connections
 * <p>
 * Supports SSL/TLS connections only due to security reasons
 *
 * @author horstexplorer
 */
public class APISocket implements Runnable {

    private static APISocket instance;

    private Thread thread;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private SSLServerSocket sslServerSocket;
    private final Logger logger = LoggerFactory.getLogger(APISocket.class);

    /**
     * Used to create an instance of this class
     */
    private APISocket(){}

    /**
     * Used to get the instance of this class without forcing initialization
     * @return APISocket
     */
    public static APISocket getInstance(){
        return getInstance(false);
    }

    /**
     * Used to get the instance of this class
     * <p>
     * Can be used to initialize the class if this didnt happened yet
     * @param initializeIfNeeded boolean
     * @return APISocket
     */
    public static APISocket getInstance(boolean initializeIfNeeded){
        if(instance == null && initializeIfNeeded){
            instance = new APISocket();
        }
        return instance;
    }

    /**
     * Used to start the web socket
     */
    public void start(){
        if(thread == null && !running.get()){
            thread = new Thread(new APISocket());
            thread.start();
        }
    }

    /**
     * Used to shutdown the web socket
     */
    public void shutdown(){
        if(running.get()){
            thread.interrupt();
            try{sslServerSocket.close();}catch (Exception ignore){}
            try{IPBanManager.shutdown();}catch (Exception ignore){}
            running.set(false);
            thread = null;
        }
    }

    @Override
    public void run(){
        if(!running.get()){
            running.set(true);

            int port = 8888;
            String certPath = "./jstorage/cert/certificate.pem";
            String keyPath = "./jstorage/cert/key.pem";

            logger.info("Setup");
            try{
                File d = new File("./jstorage/config/");
                if(!d.exists()){ d.mkdirs(); }
                File f = new File("./jstorage/config/apisocket");
                if(!f.exists()){
                    JSONObject jsonObject = new JSONObject().put("port", port).put("certPath", certPath).put("keyPath", keyPath);
                    BufferedWriter writer = new BufferedWriter(new FileWriter(f));
                    writer.write(jsonObject.toString());
                    writer.newLine();
                    writer.flush();
                    writer.close();
                }else{
                    String content = new String(Files.readAllBytes(f.toPath()));
                    if(!content.isEmpty()){
                        JSONObject jsonObject = new JSONObject(content);
                        port = jsonObject.getInt("port");
                        certPath = jsonObject.getString("certPath");
                        keyPath = jsonObject.getString("keyPath");
                    }
                }
            }catch (Exception e){
                logger.error("Setup Failed", e);
            }
            logger.info("Starting");
            try{
                // load other dependencies & prepare processors
                new IPBanManager();
                HTTPProcessor.setupActions();

                // setup
                SSLContextFactory sslContextFactory = new SSLContextFactory();
                sslContextFactory.addCertificate("jstorage", certPath, keyPath);
                SSLContext sslContext = sslContextFactory.createSSLContext();
                logger.info("Loaded SSLContext: "+sslContext.getProvider().getInfo());
                sslServerSocket = (SSLServerSocket) sslContext.getServerSocketFactory().createServerSocket(port);
                logger.info("Running At: "+sslServerSocket.getInetAddress()+":"+sslServerSocket.getLocalPort());

                // running
                while(true){
                    SSLSocket sslSocket = (SSLSocket) sslServerSocket.accept();
                    logger.info("Incoming Connection: "+sslSocket.getRemoteSocketAddress());
                    try{
                        // handshake
                        sslSocket.setEnabledCipherSuites(sslSocket.getSupportedCipherSuites());
                        sslSocket.startHandshake();
                        // processing
                        new APISocketHandler(sslSocket).start();

                    }catch (Exception e){
                        logger.error("Error For Incoming Connection: "+sslSocket.getRemoteSocketAddress(), e);
                        try{sslSocket.close();}catch (Exception ignore){}
                    }
                }
            }catch (Exception e){
                try{sslServerSocket.close();}catch (Exception ignore){}
                try{IPBanManager.shutdown();}catch (Exception ignore){}
            }finally {
                running.set(false);
            }
        }
    }
}
