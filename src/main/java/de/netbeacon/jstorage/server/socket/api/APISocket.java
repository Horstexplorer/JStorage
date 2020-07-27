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

package de.netbeacon.jstorage.server.socket.api;

import de.netbeacon.jstorage.server.socket.api.processing.APIProcessor;
import de.netbeacon.jstorage.server.tools.executor.ScalingExecutor;
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
import java.io.OutputStreamWriter;
import java.net.SocketException;
import java.nio.file.Files;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
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

    private ScalingExecutor processing;

    private ExecutorService overload;
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
            thread = new Thread(this);
            thread.start();
        }
    }

    /**
     * Used to shutdown the web socket
     */
    public void shutdown(){
        if(running.get()){
            thread.interrupt();
            try{processing.shutdownNow();}catch (Exception ignore){}
            try{overload.shutdownNow();}catch (Exception ignore){}
            try{sslServerSocket.close();}catch (Exception ignore){}
            running.set(false);
            thread = null;
        }
    }

    @Override
    public void run(){
        if(!running.get()){
            running.set(true);

            int corePoolSize = 8;
            int maxPoolSize = 16;
            int scaleQueueSize = 2048;
            int keepAliveTime = 5;
            int port = 8888;
            String certPath = "./jstorage/cert/certificate.pem";
            String keyPath = "./jstorage/cert/key.pem";

            logger.info("Setting Up API Socket");
            try{
                File d = new File("./jstorage/config/");
                if(!d.exists()){ d.mkdirs(); }
                File f = new File("./jstorage/config/apisocket");
                if(!f.exists()){
                    JSONObject jsonObject = new JSONObject()
                            .put("port", port).put("certPath", certPath)
                            .put("keyPath", keyPath)
                            .put("corePoolSize", corePoolSize)
                            .put("maxPoolSize", maxPoolSize)
                            .put("scaleQueueSize", scaleQueueSize)
                            .put("keepAliveTime", keepAliveTime);
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
                        corePoolSize = Math.max(jsonObject.getInt("corePoolSize"), 1);
                        maxPoolSize = Math.max(jsonObject.getInt("maxPoolSize"), corePoolSize);
                        scaleQueueSize = Math.max(jsonObject.getInt("scaleQueueSize"), 1);
                        keepAliveTime = Math.max(jsonObject.getInt("keepAliveTime"), 1);
                    }
                }
                // start executors
                overload = Executors.newSingleThreadExecutor();
                processing = new ScalingExecutor(corePoolSize, maxPoolSize, scaleQueueSize, keepAliveTime, TimeUnit.SECONDS);
            }catch (Exception e){
                logger.error("Setting Up API Socket Failed. Trying To Continue", e);
            }
            logger.info("Starting API Socket");
            try{
                // load other dependencies & prepare processors
                APIProcessor.setupActions();

                // setup
                SSLContextFactory sslContextFactory = new SSLContextFactory();
                sslContextFactory.addCertificate("jstorage", certPath, keyPath);
                SSLContext sslContext = sslContextFactory.createSSLContext();
                logger.info("Loaded SSLContext: "+sslContext.getProvider().getInfo());
                sslServerSocket = (SSLServerSocket) sslContext.getServerSocketFactory().createServerSocket(port);
                logger.info("API Socket Running At: "+sslServerSocket.getInetAddress()+":"+sslServerSocket.getLocalPort());

                // running
                while(true){
                    SSLSocket sslSocket = (SSLSocket) sslServerSocket.accept();
                    logger.debug("Incoming API Connection: "+sslSocket.getRemoteSocketAddress());
                    try{
                        // processing
                        try{
                            processing.execute(new APISocketHandler(sslSocket));
                        }catch (RejectedExecutionException e){
                            logger.warn("Cannot Process Incoming Connection On API Socket - Too Busy: "+sslSocket.getReuseAddress()+" Increasing the queue size or number of processing threads might fix this. Ignore if this is the intended max.", e);
                            overload.execute(() -> {
                                try{
                                    BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(sslSocket.getOutputStream()));
                                    bufferedWriter.write("HTTP/1.1 503 Service Unavailable\r\n\r\n");
                                    bufferedWriter.flush();
                                    bufferedWriter.close();
                                    sslSocket.close();
                                }catch (Exception ignore){}
                            });
                        }
                    }catch (Exception e){
                        logger.debug("Error For Incoming Connection On API Socket: "+sslSocket.getRemoteSocketAddress(), e);
                        try{sslSocket.close();}catch (Exception ignore){}
                    }
                }
            }catch(SocketException e){
                try{sslServerSocket.close();}catch (Exception ignore){}
                logger.debug("Stopped API Socket Due To Exception: ", e);
            } catch (Exception e){
                try{sslServerSocket.close();}catch (Exception ignore){}
                logger.error("Stopped API Socket Due To Exception: ", e);
            }finally {
                running.set(false);
            }
        }
    }


    /**
     * Returns the shared executor.
     * <br>
     * This should only be used by the SystemStats tool for read only operations.
     *
     * @return ScalingExecutor
     */
    public ScalingExecutor getExecutor(){
        return processing;
    }
}
