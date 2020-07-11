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

package de.netbeacon.jstorage.server.hello.socket;

import de.netbeacon.jstorage.server.hello.socket.processing.HelloProcessor;
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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Creates the socket used for "ping" responses
 * This provides a message of what this is and provides easier access to logging and statistics
 *
 * @author horstexplorer
 */
public class HelloSocket implements Runnable{

    private static HelloSocket instance;

    private Thread thread;
    private final AtomicBoolean running = new AtomicBoolean(false);

    private BlockingQueue<Runnable> workQueue;
    private ThreadPoolExecutor processing;

    private ExecutorService overload;
    private SSLServerSocket sslServerSocket;
    private final Logger logger = LoggerFactory.getLogger(HelloSocket.class);

    /**
     * Used to create an instance of this class
     */
    private HelloSocket(){}

    /**
     * Used to get the instance of this class without forcing initialization
     * @return HelloSocket
     */
    public static HelloSocket getInstance(){
        return getInstance(false);
    }

    /**
     * Used to get the instance of this class
     * <p>
     * Can be used to initialize the class if this didnt happened yet
     * @param initializeIfNeeded boolean
     * @return HelloSocket
     */
    public static HelloSocket getInstance(boolean initializeIfNeeded){
        if(instance == null && initializeIfNeeded){
            instance = new HelloSocket();
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
            try{workQueue.clear();}catch (Exception ignore){}
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

            int corePoolSize = 2;
            int maxPoolSize = 4;
            int maxQueueSize = 128;
            int keepAliveTime = 5;
            int port = 443;
            boolean isActivated = true;
            String certPath = "./jstorage/cert/certificate.pem";
            String keyPath = "./jstorage/cert/key.pem";

            logger.info("Setting Up Hello Socket");
            try{
                File d = new File("./jstorage/config/");
                if(!d.exists()){ d.mkdirs(); }
                File f = new File("./jstorage/config/hellosocket");
                if(!f.exists()){
                    JSONObject jsonObject = new JSONObject()
                            .put("port", port).put("certPath", certPath)
                            .put("keyPath", keyPath)
                            .put("corePoolSize", corePoolSize)
                            .put("maxPoolSize", maxPoolSize)
                            .put("maxQueueSize", maxQueueSize)
                            .put("keepAliveTime", keepAliveTime)
                            .put("isActivated", isActivated);
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
                        maxQueueSize = Math.max(jsonObject.getInt("maxQueueSize"), 1);
                        keepAliveTime = Math.max(jsonObject.getInt("keepAliveTime"), 1);
                        isActivated = jsonObject.getBoolean("isActivated");
                    }
                }
                // start executors
                overload = Executors.newSingleThreadExecutor();
                workQueue = new ArrayBlockingQueue<>(maxQueueSize);
                processing = new ThreadPoolExecutor(corePoolSize, maxPoolSize, keepAliveTime, TimeUnit.SECONDS, workQueue);
            }catch (Exception e){
                logger.error("Setting Up Hello Socket Failed. Trying To Continue", e);
            }
            if(isActivated){
                logger.info("Starting Hello Socket");
                try{
                    // load other dependencies & prepare processors
                    HelloProcessor.setupActions();

                    // setup
                    SSLContextFactory sslContextFactory = new SSLContextFactory();
                    sslContextFactory.addCertificate("jstorage", certPath, keyPath);
                    SSLContext sslContext = sslContextFactory.createSSLContext();
                    logger.info("Loaded SSLContext: "+sslContext.getProvider().getInfo());
                    sslServerSocket = (SSLServerSocket) sslContext.getServerSocketFactory().createServerSocket(port);
                    logger.info("Hello Socket Running At: "+sslServerSocket.getInetAddress()+":"+sslServerSocket.getLocalPort());

                    // running
                    while(true){
                        SSLSocket sslSocket = (SSLSocket) sslServerSocket.accept();
                        logger.debug("Incoming Connection On Hello Socket: "+sslSocket.getRemoteSocketAddress());
                        try{
                            // processing
                            try{
                                processing.execute(new HelloSocketHandler(sslSocket));
                            }catch (RejectedExecutionException e){
                                logger.warn("Cannot Process Incoming Connection On Hello Socket - Too Busy: "+sslSocket.getReuseAddress()+" Increasing the queue size or number of processing threads might fix this. Ignore if this is the intended max.", e);
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
                            logger.debug("Error For Incoming Connection On Hello Socket : "+sslSocket.getRemoteSocketAddress(), e);
                            try{sslSocket.close();}catch (Exception ignore){}
                        }
                    }
                }catch(SocketException e){
                    try{sslServerSocket.close();}catch (Exception ignore){}
                    logger.debug("Stopped Hello Socket Due To Exception: ", e);
                } catch (Exception e){
                    try{sslServerSocket.close();}catch (Exception ignore){}
                    logger.error("Stopped Hello Socket Due To Exception: ", e);
                }finally {
                    running.set(false);
                }
            }else{
                logger.info("Hello Socket Is Deactivated");
            }
        }
    }

}
