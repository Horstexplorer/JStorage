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

package de.netbeacon.jstorage.server.socket.hello;

import de.netbeacon.jstorage.server.internal.usermanager.UserManager;
import de.netbeacon.jstorage.server.internal.usermanager.object.User;
import de.netbeacon.jstorage.server.socket.hello.processing.HelloProcessor;
import de.netbeacon.jstorage.server.socket.hello.processing.HelloProcessorResult;
import de.netbeacon.jstorage.server.tools.exceptions.GenericObjectException;
import de.netbeacon.jstorage.server.tools.exceptions.HTTPException;
import de.netbeacon.jstorage.server.tools.info.Info;
import de.netbeacon.jstorage.server.tools.ipban.IPBanManager;
import de.netbeacon.jstorage.server.tools.ratelimiter.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSocket;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * This class takes care of evaluating incoming requests from the hello socket
 *
 * @author horstexplorer
 */
public class HelloSocketHandler implements Runnable {

    private final SSLSocket socket;
    private BufferedReader bufferedReader;
    private BufferedWriter bufferedWriter;
    private final String ip;

    private static final int maxheadersize = 8; // 8kb
    private static final long timeoutms = 3000; // 3s
    private final AtomicBoolean canceled = new AtomicBoolean(false);
    private ScheduledFuture<?> timeoutTask;
    private static final ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor();

    private static final ConcurrentHashMap<String, RateLimiter> ipRateLimiter = new ConcurrentHashMap<>();

    private final Logger logger = LoggerFactory.getLogger(HelloSocketHandler.class);

    /**
     * Instantiates a new Hello socket handler.
     *
     * @param socket the socket
     */
    protected HelloSocketHandler(SSLSocket socket){
        this.socket = socket;
        logger.debug("Handling Connection From "+socket.getRemoteSocketAddress());
        this.ip = socket.getRemoteSocketAddress().toString().substring(1, socket.getRemoteSocketAddress().toString().indexOf(":"));
    }


    @Override
    public void run() {
        try{
            try{
                // start timeout task initially (to make sure the connection is not just opened for fun)
                startTimeoutTask(2500);
                // handshake
                socket.setEnabledCipherSuites(socket.getSupportedCipherSuites());
                socket.startHandshake();
                // get streams
                bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                bufferedWriter = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));

                // check ip for blacklist, eg update later
                if(IPBanManager.getInstance().isBanned(ip)){
                    IPBanManager.getInstance().extendBan(ip, 60*10); // increase ban by 10 minutes
                    throw new HTTPException(403);
                }

                /*

                    HEADERS AND DATA INPUT

                 */
                // (re)start timeout task; this will be used to wait for the header finishing to transmit
                startTimeoutTask(timeoutms);
                // get header (8kbit max, throw 413 else), get body if exists
                int b;
                ByteBuffer byteBuffer = ByteBuffer.allocate(1024*maxheadersize);
                try{
                    List<Integer> list = new ArrayList<>();
                    while((b = bufferedReader.read()) != 0 && bufferedReader.ready()){
                        if(b == 10 || b == 13){ list.add(b); } else{ list.clear(); }                                                                // tries to find the double \r\n (\r\n\r\n) combination indicating the beginning of the body.
                        if(list.size() == 4 && (list.get(0) == 13) && (list.get(1) == 10) && (list.get(2) == 13) && (list.get(3) == 10)){break;}    // this solution is hideous but for now it should work
                        byteBuffer.put((byte)b);
                    }
                }catch (BufferOverflowException e){
                    e.printStackTrace();
                    throw new HTTPException(413, "Header Exceeds Limit");
                }
                byteBuffer.flip();
                byte[] payload = new byte[byteBuffer.remaining()];
                byteBuffer.get(payload);
                HashMap<String, String> headers = new HashMap<>();
                Arrays.stream( new String(payload).split("\r\n")).forEach(s -> {
                    if(s.contains("HTTP")){
                        headers.put("http_method", s.substring(0,s.indexOf("/")).trim());
                        headers.put("http_url", s.substring(s.indexOf("/"), s.indexOf("HTTP")).trim());
                    }else{
                        if(s.contains(":")){
                            headers.put(s.substring(0, s.indexOf(":")).toLowerCase(), s.substring(s.indexOf(":")+1).trim());
                        }
                    }
                });
                // stop timeout
                stopTimeout();
                // analyse headers
                // check if all required headers exist
                if(!headers.containsKey("http_method") || !headers.containsKey("http_url")){
                    // invalid request, something broke
                    throw new HTTPException(400);
                }
                if(!(headers.get("http_method").equalsIgnoreCase("GET"))){
                    // invalid method
                    throw new HTTPException(405);
                }
                /*

                    USER AUTH & ACCESS

                 */

                // get user from logintoken
                User user = null;
                if(headers.containsKey("token")){
                    try{ user = UserManager.getInstance().getUserByLoginToken(headers.get("token")); }
                    catch (GenericObjectException e){
                        throw new HTTPException(403);
                    }
                    // verify token
                    if(!user.verifyLoginToken(headers.get("token"))){
                        throw new HTTPException(403);
                    }
                }

                // check ratelimit
                if(user != null && !user.allowProcessing()){
                    throw new HTTPException(429);
                }
                if(user == null){
                    if(!ipRateLimiter.containsKey(ip)){
                        RateLimiter rateLimiter = new RateLimiter(TimeUnit.MINUTES, 1);
                        rateLimiter.setMaxUsages(60);
                        ipRateLimiter.put(ip, rateLimiter);
                    }
                    if(!IPBanManager.getInstance().isWhitelisted(ip) && !ipRateLimiter.get(ip).takeNice()){
                        throw new HTTPException(429);
                    }
                }

                /*

                    PROCESS

                 */

                // prepare
                HelloProcessor helloProcessor = new HelloProcessor(user, headers.get("http_url"));
                // process
                helloProcessor.process();
                // get result
                HelloProcessorResult hpr = helloProcessor.getResult();

                /*

                    RESPONSE

                 */

                if(hpr == null){
                    throw new HTTPException(500); // should not happen, as processing should always return
                }
                // send
                sendLines("HTTP/1.1 "+hpr.getHTTPStatusCode()+" "+hpr.getHTTPStatusMessage(), "Server: JStorage_API/"+Info.VERSION);
                // server closes the connection
                sendLines("Connection: close");
                // send max & remaining bucket size + estimated refill time
                if(user != null){
                    sendLines("Ratelimit-Limit: "+user.getMaxBucket(),"Ratelimit-Remaining: "+user.getRemainingBucket(), "Ratelimit-Reset: "+user.getBucketRefillTime());
                }else{
                    sendLines("Ratelimit-Limit: "+ipRateLimiter.get(ip).getMaxUsages(),"Ratelimit-Remaining: "+ipRateLimiter.get(ip).getRemainingUsages(), "Ratelimit-Reset: "+ipRateLimiter.get(ip).getRefillTime());
                }
                // send data
                if(hpr.getBodyPage() != null){
                    sendLines("Content-Type: text/html", "Content-Length: "+hpr.getBodyPage().length());
                    endHeaders(); // spacer between header and data
                    sendData(hpr.getBodyPage());
                }else if(hpr.getBodyJSON() != null){
                    sendLines("Content-Type: application/json", "Content-Length: "+hpr.getBodyJSON().toString().length());
                    endHeaders(); // spacer between header and data
                    sendData(hpr.getBodyJSON().toString());
                }else{
                    endHeaders(); // "server: I finished sending headers"
                }
                logger.debug("Send Result: "+hpr.getHTTPStatusMessage());
                // done processing :3 *happy calculation noises*
            }catch (HTTPException e){
                IPBanManager.getInstance().flagIP(ip); // may change later as not every exception should trigger ab ip flag
                logger.debug("Send Result: ", e);
                if(e.getAdditionalInformation() == null){
                    sendLines("HTTP/1.1 "+e.getStatusCode()+" "+e.getMessage(), "Server: JStorage_API/"+Info.VERSION, "Connection: close");
                }else{
                    sendLines("HTTP/1.1 "+e.getStatusCode()+" "+e.getMessage(), "Server: JStorage_API/"+Info.VERSION, "Connection: close", "Additional-Information: "+e.getAdditionalInformation());
                }
                endHeaders(); // "server: I finished sending headers"
            }
        }catch (SSLException e){
            // handshake failed or something else we dont really need to know
            logger.debug("SSLException On Hello Socket: ", e);
        }catch (Exception e){
            // return 500
            try{sendLines("HTTP/1.1 500 Internal Server Error", "Server: JStorage_API/"+Info.VERSION, "Connection: close"); endHeaders();}catch (Exception ignore){}
            logger.error("Exception On Hello Socket: ", e);
        }finally {
            logger.debug("Finished Processing Of "+socket.getRemoteSocketAddress());
            close();
        }
    }

    /**
     * Used to send strings as different lines back to the client
     *
     * @param lines one or more strings
     * @throws Exception on socket/writer error
     */
    private void sendLines(String... lines) throws Exception{
        for(String s : lines){
            bufferedWriter.write(s+"\r\n");
        }
        bufferedWriter.flush();
    }

    /**
     * Used to send a new line after the headers had been sent
     * @throws Exception on exception
     */
    private void endHeaders() throws Exception{
        bufferedWriter.newLine();
        bufferedWriter.flush();
    }

    /**
     * Used to send data without adding line seperators
     *
     * @param data data as string
     * @throws Exception on socket/writer error
     */
    private void sendData(String data) throws Exception{
        bufferedWriter.write(data);
        bufferedWriter.flush();
    }

    /**
     * Used to close the connection
     */
    private void close(){
        try{bufferedReader.close();}catch (Exception ignore){}
        try{bufferedWriter.close();}catch (Exception ignore){}
        try{socket.close();}catch (Exception ignore){}
    }

    /**
     * Used to start a timeout for the connection
     * <br>
     * This will close the connection after a given time if not stopped
     * If the task is already running it will be canceled and restarted
     *
     * @param timeout timeout in ms
     */
    private void startTimeoutTask(long timeout){
        if(timeoutTask != null){
            timeoutTask.cancel(true);
        }
        timeoutTask = ses.schedule(()->{
            canceled.set(true);
            try {
                sendLines("HTTP/1.1 408 Request Timeout", "Server: JStorage_API/"+ Info.VERSION, "Connection: close");
                endHeaders();
            } catch (Exception ignore) {}
            close();
        }, timeout, TimeUnit.MILLISECONDS);
    }

    /**
     * Used to stop the timeout task
     */
    private void stopTimeout(){
        if(timeoutTask != null){
            timeoutTask.cancel(true);
        }
    }
}
