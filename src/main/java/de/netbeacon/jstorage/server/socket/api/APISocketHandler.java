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

import de.netbeacon.jstorage.server.internal.usermanager.UserManager;
import de.netbeacon.jstorage.server.internal.usermanager.object.User;
import de.netbeacon.jstorage.server.socket.api.processing.APIProcessor;
import de.netbeacon.jstorage.server.socket.api.processing.APIProcessorResult;
import de.netbeacon.jstorage.server.tools.exceptions.GenericObjectException;
import de.netbeacon.jstorage.server.tools.exceptions.HTTPException;
import de.netbeacon.jstorage.server.tools.info.Info;
import de.netbeacon.jstorage.server.tools.ipban.IPBanManager;
import org.json.JSONObject;
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
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * The type Api socket handler.
 *
 * @author horstexplorer
 */
public class APISocketHandler implements Runnable {

    private final SSLSocket socket;
    private BufferedReader bufferedReader;
    private BufferedWriter bufferedWriter;
    private final String ip;

    private static final int maxheadersize = 8; // 8kb
    private static final int maxbodysize = 8000; //8mb

    private final Logger logger = LoggerFactory.getLogger(APISocketHandler.class);

    /**
     * Instantiates a new Api socket handler.
     *
     * @param socket the socket
     */
    protected APISocketHandler(SSLSocket socket){
        this.socket = socket;
        logger.debug("Handling Connection From "+socket.getRemoteSocketAddress());
        this.ip = socket.getRemoteSocketAddress().toString().substring(1, socket.getRemoteSocketAddress().toString().indexOf(":"));
    }


    /*
                Note on how such header should look like

                GET /some/path HTTP/1.1
                Host: 127.0.0.1:8888
                Token: <authtoken>
                Content-Type: application/json
                Content-Length: 123

                // body should contains 123 char json

    */

    @Override
    public void run(){
        try{
            try{
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

                boolean keepAlive = false;
                do{

                    /*

                    HEADERS AND DATA INPUT

                 */

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
                    // analyse headers
                    // check if all required headers exist
                    if(!headers.containsKey("http_method") || !headers.containsKey("http_url")){
                        // invalid request, something broke
                        throw new HTTPException(400);
                    }
                    if(headers.containsKey("connection")){
                        if(headers.get("connection").toLowerCase().contains("keep-alive")){
                            keepAlive = true;
                        }else if(headers.get("connection").toLowerCase().contains("close")){
                            keepAlive = false;
                        }
                    }
                    if(!(headers.get("http_method").equalsIgnoreCase("GET") || headers.get("http_method").equalsIgnoreCase("PUT") || headers.get("http_method").equalsIgnoreCase("POST") || headers.get("http_method").equalsIgnoreCase("DELETE"))){
                        // invalid method
                        throw new HTTPException(405);
                    }
                    if(!headers.containsKey("token") && !headers.containsKey("authorization")){
                        // unauthorized
                        throw new HTTPException(401);
                    }
                    // analyze method
                    JSONObject bodycontent = null;
                    if(headers.get("http_method").equalsIgnoreCase("PUT") || headers.get("http_method").equalsIgnoreCase("POST") || headers.get("http_method").equalsIgnoreCase("DELETE")){
                        // check if contains data
                        if(headers.containsKey("content-length") ^ headers.containsKey("content-type")){ // only partial headers (xor)
                            throw new HTTPException(400);
                        }else if(headers.containsKey("content-length") && headers.containsKey("content-type")){ // both
                            // check values
                            if(!(headers.get("content-type").equalsIgnoreCase("application/json") || headers.get("content-type").equalsIgnoreCase("application/json; charset=utf-8"))){
                                throw new HTTPException(406);
                            }
                            int clength = -1;
                            try{clength = Integer.parseInt(headers.get("content-length"));if(clength <= 0){throw new Exception();}
                            }catch (Exception e){throw new HTTPException(400);}
                            // get data
                            //bufferedReader.readLine(); // skip empty line // not needed as we use this line to determine that we stop reading the header
                            // start reading, max & 16MB
                            if(1024*maxbodysize >= clength){
                                ByteBuffer bodyBuffer = ByteBuffer.allocate(1024*maxbodysize);
                                try{
                                    int byt;
                                    while (clength > 0 && (byt = bufferedReader.read()) != -1){
                                        clength--;
                                        bodyBuffer.put((byte) byt);
                                    }
                                    bodyBuffer.flip();
                                    byte[] bodydata = new byte[bodyBuffer.remaining()];
                                    bodyBuffer.get(bodydata);
                                    bodycontent = new JSONObject(new String(bodydata));
                                }catch (BufferOverflowException e){ // should never be thrown
                                    throw new HTTPException(413, "Body/Payload Exceeds Limit");
                                }catch (Exception e){ // most likely failed to build the json
                                    throw new HTTPException(422);
                                }
                            }else{
                                throw new HTTPException(413, "Body/Payload Exceeds Limit"); // should be thrown instead of rethrow in BOE
                            }
                        }else{ // none
                            // not required as some data may be updated via header/request url
                            // may throw an exception in the future
                        }
                    }else{
                        // should not contain any body
                        if(headers.containsKey("content-length") || headers.containsKey("content-type")){
                            throw new HTTPException(409, "Should Not Contain Data");
                        }
                    }

                /*

                    USER AUTH & ACCESS

                 */

                    // get user from logintoken
                    User user;
                    int user_loginMode = -1; // as a user may have multiple requests at the same time - this is not shared inside the user object
                    if(headers.containsKey("token")){
                        try{ user = UserManager.getInstance().getUserByLoginToken(headers.get("token")); }
                        catch (GenericObjectException e){
                            throw new HTTPException(403);
                        }
                        // verify token
                        if(!user.verifyLoginToken(headers.get("token"))){
                            throw new HTTPException(403);
                        }
                        // set value
                        user_loginMode = 1;
                    }else if(headers.containsKey("authorization")){
                        // parse the base64
                        String hcontent = headers.get("authorization");
                        if(!"basic".equalsIgnoreCase(hcontent.substring(0, hcontent.indexOf(" ")))){
                            throw new HTTPException(403);
                        }
                        String userid_pass = new String(Base64.getDecoder().decode(hcontent.substring(hcontent.indexOf(" ")+1).getBytes(StandardCharsets.UTF_8)));
                        try{ user = UserManager.getInstance().getUserByID(userid_pass.substring(0, userid_pass.indexOf(":"))); }
                        catch (GenericObjectException e){
                            throw new HTTPException(403);
                        }
                        // try to login using the password
                        if(!user.verifyPassword(userid_pass.substring(userid_pass.indexOf(":")+1))){
                            throw new HTTPException(403);
                        }
                        // set value
                        user_loginMode = 0;
                    }else{
                        // throw exception - this should not occur
                        throw new HTTPException(401);
                    }
                    // check ratelimit
                    if(!user.allowProcessing()){
                        throw new HTTPException(429);
                    }

                /*

                    PROCESS

                 */

                    // prepare
                    APIProcessor httpProcessor = new APIProcessor(user, user_loginMode, headers.get("http_method"), headers.get("http_url"), bodycontent);
                    // process
                    httpProcessor.process();
                    // get result
                    APIProcessorResult hpr = httpProcessor.getResult();

                /*

                    RESPONSE

                 */

                    if(hpr == null){
                        throw new HTTPException(500); // should not happen, as processing should always return
                    }
                    // send
                    sendLines("HTTP/1.1 "+hpr.getHTTPStatusCode()+" "+hpr.getHTTPStatusMessage(), "Server: JStorage_API/"+Info.VERSION);
                    // server closes the connection
                    if(keepAlive){
                        sendLines("Connection: keep-alive");
                    }else{
                        sendLines("Connection: close");
                    }
                    // add additional
                    if(hpr.getAdditionalInformation() != null){
                        sendLines("Additional-Information: "+hpr.getAdditionalInformation());
                    }
                    // add internal
                    if(hpr.getInternalStatus() != null){
                        sendLines("Internal-Status: "+hpr.getInternalStatus());
                    }
                    // send max & remaining bucket size + estimated refill time
                    sendLines("Ratelimit-Limit: "+user.getMaxBucket(),"Ratelimit-Remaining: "+user.getRemainingBucket(), "Ratelimit-Reset: "+user.getBucketRefillTime());
                    // send data
                    if(hpr.getResult() != null){
                        sendLines("Content-Type: application/json", "Content-Length: "+hpr.getResult().toString().length());
                        endHeaders(); // spacer between header and data
                        sendData(hpr.getResult().toString());
                    }else{
                        endHeaders(); // "server: I finished sending headers"
                    }
                    logger.debug("Sent Result: "+hpr.getHTTPStatusMessage()+ " "+((hpr.getResult() != null)? hpr.getResult().toString() : "empty"));
                    // done processing :3 *happy calculation noises*
                    logger.debug("Finished Processing Of "+socket.getRemoteSocketAddress());
                    if(!keepAlive){
                        close();
                    }
                }while (keepAlive);
            }catch (HTTPException e){ // should only be thrown if something went wrong during processing - but not related to issues of the processing itself
                IPBanManager.getInstance().flagIP(ip); // may change later as not every exception should trigger ab ip flag
                logger.debug("Sent Result: ", e);
                if(e.getAdditionalInformation() == null){
                    sendLines("HTTP/1.1 "+e.getStatusCode()+" "+e.getMessage(), "Server: JStorage_API/"+Info.VERSION);
                }else{
                    sendLines("HTTP/1.1 "+e.getStatusCode()+" "+e.getMessage(), "Server: JStorage_API/"+Info.VERSION, "Additional-Information: "+e.getAdditionalInformation());
                }
                endHeaders(); // "server: I finished sending headers"
                close();
            }
        }catch (SSLException e){
            // handshake failed or something else we dont really need to know
            logger.debug("SSLException On API Socket: ", e);
            close();
        }catch (Exception e){
            // return 500
            try{sendLines("HTTP/1.1 500 Internal Server Error", "Server: JStorage_API/"+Info.VERSION); endHeaders();}catch (Exception ignore){}
            logger.error("Exception On API Socket: ", e);
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
}
