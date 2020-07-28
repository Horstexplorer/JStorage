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

package de.netbeacon.jstorage.server.socket.notification;

import de.netbeacon.jstorage.server.internal.notificationmanager.NotificationManager;
import de.netbeacon.jstorage.server.internal.notificationmanager.objects.DataNotification;
import de.netbeacon.jstorage.server.internal.usermanager.UserManager;
import de.netbeacon.jstorage.server.internal.usermanager.object.DependentPermission;
import de.netbeacon.jstorage.server.internal.usermanager.object.GlobalPermission;
import de.netbeacon.jstorage.server.internal.usermanager.object.User;
import de.netbeacon.jstorage.server.socket.notification.object.NotificationListener;
import de.netbeacon.jstorage.server.tools.exceptions.GenericObjectException;
import de.netbeacon.jstorage.server.tools.exceptions.HTTPException;
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
import java.util.*;

/**
 * This class takes care of evaluating incoming requests from the notification socket
 *
 * @author horstexplorer
 */
public class NotificationSocketHandler implements Runnable{

    private final SSLSocket socket;
    private final String ip;
    private BufferedReader bufferedReader;
    private BufferedWriter bufferedWriter;

    private NotificationListener notificationListener;

    private static final int maxheadersize = 8; // 8kb

    private final Logger logger = LoggerFactory.getLogger(NotificationSocketHandler.class);

    protected NotificationSocketHandler(SSLSocket sslSocket){
        this.socket = sslSocket;
        this.ip = socket.getRemoteSocketAddress().toString().substring(1, socket.getRemoteSocketAddress().toString().indexOf(":"));
    }

    @Override
    public void run() {
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
                /*
                    HEADERS
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
                // check headers
                if(!headers.containsKey("requested-notification")){
                    throw new HTTPException(400);
                }
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
                }else{
                    throw new HTTPException(401);
                }
                // check for required permission
                if(!(user.hasGlobalPermission(GlobalPermission.Admin) || user.hasGlobalPermission(GlobalPermission.DBAdmin) || user.hasGlobalPermission(GlobalPermission.UseNotifications))){
                    throw new HTTPException(403);
                }

                // analyze client headers
                // requested-notification: db db:table db:table db:table
                HashMap<String, HashSet<String>> requestedNotifications = new HashMap<>();
                String reqnots = headers.get("requested-notification");
                String[] reqsar = reqnots.split("\\s");
                for(String s : reqsar){
                    String db = "";
                    String table = "";
                    if(s.contains(":")){
                        db = s.substring(0, s.indexOf(":")).toLowerCase();
                        table = s.substring(s.indexOf(":")+1).toLowerCase();
                    }else{
                        db = s;
                    }
                    // check permissions
                    if(!(user.hasDependentPermission(db, DependentPermission.DBAdmin_Creator) || user.hasDependentPermission(db, DependentPermission.DBAdmin_User) || user.hasDependentPermission(db, DependentPermission.DBAccess_Modify) || user.hasDependentPermission(db, DependentPermission.DBAccess_Read))){
                        throw new HTTPException(403);
                    }
                    // if it is fine
                    if(!requestedNotifications.containsKey(db)){
                        requestedNotifications.put(db, new HashSet<String>());
                    }
                    if(!table.isBlank()){
                        requestedNotifications.get(db).add(table);
                    }
                }

                // send ok
                sendLines("HTTP/1.1 200 OK");
                endHeaders();
                // register
                notificationListener = new NotificationListener(user, requestedNotifications);
                NotificationManager.getInstance().register(notificationListener, true);
                // listen to notifications
                while(true){
                    DataNotification notification = notificationListener.getNotification();
                    JSONObject jsonObject = notification.asJSON();
                    bufferedWriter.write(jsonObject.toString());
                    bufferedWriter.newLine();
                    bufferedWriter.flush();
                }

            }catch (HTTPException e){
                IPBanManager.getInstance().flagIP(ip); // may change later as not every exception should trigger ab ip flag
                logger.debug("Send Result: ", e);
                if(e.getAdditionalInformation() == null){
                    sendLines("HTTP/1.1 "+e.getStatusCode()+" "+e.getMessage());
                }else{
                    sendLines("HTTP/1.1 "+e.getStatusCode()+" "+e.getMessage(), "Additional-Information: "+e.getAdditionalInformation());
                }
                endHeaders(); // "server: I finished sending headers"
            }
        }catch (SSLException e){
            // handshake failed or something else we dont really need to know
            logger.debug("SSLException On Hello Socket: ", e);
        }catch (Exception e){
            // return 500
            try{sendLines("HTTP/1.1 500 Internal Server Error"); endHeaders();}catch (Exception ignore){}
            logger.error("Exception On Notification Socket: ", e);
        }finally {
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
     * @throws Exception
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
        try{
            if(notificationListener != null){
                NotificationManager.getInstance().register(notificationListener, false);
            }
        }catch (Exception ignore){}
        try{bufferedReader.close();}catch (Exception ignore){}
        try{bufferedWriter.close();}catch (Exception ignore){}
        try{socket.close();}catch (Exception ignore){}
    }
}
