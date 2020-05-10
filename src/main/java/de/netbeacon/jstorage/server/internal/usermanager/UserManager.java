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

package de.netbeacon.jstorage.server.internal.usermanager;

import de.netbeacon.jstorage.server.internal.usermanager.object.DependentPermission;
import de.netbeacon.jstorage.server.internal.usermanager.object.GlobalPermission;
import de.netbeacon.jstorage.server.internal.usermanager.object.User;
import de.netbeacon.jstorage.server.tools.exceptions.GenericObjectException;
import de.netbeacon.jstorage.server.tools.exceptions.SetupException;
import de.netbeacon.jstorage.server.tools.exceptions.ShutdownException;
import org.apache.commons.lang3.RandomStringUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.util.Base64;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class takes care of preparing and accessing all User objects
 * <p>
 * The user should only access the objects through this class but should be able to create them outside of it
 *
 * @author horstexplorer
 */
public class UserManager {

    private static final AtomicBoolean ready = new AtomicBoolean(false);
    private static final AtomicBoolean shutdown = new AtomicBoolean(false);
    private static final ConcurrentHashMap<String, User> userPool = new ConcurrentHashMap<>();

    private static final Logger logger = LoggerFactory.getLogger(UserManager.class);

    /**
     * Used to set up all User objects
     *
     * @throws SetupException on setup() throwing an error
     */
    public UserManager() throws SetupException{
        if(!ready.get() && !shutdown.get()){
            setup();
            ready.set(true);
        }
    }

    /*                  DATA                    */

    /**
     * Used to get a User for a matching user id
     *
     * @param userID id of the target user
     * @return User user by id
     * @throws GenericObjectException on various errors such as the object not being found
     */
    public static User getUserByID(String userID) throws GenericObjectException {
        try{
            if(ready.get()){
                if(userPool.containsKey(userID)){
                    return userPool.get(userID);
                }
                logger.debug("User "+userID+" Not Found");
                throw new GenericObjectException(200, "UserManager: User Not Found");
            }
            logger.error("Not Ready Yet");
            throw new GenericObjectException(100, "UserManager: Not Ready Yet");
        }catch (GenericObjectException e){
            throw e;
        }
    }

    /**
     * Used to create a new User
     *
     * @param userName username
     * @return User on successful creation
     * @throws GenericObjectException on various errors such as the UserManager not being ready
     */
    public static User createUser(String userName) throws GenericObjectException {
        try{
            if(ready.get()){
                User user = new User(userName);
                userPool.put(user.getUserID(), user);
                return user;
            }
            logger.error("Not Ready Yet");
            throw new GenericObjectException(100, "UserManager: Not Ready Yet");
        }catch (GenericObjectException e){
            throw e;
        }
    }

    /**
     * Used to delete a user by his userID
     *
     * @param userID target userid
     * @throws GenericObjectException on various errors such as the UserManager not being ready
     */
    public static void deleteUser(String userID) throws GenericObjectException {
        try{
            if(ready.get()){
                if(userPool.containsKey(userID)){
                    User user = userPool.get(userID);
                    user.onUnload();
                    userPool.remove(user.getUserID());
                    return;
                }
                logger.debug("User "+userID+" Not Found");
                throw new GenericObjectException(200, "UserManager: User Not Found");
            }
            logger.error("Not Ready Yet");
            throw new GenericObjectException(100, "UserManager: Not Ready Yet");
        }catch (GenericObjectException e){
            throw e;
        }
    }

    /**
     * Used to check if the UserManager contains a specific user
     *
     * @param userID identifier of the user. See {@link User} for further information.
     * @return boolean boolean
     */
    public static boolean containsUser(String userID){
        return userPool.containsKey(userID);
    }

    /*                  ADVANCED                */

    /**
     * Used to insert a user to this UserManager
     *
     * @param user User object which should be inserted
     * @throws GenericObjectException on various errors such as the UserManager not being ready
     */
    public static void insertUser(User user) throws GenericObjectException {
        try{
            if(ready.get()){
                if(!userPool.containsKey(user.getUserID())){
                    userPool.put(user.getUserID(), user);
                    return;
                }
                logger.debug("User "+user.getUserID()+" Already Existing");
                throw new GenericObjectException(300, "UserManager: User Already Existing"); // should not occur as userIDs are unique
            }
            logger.error("Not Ready Yet");
            throw new GenericObjectException(100, "UserManager: Not Ready Yet");
        }catch (GenericObjectException e){
            throw e;
        }
    }

    /**
     * Used to get a User by its loginToken
     *
     * @param loginToken See {@link User#getLoginToken()}
     * @return User user by login token
     * @throws GenericObjectException on exception such as user not found
     */
    public static User getUserByLoginToken(String loginToken) throws GenericObjectException{
        String userID;
        try{
            userID = new String(Base64.getDecoder().decode(loginToken.substring(0, loginToken.indexOf(".")).getBytes()));
        }catch (Exception e){
            logger.debug("Invalid LoginToken Format");
            throw new GenericObjectException(400, "Invalid LoginToken Format: "+e);
        }
        return getUserByID(userID);
    }

    /*                  MISC                    */

    /**
     * Used for initial setup of the UserManager
     * <p>
     * Creates all listed User objects. See {@link User}
     *
     * @throws SetupException on error
     */
    private void setup() throws SetupException {
        if(!ready.get() && !shutdown.get()){
            try{
                File d = new File("./jstorage/config/");
                if(!d.exists()){ d.mkdirs(); }
                File f = new File("./jstorage/config/usermanager");
                if(!f.exists()){ f.createNewFile(); }
                else{
                    // load users from file
                    String content = new String(Files.readAllBytes(f.toPath()));
                    if(!content.isEmpty()){
                        JSONObject jsonObject = new JSONObject(content);
                        JSONArray users = jsonObject.getJSONArray("users");
                        for(int i = 0; i < users.length(); i++){
                            JSONObject userj = users.getJSONObject(i);
                            try{
                                String userID = userj.getString("userID");
                                String userName = userj.getString("userName");
                                String passwordHash = userj.getString("passwordHash");
                                String loginRandom = userj.getString("loginRandom");
                                int bucketsize = userj.getInt("maxBucketSize");
                                JSONArray globalPermissions = userj.getJSONArray("globalPermissions");
                                JSONArray dependentPermissions = userj.getJSONArray("dependentPermissions");
                                User user = new User(userID, userName, passwordHash, loginRandom, bucketsize);
                                // get permissions ready
                                for(int o = 0; o < globalPermissions.length() ; o++){
                                    try{
                                        GlobalPermission gp = GlobalPermission.getByValue(globalPermissions.getString(o));
                                        if(gp != null){
                                            user.addGlobalPermission(gp);
                                        }
                                    }catch (Exception e){
                                        logger.error("Loading GP For User "+user.getUserID()+" Failed During Setup", e);
                                    }
                                }
                                for(int o = 0; o < dependentPermissions.length() ; o++){
                                    JSONObject dpj = dependentPermissions.getJSONObject(o);
                                    try{
                                        String database = dpj.getString("database");
                                        JSONArray dpa = dpj.getJSONArray("permissions");
                                        for(int p = 0; p < dpa.length(); p++){
                                            DependentPermission dp = DependentPermission.getByValue(dpa.getString(p));
                                            if(dp != null){
                                                user.addDependentPermission(database, dp);
                                            }
                                        }
                                    }catch (Exception e){
                                        logger.error("Loading DP For User "+user.getUserID()+" Failed During Setup", e);
                                    }
                                }
                                userPool.put(user.getUserID(), user);
                            }catch (Exception e){
                                logger.error("Creating User From Object "+i+" Failed During Setup", e);
                            }
                        }
                    }
                }
                if(userPool.isEmpty()){
                    // add default admin user
                    User user = new User("admin");
                    user.addGlobalPermission(GlobalPermission.Admin);
                    String passwd = RandomStringUtils.randomAlphanumeric(16);
                    user.setPassword(passwd);
                    userPool.put(user.getUserID(), user);
                    logger.info("--------------------------------------------------------------------------------");
                    logger.info("UserManager: Created Admin User:");
                    logger.info("UserID: "+user.getUserID());
                    logger.info("Password: "+passwd);
                    logger.info("LoginToken: "+user.getLoginToken());
                    logger.info("--------------------------------------------------------------------------------");
                }
            }catch (Exception e){
                logger.error("Setup Failed", e);
                throw new SetupException("UserManager: Setup: Failed: "+e.getMessage());
            }
        }
    }

    /**
     * Used to store content on shutdown
     *
     * @throws ShutdownException on any error. This may result in data getting lost.
     */
    public static void shutdown() throws ShutdownException {
        shutdown.set(true);
        ready.set(false);
        try{
            // create json
            JSONObject jsonObject = new JSONObject();
            JSONArray users = new JSONArray();
            userPool.forEach((key, value) -> {
                users.put(value.export());
                value.onUnload();
            });
            jsonObject.put("users", users);
            // may contain further settings later
            // write to file
            File d = new File("./jstorage/config/");
            if(!d.exists()){ d.mkdirs(); }
            File f = new File("./jstorage/config/usermanager");
            BufferedWriter writer = new BufferedWriter(new FileWriter(f));
            writer.write(jsonObject.toString());
            writer.newLine();
            writer.flush();
            writer.close();
            // clear
            userPool.clear();
        }catch (Exception e){
            throw new ShutdownException("UserManager: Shutdown: Failed. Data May Be Lost: "+e.getMessage());
        }
        shutdown.set(false);
    }

    /*                      POOL                    */

    /**
     * Returns the internal storage object for all User objects
     *
     * @return ConcurrentHashMap<String, User> data pool
     */
    public static ConcurrentHashMap<String, User> getDataPool() {
        return userPool;
    }
}
