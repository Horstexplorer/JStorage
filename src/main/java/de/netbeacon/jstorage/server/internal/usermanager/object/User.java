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

package de.netbeacon.jstorage.server.internal.usermanager.object;

import de.netbeacon.jstorage.server.tools.exceptions.SetupException;
import de.netbeacon.jstorage.server.tools.ratelimiter.RateLimiter;
import org.json.JSONArray;
import org.json.JSONObject;
import org.mindrot.jbcrypt.BCrypt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * This class represents a user
 *
 * @author horstexplorer
 */
public class User {

    private String userID;
    private final String userName;
    private String passwordHash;
    // permission
    private final HashSet<GlobalPermission> globalPermission = new HashSet<>();
    private final HashMap<String, HashSet<DependentPermission>> dependentPermission = new HashMap<>(); //<DataBase, HashSet<Permission>>
    // fast login
    private String loginRandom;
    private final RateLimiter rateLimiter;

    // statics
    private static final HashSet<String> occupiedIDs = new HashSet<>();

    private final Logger logger = LoggerFactory.getLogger(User.class);

    /**
     * Used to create a new User
     *
     * @param userName username
     */
    public User(String userName){
        this.userID = String.valueOf(Math.abs(new SecureRandom().nextLong()));
        while(occupiedIDs.contains(this.userID)){
            this.userID = String.valueOf(Math.abs(new SecureRandom().nextLong()));
        }
        occupiedIDs.add(this.userID);
        this.userName = userName.toLowerCase();

        rateLimiter = new RateLimiter(TimeUnit.MINUTES, 1);
        setMaxBucketSize(60);

        logger.debug("Created New User "+userID);
    }

    /**
     * Used to create a new User
     *
     * Only for internal used to restore users from file on startup </br>
     *
     * @param userID       userID
     * @param userName     userName
     * @param passwordHash passwordHash
     * @param loginRandom  loginRandom
     * @param maxBucket    max bucket size
     * @throws SetupException if userID already in use
     */
    public User(String userID, String userName, String passwordHash, String loginRandom, int maxBucket) throws SetupException {
        if(occupiedIDs.contains(userID)){
            throw new SetupException("UserID Already In Use");
        }
        this.userID = userID;
        occupiedIDs.add(this.userID);
        this.userName = userName.toLowerCase();
        this.passwordHash = passwordHash;
        this.loginRandom = loginRandom;

        rateLimiter = new RateLimiter(TimeUnit.MINUTES, 1);
        setMaxBucketSize(maxBucket);

        logger.debug("Created New User "+userID);
    }

    /*                  OBJECT                  */

    /**
     * Returns the user id
     *
     * @return String string
     */
    public String getUserID(){ return userID; }

    /**
     * Returns the user name
     *
     * @return String string
     */
    public String getUserName(){ return userName; }

    /*                  AUTH/ACCESS                */

    /**
     * Used to check if the password matches the stored hash
     *
     * @param password password
     * @return boolean boolean
     */
    public boolean verifyPassword(String password){
        if(passwordHash != null && !passwordHash.isEmpty()){
            return BCrypt.checkpw(password, passwordHash);
        }
        return false;
    }

    /**
     * Used to set the password
     *
     * @param password password
     */
    public void setPassword(String password){
        this.passwordHash = BCrypt.hashpw(password, BCrypt.gensalt(12));
    }

    /**
     * Returns the login token for this user
     *
     * Creates a new login token if none existed yet </br>
     *
     * @return String string
     */
    public String getLoginToken(){
        if(loginRandom == null || loginRandom.isEmpty()){
            createNewLoginRandom();
        }
        return new String(Base64.getEncoder().encode(userID.getBytes()))+"."+new String(Base64.getEncoder().encode(loginRandom.getBytes()));
    }

    /**
     * Used to create a new login token
     */
    public void createNewLoginRandom(){
        this.loginRandom = BCrypt.gensalt(8);
    }

    /**
     * Used to check if the login token matches this user
     *
     * @param loginToken logintoken
     * @return boolean boolean
     */
    public boolean verifyLoginToken(String loginToken){
        if(this.loginRandom != null && !this.loginRandom.isEmpty()){
            try{
                String userID = new String(Base64.getDecoder().decode(loginToken.substring(0, loginToken.indexOf(".")).getBytes()));
                String loginRandom = new String(Base64.getDecoder().decode(loginToken.substring(loginToken.indexOf(".")+1)));
                return (this.userID.equals(userID) && this.loginRandom.equals(loginRandom));
            }catch (Exception ignore){}
        }
        return false;
    }

    /**
     * Used to check if the user request is within his rate limit
     *
     * returns true on success - decreases the rate limit on each call </br>
     *
     * @return boolean boolean
     */
    public boolean allowProcessing(){
        return rateLimiter.takeNice();
    }

    /**
     * Used to set the maximum size for the request bucket
     *
     * size == 0 -> set to default of 240 </br>
     *
     * @param newSize new size
     */
    public void setMaxBucketSize(int newSize){
        rateLimiter.setMaxUsages(newSize);
    }

    /**
     * Used to get the currently remaining bucket size
     *
     * @return int int
     */
    public long getRemainingBucket(){
        return rateLimiter.getRemainingUsages();
    }

    /**
     * Used to get the maximum bucket size
     *
     * @return int int
     */
    public long getMaxBucket(){
        return rateLimiter.getMaxUsages();
    }

    /**
     * Used to get the timestamp when the bucket will be fully refilled
     *
     * @return long
     */
    public long getBucketRefillTime() {return rateLimiter.getRefillTime(); }

    /*                  ACCESS               */

    /**
     * Used to add global permission
     *
     * @param permission permissions
     */
    public void addGlobalPermission(GlobalPermission... permission){
        globalPermission.addAll(Arrays.asList(permission));
    }

    /**
     * Used to remove global permission
     *
     * @param permission permissions
     */
    public void removeGlobalPermission(GlobalPermission... permission){
        globalPermission.removeAll(Arrays.asList(permission));
    }

    /**
     * Used to check if the user has a specific permission
     *
     * @param permission permissions
     * @return the boolean
     */
    public boolean hasGlobalPermission(GlobalPermission permission){
        return globalPermission.contains(permission);
    }

    /**
     * Used to add dependent permissions from a specific database
     *
     * @param dependingOn name of the object
     * @param permissions permissions
     */
    public void addDependentPermission(String dependingOn, DependentPermission ... permissions){
        dependingOn = dependingOn.toLowerCase();
        if(!dependentPermission.containsKey(dependingOn)){
            dependentPermission.put(dependingOn, new HashSet<>());
        }
        dependentPermission.get(dependingOn).addAll(Arrays.asList(permissions));
    }

    /**
     * Used to remove dependent permissions from a specific object
     *
     * @param dependingOn name of the object
     * @param permissions permissions
     */
    public void removeDependentPermission(String dependingOn, DependentPermission ... permissions){
        dependingOn = dependingOn.toLowerCase();
        if(dependentPermission.containsKey(dependingOn)){
            dependentPermission.get(dependingOn).removeAll(Arrays.asList(permissions));
        }
        if(dependentPermission.get(dependingOn).isEmpty()){
            dependentPermission.remove(dependingOn);
        }
    }

    /**
     * Used to remove all dependent permissions for a specific object
     *
     * @param dependingOn name of the object, converted to lower case
     */
    public void removeDependentPermissions(String dependingOn){
        dependingOn = dependingOn.toLowerCase();
        if(dependentPermission.containsKey(dependingOn)){
            dependentPermission.get(dependingOn).clear();
            dependentPermission.remove(dependingOn);
        }
    }

    /**
     * Used to check if the user has a specific permission for the object
     *
     * @param dependingOn name of the object
     * @param permission  permission
     * @return boolean boolean
     */
    public boolean hasDependentPermission(String dependingOn, DependentPermission permission){
        dependingOn = dependingOn.toLowerCase();
        if(dependentPermission.containsKey(dependingOn)){
            return dependentPermission.get(dependingOn).contains(permission);
        }
        return false;
    }

    /*                  MISC                    */

    /**
     * Should be called when the user is unloaded or deleted to cancel the refill task and update the thread pool
     *
     * The userID of the current object will be available again - this function should only be used by the DataManager itself. </br>
     */
    public void onUnload(){
        occupiedIDs.remove(userID); // this may result in objects being created which are not unique
    }

    /**
     * Creates an JSONObject containing all information about the user
     *
     * @return JSONObject json object
     */
    public JSONObject export(){
        JSONObject jsonObject = new JSONObject()
                .put("userID", userID)
                .put("userName", userName)
                .put("passwordHash", (passwordHash == null ? "" : passwordHash))
                .put("loginRandom", (loginRandom == null ? "" : loginRandom))
                .put("maxBucketSize", rateLimiter.getMaxUsages());
        JSONArray globalPermissions = new JSONArray();
        for(GlobalPermission g : globalPermission){
            globalPermissions.put(g);
        }
        JSONArray dependentPermissions = new JSONArray();
        for(Map.Entry<String, HashSet<DependentPermission>> entry : dependentPermission.entrySet()){
            JSONObject jsonObject1 = new JSONObject()
                    .put("database", entry.getKey());
            JSONArray permissions = new JSONArray();
            for(DependentPermission p : entry.getValue()){
                permissions.put(p);
            }
            jsonObject1.put("permissions", permissions);
            dependentPermissions.put(jsonObject1);
        }
        return jsonObject
                .put("globalPermissions", globalPermissions)
                .put("dependentPermissions", dependentPermissions);
    }

    /**
     * Returns all global permissions
     *
     * @return HashSet<GlobalPermission> hash set
     */
    public HashSet<GlobalPermission> getGlobalPermissions(){
        return globalPermission;
    }

    /**
     * Returns all DependentPermission
     *
     * @return HashMap<String, HashSet < DependentPermission> >
     */
    public HashMap<String, HashSet<DependentPermission>> getDependentPermission(){
        return dependentPermission;
    }
}