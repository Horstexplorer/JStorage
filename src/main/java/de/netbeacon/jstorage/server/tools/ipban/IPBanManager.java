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

package de.netbeacon.jstorage.server.tools.ipban;

import de.netbeacon.jstorage.server.tools.exceptions.SetupException;
import de.netbeacon.jstorage.server.tools.exceptions.ShutdownException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

/**
 * Manages banned IPs
 *
 * Supports banned, flagged and whitelisted states  </br>
 *
 * @author horstexplorer
 */
public class IPBanManager {

    private static IPBanManager instance;

    private final AtomicBoolean ready = new AtomicBoolean(false);
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    private static final Pattern ipPattern = Pattern.compile("^(((([1]?\\d)?\\d|2[0-4]\\d|25[0-5])\\.){3}(([1]?\\d)?\\d|2[0-4]\\d|25[0-5]))|([\\da-fA-F]{1,4}(\\:[\\da-fA-F]{1,4}){7})|(([\\da-fA-F]{1,4}:){0,5}::([\\da-fA-F]{1,4}:){0,5}[\\da-fA-F]{1,4})$");
    private int banAfterFlags = 10;
    private final ConcurrentHashMap<String, IPBanObject> banlist = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicInteger> flaglist = new ConcurrentHashMap<>();
    private final HashSet<String> whitelist = new HashSet<>();

    private final ReentrantLock lock = new ReentrantLock();

    private static ScheduledExecutorService ses;
    private static ScheduledFuture<?> cleanTask;
    private static ScheduledFuture<?> flagTask;

    private final Logger logger = LoggerFactory.getLogger(IPBanManager.class);

    /**
     * Used to set up the banned ips
     */
    protected IPBanManager(){}

    /**
     * Used to get the instance of this class without forcing initialization
     *
     * @return IPBanManager
     */
    public static IPBanManager getInstance(){
        return getInstance(false);
    }

    /**
     * Used to get the instance of this class
     *
     * Can be used to initialize the class if this didnt happened yet </br>
     *
     * @param initializeIfNeeded boolean
     * @return IPBanManager
     */
    public static IPBanManager getInstance(boolean initializeIfNeeded){
        if(instance == null && initializeIfNeeded){
            instance = new IPBanManager();
        }
        return instance;
    }

    /*                  OBJECT                  */

    /**
     * Used to change the number of flags until an IP is getting banned
     *
     * If value <= 0 then it will reset to default (5) </br>
     *
     * @param value value
     */
    public void setBanAfterFlags(int value){
        if(value > 0){
            banAfterFlags = value;
        }else{
            banAfterFlags = 5;
        }
    }

    /**
     * Used to add an IP to the whitelist
     *
     * This will exclude an ip from getting flagged or banned </br>
     *
     * @param ip as String (something like aaa.bbb.ccc.ddd or IPv6 equivalent)
     * @return boolean, false if the ip is already whitelisted or is invalid
     */
    public boolean addToWhitelist(String ip){
        if(ipPattern.matcher(ip).matches()){
            if(!whitelist.contains(ip)){
                whitelist.add(ip);
                logger.debug("IP "+ip+" Added To Whitelist");
                return true;
            }
        }
        return false;
    }

    /**
     * Used to remove an IP from the whitelist
     *
     * @param ip as String (something like aaa.bbb.ccc.ddd or IPv6 equivalent)
     * @return boolean, false if the ip is not on the whitelist or is invalid
     */
    public boolean removeFromWhitelist(String ip){
        if(ipPattern.matcher(ip).matches()){
            if(whitelist.contains(ip)){
                whitelist.remove(ip);
                logger.debug("IP "+ip+" Removed From Whitelist");
                return true;
            }
        }
        return false;
    }

    /*                  TEST_TOOLS                   */

    /**
     * Used to check if the string is a valid IPv4 or v6
     *
     * This may only be used to check the pattern </br>
     *
     * @param ip as String (something like aaa.bbb.ccc.ddd or IPv6 equivalent)
     * @return boolean boolean
     */
    public boolean isIP(String ip){
        return ipPattern.matcher(ip).matches();
    }

    /**
     * Used to check if a certain IP is banned or not
     *
     * @param ip as String (something like aaa.bbb.ccc.ddd or IPv6 equivalent)
     * @return boolean, true if is banned
     */
    public boolean isBanned(String ip){
        if(ipPattern.matcher(ip).matches()){
            return banlist.containsKey(ip);
        }
        return false;
    }

    /**
     * Used to check if a certain IP is flagged or not
     *
     * Returns the number of flags an ip currently has. </br>
     * Will return -1 if not flagged or ip in the wrong format </br>
     *
     * @param ip as String (something like aaa.bbb.ccc.ddd or IPv6 equivalent)
     * @return int int
     */
    public int isFlagged(String ip){
        if(ipPattern.matcher(ip).matches()){
            if(flaglist.containsKey(ip)){
                return flaglist.get(ip).get();
            }
        }
        return -1;
    }

    /**
     * Used to check if a certain IP is whitelisted or not
     *
     * @param ip as String (something like aaa.bbb.ccc.ddd or IPv6 equivalent)
     * @return boolean boolean
     */
    public boolean isWhitelisted(String ip){
        if(ipPattern.matcher(ip).matches()){
            return whitelist.contains(ip);
        }
        return false;
    }

    /*                  BAN_TOOLS                   */

    /**
     * Used to remove an IP from the ban list
     *
     * Will return false if ip is not banned </br>
     *
     * @param ip as String (something like aaa.bbb.ccc.ddd or IPv6 equivalent)
     * @return boolean boolean
     */
    public boolean unbanIP(String ip){
        if(ipPattern.matcher(ip).matches()){
            if(banlist.containsKey(ip)){
               banlist.remove(ip);
               logger.debug("IP "+ip+" Unbanned");
               return true;
            }
        }
        return false;
    }

    /**
     * Used to add an IP to the ban list
     *
     * Returns false if ip is already banned </br>
     * Ignores Whitelisted IPs (returns false) </br>
     *
     * @param ip as String (something like aaa.bbb.ccc.ddd or IPv6 equivalent)
     * @return boolean boolean
     */
    public boolean banIP(String ip){
        if(ipPattern.matcher(ip).matches()){
            if(!banlist.containsKey(ip) && !whitelist.contains(ip)){
                try{
                    IPBanObject ipbo = new IPBanObject(ip);
                    banlist.put(ip, ipbo);
                    logger.debug("IP "+ip+" Banned");
                    return true;
                }catch (Exception ignore){}
            }
        }
        return false;
    }

    /**
     * Used to add an IP to the ban list. Will be removed after duration
     *
     * Returns false if ip is already banned </br>
     * Ignores Whitelisted IPs (returns false) </br>
     *
     * @param ip       as String (something like aaa.bbb.ccc.ddd or IPv6 equivalent)
     * @param duration in seconds
     * @return boolean boolean
     */
    public boolean banIP(String ip, long duration){
        if(ipPattern.matcher(ip).matches()){
            if(!banlist.containsKey(ip) && !whitelist.contains(ip)){
                try{
                    IPBanObject ipbo = new IPBanObject(ip, duration);
                    banlist.put(ip, ipbo);
                    logger.debug("IP "+ip+" Banned For "+duration+"s");
                    return true;
                }catch (Exception ignore){}
            }
        }
        return false;
    }

    /**
     * Used to extend the ban of an IP for a certain duration
     *
     * Returns false if ip is not banned or permanently banned </br>
     *
     * @param ip         as String (something like aaa.bbb.ccc.ddd or IPv6 equivalent)
     * @param additional in seconds
     * @return boolean boolean
     */
    public boolean extendBan(String ip, long additional){
        if(ipPattern.matcher(ip).matches()){
            if(banlist.containsKey(ip) && banlist.get(ip).getUntil() > -1L){
                banlist.get(ip).increaseBan(additional);
                logger.debug("IP "+ip+" Ban Increased By "+additional+"s");
                return true;
            }
        }
        return false;
    }

    /*                  FLAG_TOOLS                   */

    /**
     * Flag an IP for bad behaviour
     *
     * This will flag an IP for known bad behaviour. </br>
     * Flags will be removed by 1 every minute, more than x flags and the IP gets banned for an hour </br>
     * Ignores Whitelisted IPs (returns false) </br>
     *
     * @param ip as String (something like aaa.bbb.ccc.ddd or IPv6 equivalent)
     * @return boolean boolean
     */
    public boolean flagIP(String ip){
        if(ipPattern.matcher(ip).matches()){
            if(!whitelist.contains(ip)){
                if(!flaglist.containsKey(ip)){
                    flaglist.put(ip, new AtomicInteger(0));
                }
                flaglist.get(ip).set(flaglist.get(ip).get()+1);
                if(flaglist.get(ip).get() >= banAfterFlags){
                    return banIP(ip, 60*60); // ban for 60 minutes
                }
                logger.debug("IP "+ip+" Has Been Flagged");
                return true;
            }
        }
        return false;
    }

    /*                  MISC                    */

    /**
     * Used for initial setup of the IPBanManager
     *
     * @throws SetupException on exception
     */
    public void setup() throws SetupException {
        try{
            lock.lock();
            if(shutdown.get() | ready.get()){
                return;
            }
            // check dir
            File d = new File("./jstorage/config/");
            if(!d.exists()){ d.mkdirs(); }
            File f = new File("./jstorage/config/ipbanmanager");
            // load content from file
            if(!f.exists()){ f.createNewFile(); }
            else{
                try{
                    String content = new String(Files.readAllBytes(f.toPath()));
                    if(!content.isEmpty()) {
                        JSONObject jsonObject = new JSONObject(content);
                        // settings
                        setBanAfterFlags(jsonObject.getInt("banAfterFlags"));
                        // data
                        JSONArray jsonArray = jsonObject.getJSONArray("bannedIPs");
                        for (int i = 0; i < jsonArray.length(); i++) {
                            JSONObject ipbaj = jsonArray.getJSONObject(i);
                            try {
                                String ip = ipbaj.getString("ip");
                                if (ipPattern.matcher(ip).matches()) {
                                    if (ipbaj.has("until")) {
                                        IPBanObject ipBanObject = new IPBanObject(ip, ipbaj.getLong("until"));
                                        banlist.put(ip, ipBanObject);
                                    } else {
                                        IPBanObject ipBanObject = new IPBanObject(ip);
                                        banlist.put(ip, ipBanObject);
                                    }
                                }
                            } catch (Exception e) {
                                logger.error("Error Adding IP To Ban List During Setup For Object "+i, e);
                            }
                        }
                        JSONArray jsonArray2 = jsonObject.getJSONArray("flaggedIPs");
                        for (int i = 0; i < jsonArray2.length(); i++) {
                            JSONObject fip = jsonArray2.getJSONObject(i);
                            try {
                                String ip = fip.getString("ip");
                                if (ipPattern.matcher(ip).matches()) {
                                    int flags = fip.getInt("flags");
                                    if (flags > 0) {
                                        if (flags > banAfterFlags) {
                                            IPBanObject ipBanObject = new IPBanObject(ip, System.currentTimeMillis() + 3600000);
                                            banlist.put(ip, ipBanObject);
                                        } else {
                                            flaglist.put(ip, new AtomicInteger(flags));
                                        }
                                    }
                                }
                            } catch (Exception e) {
                                logger.error("Error Adding IP To Flag List During Setup For Object "+i, e);
                            }
                        }
                        JSONArray jsonArray3 = jsonObject.getJSONArray("whitelistedIPs");
                        for (int i = 0; i < jsonArray3.length(); i++) {
                            String ip = jsonArray3.getString(i);
                            if (ipPattern.matcher(ip).matches()) {
                                whitelist.add(ip);
                            }
                        }
                    }
                }catch (Exception e){
                    logger.error("Error Processing File. Data May Be Lost", e);
                }
            }
            // create clean & flag task
            ses = Executors.newScheduledThreadPool(2);
            cleanTask = ses.scheduleAtFixedRate(() -> banlist.forEach((key, value)->{
                if(!value.isValid()){
                    banlist.remove(key); // should not throw an exception while iterating cuz this is a concurrenthashmap
                }
            }), 1, 1, TimeUnit.SECONDS);
            flagTask = ses.scheduleAtFixedRate(() -> flaglist.forEach((key, value)->{
                if(value.get()-1 > 0){
                   value.set(value.get()-1);
                }else{
                    flaglist.remove(key);
                }
            }), 1, 1, TimeUnit.MINUTES);
            logger.debug("Setup Successful");
        }catch (Exception e){
            logger.error("Setup Failed", e);
            throw new SetupException("IPBanManager: Setup Failed: "+e.getMessage());
        }finally {
            lock.unlock();
        }
    }

    /**
     * Used to store content on shutdown
     *
     * @throws ShutdownException on exception
     */
    public void shutdown() throws ShutdownException {
        if(ready.get()){
            try{
                shutdown.set(true);
                ready.set(false);
                // cancel tasks
                cleanTask.cancel(true);
                flagTask.cancel(true);
                // shutdown executor
                ses.shutdown();
                // export objects
                JSONObject jsonObject = new JSONObject()
                        .put("banAfterFlags", banAfterFlags);
                JSONArray jsonArray = new JSONArray();
                banlist.forEach((k, v)-> jsonArray.put(v.export()));
                JSONArray jsonArray2 = new JSONArray();
                flaglist.forEach((k,v)-> jsonArray2.put(new JSONObject().put("ip", k).put("flags", v)));
                JSONArray jsonArray3 = new JSONArray();
                whitelist.forEach(jsonArray3::put);
                jsonObject
                        .put("bannedIPs", jsonArray)
                        .put("flaggedIPs", jsonArray2)
                        .put("whitelistedIPs", jsonArray3);

                File d = new File("./jstorage/config/");
                if(!d.exists()){ d.mkdirs(); }
                File f = new File("./jstorage/config/ipbanmanager");
                if(!f.exists()){ f.createNewFile(); }
                BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(f));
                bufferedWriter.write(jsonObject.toString());
                bufferedWriter.newLine();
                bufferedWriter.flush();
                bufferedWriter.close();
                // clear
                banlist.clear();
                flaglist.clear();
                whitelist.clear();
                // reset
                shutdown.set(false);
                logger.debug("Shutdown Successful");
            }catch (Exception e){
                shutdown.set(false);
                logger.error("Shutdown Failed", e);
                throw new ShutdownException("IPBanManager: Shutdown Failed. Data May Be Lost: "+e.getMessage());
            }
        }
    }

    /**
     * Used for internal data management
     */
    private static class IPBanObject{
        private String ip;
        private long until = -1L;

        /**
         * Instantiates a new Ip ban object.
         *
         * @param ip the ip
         * @throws SetupException the setup exception
         */
        protected IPBanObject(String ip) throws SetupException {
            if(!ipPattern.matcher(ip).matches()){
                throw new SetupException("Invalid IP Format");
            }
        }

        /**
         * Instantiates a new Ip ban object.
         *
         * @param ip       the ip
         * @param duration the duration
         * @throws SetupException the setup exception
         */
        protected IPBanObject(String ip, long duration) throws SetupException {
            if(!ipPattern.matcher(ip).matches()){
                throw new SetupException("Invalid IP Format");
            }
            this.ip = ip;
            this.until = System.currentTimeMillis()+duration*1000;
        }

        /**
         * Get until long.
         *
         * @return the long
         */
        protected long getUntil(){ return until; }

        /**
         * Increase ban.
         *
         * @param duration the duration
         */
        protected void increaseBan(long duration){
            until += duration*1000;
        }

        /**
         * Is valid boolean.
         *
         * @return the boolean
         */
        protected boolean isValid(){
            if(until != -1L){
                return (until > System.currentTimeMillis());
            }
            return true;
        }

        /**
         * Export json object.
         *
         * @return the json object
         */
        protected JSONObject export(){
            return new JSONObject()
                    .put("ip", ip)
                    .put("until", until);
        }

    }
}
