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


package de.netbeacon.jstorage.server.api.socket.processing;

import de.netbeacon.jstorage.server.api.socket.processing.action.*;
import de.netbeacon.jstorage.server.internal.usermanager.object.User;
import de.netbeacon.jstorage.server.tools.exceptions.CryptException;
import de.netbeacon.jstorage.server.tools.exceptions.DataStorageException;
import de.netbeacon.jstorage.server.tools.exceptions.GenericObjectException;
import de.netbeacon.jstorage.server.tools.httpprocessing.HTTPProcessorHelper;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

/**
 * Used to unify http result processing
 *
 * @author horstexplorer
 */
public class HTTPProcessor {

    private final String processingId;
    private final String requestMethod;
    private final String requestURL;
    private final User user;
    private final int userLoginMode;
    private final JSONObject payload;
    private List<String> path;
    private HashMap<String, String> args;
    private final HTTPProcessorResult result = new HTTPProcessorResult();
    private boolean processed = false;

    private static final HashMap<String, Object> actions = new HashMap<>();

    private final Logger logger = LoggerFactory.getLogger(HTTPProcessor.class);

    /**
     * Sets up a new HTTPGetProcessor for the given input
     *
     * @param user          the user which requested processing
     * @param userLoginMode describes the mode the user used to authenticate
     * @param requestMethod the request method
     * @param requestURL    the request url
     * @param payload       data which might be send to the server within the body
     */
    public HTTPProcessor(User user, int userLoginMode, String requestMethod, String requestURL, JSONObject payload){
        this.user = user;
        this.userLoginMode = userLoginMode;
        this.requestMethod = requestMethod;
        this.requestURL = requestURL;
        this.payload = payload;
        this.processingId = UUID.randomUUID().toString();
        logger.debug("Created New HTTPProcessor For User "+user+" (Processing ID: "+processingId+")");
    }

    /**
     * Process.
     */
    public void process() {
        try{
            // parse urls
            path = HTTPProcessorHelper.getPath(requestURL); // path has to be in the /<action>?arg1=val&arg2=val format
            args = HTTPProcessorHelper.getArgs(requestURL);
            // check which action fits
            ProcessingAction actionbp = getAction(path);
            if(actionbp != null){
                // create new instance
                ProcessingAction action = actionbp.createNewInstance();
                // set data
                action.setup(user, result, args);
                // check if we have everything needed to process
                if(!action.supportedHTTPMethod(requestMethod.toLowerCase())) {
                    result.setHTTPStatusCode(405);
                    result.addAdditionalInformation("Invalid Method");
                    return;
                }
                if(!args.keySet().containsAll(action.requiredArguments())) {
                    result.setHTTPStatusCode(400);
                    result.addAdditionalInformation("Invalid Arguments");
                    return;
                }
                if((action.requiresData() && (payload == null))) {
                    result.setHTTPStatusCode(400);
                    result.addAdditionalInformation("Data Required");
                    return;
                }else{
                    action.setPayload(payload); // might be null - required to allow optional payloads
                }
                if(!action.loginModeIsSupported(userLoginMode)) {
                    result.setHTTPStatusCode(403);
                    result.addAdditionalInformation("Invalid Login Method");
                    return;
                }
                if(!action.userHasPermission()) {
                    result.setHTTPStatusCode(403);
                    result.addAdditionalInformation("Not Authorized");
                    return;
                }
                // process
                try{
                    action.process();
                    // set http ok - result has been set by processor
                    result.setHTTPStatusCode(200);
                }catch (DataStorageException e){
                    result.setInternalStatus("DataStorageException "+e.getType()+" "+e.getMessage());
                    switch (e.getType()){
                        case 101:
                        case 102:
                        case 110:
                        case 111:
                        case 112:
                        case 120:
                            result.setHTTPStatusCode(400);
                            break;
                        case 201:
                        case 202:
                        case 203:
                        case 204:
                        case 205:
                        case 206:
                            result.setHTTPStatusCode(404);
                            break;
                        case 211:
                        case 212:
                        case 213:
                        case 214:
                        case 215:
                        case 216:
                        case 220:
                        case 221:
                        case 231:
                        case 232:
                        case 240:
                        case 241:
                        case 242:
                            result.setHTTPStatusCode(400);
                            break;
                        case 300:
                            result.setHTTPStatusCode(423);
                            break;
                        case 400:
                            result.setHTTPStatusCode(400);
                            break;
                        case 0:
                        default:
                            result.setHTTPStatusCode(500);
                            break;
                    }
                }catch(GenericObjectException e){
                    result.setInternalStatus("GenericObjectException "+e.getType()+" "+e.getMessage());
                    switch (e.getType()){
                        case 100:
                        case 101:
                        case 102:
                            result.setHTTPStatusCode(400);
                            break;
                        case 200:
                            result.setHTTPStatusCode(404);
                            break;
                        case 300:
                        case 400:
                            result.setHTTPStatusCode(400);
                            break;
                        case 0:
                        default:
                            result.setHTTPStatusCode(500);
                            break;
                    }
                }catch (CryptException e){
                    result.setInternalStatus("CryptException "+e.getType()+" "+e.getMessage());
                    result.setHTTPStatusCode(500);
                }catch (NullPointerException e){
                    result.setInternalStatus("Somehow this occurred. Something is really broken");
                    result.setHTTPStatusCode(500);
                }
            }else{
                result.setHTTPStatusCode(400);
                result.addAdditionalInformation("Invalid Action / Path");
            }
        }catch (Exception e){
            result.setHTTPStatusCode(500);
            result.addAdditionalInformation(e.getMessage());
        }finally {
            processed = true;
            logger.debug("HTTPProcessor "+processingId+" Finished With Code "+result.getHTTPStatusCode());
        }
    }

    /**
     * Gets result.
     *
     * @return the result
     */
    public HTTPProcessorResult getResult() {
        if(processed){
            return result;
        }
        return null;
    }



    /**
     * Setup actions.
     */
    public static void setupActions(){
        if(actions.isEmpty()){

            // data actions
            addAction(Arrays.asList("data", "db"), new DataAction_CreateDataBase());
            addAction(Arrays.asList("data", "db", "table"), new DataAction_CreateDataTable());
            addAction(Arrays.asList("data", "db", "table", "dataset"), new DataAction_CreateDataSet());
            addAction(Arrays.asList("data", "db", "table", "dataset", "datatype"), new DataAction_CreateDataType());
            addAction(Arrays.asList("data", "db"), new DataAction_DeleteDataBase());
            addAction(Arrays.asList("data", "db", "table"), new DataAction_DeleteDataTable());
            addAction(Arrays.asList("data", "db", "table", "dataset"), new DataAction_DeleteDataSet());
            addAction(Arrays.asList("data", "db", "table", "dataset", "datatype"), new DataAction_DeleteDataType());
            addAction(Arrays.asList("data", "db"), new DataAction_DataBaseInfo());
            addAction(Arrays.asList("data", "db", "table"), new DataAction_DataTableInfo());
            addAction(Arrays.asList("data", "db", "table", "dataset"), new DataAction_DataSetInfo());
            addAction(Arrays.asList("data", "db"), new DataAction_DataBaseSettings());
            addAction(Arrays.asList("data", "db", "table"), new DataAction_DataTableSettings());
            addAction(Arrays.asList("data", "db", "table", "dataset"), new DataAction_DataSetSettings());
            addAction(Arrays.asList("data", "db", "table", "dataset"), new DataAction_GetDataSet());
            // cache actions
            addAction(Arrays.asList("cache"), new CacheAction_CacheSettings());
            addAction(Arrays.asList("cache"), new CacheAction_CacheInfo());
            addAction(Arrays.asList("cache", "mng"), new CacheAction_CreateCache());
            addAction(Arrays.asList("cache", "mng"), new CacheAction_ClearCache());
            addAction(Arrays.asList("cache", "mng"), new CacheAction_DeleteCache());
            addAction(Arrays.asList("cache", "access"), new CacheAction_GetCachedData());
            addAction(Arrays.asList("cache", "access"), new CacheAction_CreateCachedData());
            addAction(Arrays.asList("cache", "access"), new CacheAction_DeleteCachedData());
            // user actions
            addAction(Arrays.asList("user"), new UserAction_UserSettings());
            addAction(Arrays.asList("user"), new UserAction_UserInfo());
            addAction(Arrays.asList("user", "mng"), new UserAction_CreateUser());
            addAction(Arrays.asList("user", "mng"), new UserAction_DeleteUser());
            addAction(Arrays.asList("user", "mng"), new UserAction_UserChangePassword());
            addAction(Arrays.asList("user", "mng"), new UserAction_UserGetNewLoginToken());
            // other
            addAction(Arrays.asList("info", "basic"), new InfoAction_BasicInfo());
            addAction(Arrays.asList("info", "stats"), new InfoAction_Statistics());
        }
    }

    /**
     * Used to insert actions with a given path
     * <p>
     * Wont override existing objects
     * @param path path/to
     * @param action ProcessingAction
     */
    private static void addAction(List<String> path, ProcessingAction action){
        HashMap<String, Object> current = actions;
        for (String s : path) {
            if (!current.containsKey(s.toLowerCase())) {
                current.put(s.toLowerCase(), new HashMap<String, Object>());
            }
            if (current.get(s) instanceof HashMap) {
                current = (HashMap<String, Object>) current.get(s.toLowerCase());
            } else {
                return; // wont overwrite existing objects
            }
        }
        current.put(action.getAction().toLowerCase(), action); // add new action
    }

    /**
     * Used to get a specific action matching the path
     *
     * @param path containing/the/path/action
     * @return ProcessingAction or null
     */
    private ProcessingAction getAction(List<String> path){
        HashMap<String, Object> current = actions;
        for (int i = 0; i < path.size(); i++) {
            if(current.containsKey(path.get(i).toLowerCase())){
                if((i == path.size()-1)){ // last object in path
                    if(current.get(path.get(i).toLowerCase()) instanceof ProcessingAction){
                        return (ProcessingAction) current.get(path.get(i).toLowerCase());
                    }else{
                        return null;
                    }
                }else{
                    if(current.get(path.get(i).toLowerCase()) instanceof HashMap){
                        current = (HashMap<String, Object>) current.get(path.get(i).toLowerCase());
                    }else{
                        return null;
                    }
                }
            }else{
                return null;
            }
        }
        return null;
    }

}
