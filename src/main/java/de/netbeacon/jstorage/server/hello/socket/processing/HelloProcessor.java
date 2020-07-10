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

package de.netbeacon.jstorage.server.hello.socket.processing;

import de.netbeacon.jstorage.server.hello.socket.processing.action.DefaultHelloAction;
import de.netbeacon.jstorage.server.hello.socket.processing.action.HelloProcessingAction;
import de.netbeacon.jstorage.server.hello.socket.processing.action.HelloResponseAction;
import de.netbeacon.jstorage.server.internal.usermanager.object.User;
import de.netbeacon.jstorage.server.tools.httpprocessing.HTTPProcessorHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

public class HelloProcessor {
    private final String processingId;
    private final String requestMethod;
    private final String requestURL;
    private final User user;
    private List<String> path;
    private HashMap<String, String> args;
    private final HelloProcessorResult result = new HelloProcessorResult();
    private boolean processed = false;

    private static HelloProcessingAction defaultAction;
    private static final HashMap<String, Object> actions = new HashMap<>();

    private final Logger logger = LoggerFactory.getLogger(HelloProcessor.class);

    public HelloProcessor(User user, String requestMethod, String requestURL){
        this.user = user;
        this.requestMethod = requestMethod;
        this.requestURL = requestURL;
        this.processingId = UUID.randomUUID().toString();
        logger.debug("Created New HelloProcessor (Processing ID: "+processingId+")");
    }


    public void process(){
        try{
            // parse urls
            path = HTTPProcessorHelper.getPath(requestURL); // path has to be in the /<action>?arg1=val&arg2=val format
            args = HTTPProcessorHelper.getArgs(requestURL);
            // check which action fits
            HelloProcessingAction actionbp = getAction(path);
            if(actionbp != null){
                HelloProcessingAction action = actionbp.createNewInstance();
                // setup
                action.setup(user, result, args);
                // check auth
                if(action.requiresAuth() && user == null){
                    result.setHTTPStatusCode(401);
                    return;
                }
                // process
                action.process();
                result.setHTTPStatusCode(200);
            }else{
                result.setHTTPStatusCode(404);
            }
        }catch (Exception e){
            result.setHTTPStatusCode(500);
            logger.debug("Error Processing Request On Hello Socket: "+processingId+": ", e);
        }finally {
            processed = true;
            logger.debug("HelloProcessor "+processingId+" Finished With Code "+result.getHTTPStatusCode());
        }
    }

    /**
     * Gets result.
     *
     * @return the result
     */
    public HelloProcessorResult getResult() {
        if(processed){
            return result;
        }
        return null;
    }

    public static void setupActions(){
        addAction(Arrays.asList(), new DefaultHelloAction(), true);
        addAction(Arrays.asList(), new HelloResponseAction(), false);
    }

    /**
     * Used to insert actions with a given path
     * <p>
     * Wont override existing objects
     * @param path path/to
     * @param action HelloProcessingAction
     * @param isDefault set the action as default
     */
    private static void addAction(List<String> path, HelloProcessingAction action, boolean isDefault){
        if(isDefault){
            defaultAction = action;
            return;
        }
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
     * @return HelloProcessingAction
     */
    private HelloProcessingAction getAction(List<String> path){
        HashMap<String, Object> current = actions;
        for (int i = 0; i < path.size(); i++) {
            if(current.containsKey(path.get(i).toLowerCase())){
                if((i == path.size()-1)){ // last object in path
                    if(current.get(path.get(i).toLowerCase()) instanceof HelloProcessingAction){
                        return (HelloProcessingAction) current.get(path.get(i).toLowerCase());
                    }else{
                        return defaultAction;
                    }
                }else{
                    if(current.get(path.get(i).toLowerCase()) instanceof HashMap){
                        current = (HashMap<String, Object>) current.get(path.get(i).toLowerCase());
                    }else{
                        return defaultAction;
                    }
                }
            }else{
                return defaultAction;
            }
        }
        return defaultAction;
    }
}
