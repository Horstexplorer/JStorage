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

package de.netbeacon.jstorage.server.internal.notificationmanager.objects;

import de.netbeacon.jstorage.server.internal.usermanager.object.User;
import org.json.JSONObject;

/**
 * A helper class representing a notification
 *
 * @author horstexplorer
 */
public class DataNotification {

    public enum Content{
        heartbeat,
        created,
        updated,
        deleted
    }

    private final User user;
    private final String dataBase;
    private final String dataTable;
    private final String dataSet;
    private final String dataType;
    private final Content content;
    private final Long timestamp;

    /**
     * Used to create new instances of this class
     *
     * @param user the user causing this notification
     * @param dataBase the affected db (might be null)
     * @param dataTable the affected table (might be null)
     * @param dataSet the affected dataset (might be null)
     * @param dataType the affected datatype (might be null)
     * @param content what happened
     */
    public DataNotification(User user, String dataBase, String dataTable, String dataSet, String dataType, Content content){
        this.user = user;
        this.dataBase = (dataBase != null)?dataBase.toLowerCase():null;
        this.dataTable = (dataTable != null)?dataTable.toLowerCase():null;
        this.dataSet = (dataSet != null)?dataSet.toLowerCase():null;
        this.dataType = (dataType != null)?dataType.toLowerCase():null;
        this.content = content;
        this.timestamp = System.currentTimeMillis();
    }

    /**
     * Used to get the user causing this notification
     *
     * @return User
     */
    public User getOriginUser(){
        return user;
    }

    /**
     * Used to get the affected db
     *
     * @return String or null
     */
    public String getOriginDB(){
        return dataBase;
    }

    /**
     * Used to get the affected table
     *
     * @return String or null
     */
    public String getOriginTable(){
        return dataTable;
    }

    /**
     * Used to get the content of the notification
     *
     * @return Content
     */
    public Content getContent() {
        return content;
    }

    /**
     * Returns this notification as json with a timestamp
     *
     * @return JSONObject
     */
    public JSONObject asJSON(){
        JSONObject jsonObject = new JSONObject()
                .put("notification_type", content)
                .put("timestamp", timestamp);
        if(dataBase == null){
            return jsonObject;
        }
        jsonObject.put("database", dataBase);
        if(dataTable == null){
            return jsonObject;
        }
        jsonObject.put("table", dataTable);
        if(dataSet == null){
            return jsonObject;
        }
        jsonObject.put("dataset", dataSet);
        if(dataType == null){
            return jsonObject;
        }
        return jsonObject.put("datatype", dataType);
    }
}
