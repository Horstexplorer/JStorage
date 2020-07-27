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

    public DataNotification(User user, String dataBase, String dataTable, String dataSet, String dataType, Content content){
        this.user = user;
        this.dataBase = dataBase.toLowerCase();
        this.dataTable = dataTable.toLowerCase();
        this.dataSet = dataSet.toLowerCase();
        this.dataType = dataType.toLowerCase();
        this.content = content;
    }

    public User getOriginUser(){
        return user;
    }

    public String getOriginDB(){
        return dataBase;
    }

    public String getOriginTable(){
        return dataTable;
    }

    public JSONObject asJSON(){
        JSONObject jsonObject = new JSONObject()
                .put("notification_type", content)
                .put("timestamp", System.currentTimeMillis());
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
