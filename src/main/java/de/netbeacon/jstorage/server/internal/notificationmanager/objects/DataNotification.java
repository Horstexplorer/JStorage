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

import de.netbeacon.jstorage.server.internal.datamanager.objects.DataBase;
import de.netbeacon.jstorage.server.internal.datamanager.objects.DataSet;
import de.netbeacon.jstorage.server.internal.datamanager.objects.DataTable;
import org.json.JSONObject;

public class DataNotification {

    public enum Content{
        HeatBeat,
        Created,
        Updated,
        Deleted
    }

    private DataBase dataBase;
    private DataTable dataTable;
    private DataSet dataSet;
    private String dataType;
    private Content content;

    public String getOriginDB(){
        return dataBase.getIdentifier();
    }

    public String getOriginTable(){
        return dataTable.getIdentifier();
    }

    public String getOriginDataSet(){
        return dataSet.getIdentifier();
    }

    public JSONObject asJSON(){
        JSONObject jsonObject = new JSONObject().put("notification_type", content);
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
