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

package de.netbeacon.jstorage.server.tools.exceptions;

/**
 * This exception should be thrown whenever some internal data storage should create exceptions
 *
 * @author horstexplorer
 */
public class DataStorageException extends Exception{

    /*

        Error Codes:

            000 Unknown Error

            1xx Load/Unload Error
                0x - Error Performing Action
                    1 - Load
                    2 - Unload
                1x - Current Action Running
                    0 - Unknown
                    1 - Load
                    2 - Unload
                20 - Timeout

            2xx Expectation Failed
                0x - Data Not Found
                    1 - DataSet
                    2 - DataShard
                    3 - DataTable
                    4 - DataBase
                    5 - CachedData
                    6 - Cache
                1x - Data Already Existing
                    1 - DataSet
                    2 - DataShard
                    3 - DataTable
                    4 - DataBase
                    5 - CachedData
                    6 - Cache
                2x - Mismatch
                    0 - Default (Database/Table/Cache)
                    1 - Structure
                3x - Object Not Ready
                    1 - Not Ready
                    2 - Shutdown Running
                4x - Validation
                    0 - Unknown
                    1 - Not Valid
                    2 - Not Expired

             300 Data Inconsistency Lock
             400 Unable To Modify

     */

    private final int type;
    private String additional = "";

    /**
     * Instantiates a new Data storage exception.
     *
     * @param type         the type
     * @param errorMessage the error message
     */
    public DataStorageException(int type, String errorMessage){
        super(errorMessage);
        this.type = type;
    }

    /**
     * Instantiates a new Data storage exception.
     *
     * @param type         the type
     * @param errorMessage the error message
     * @param additional   the additional
     */
    public DataStorageException(int type, String errorMessage, String additional){
        super(errorMessage);
        this.type = type;
        this.additional = additional;
    }

    /**
     * Get type int.
     *
     * @return the int
     */
    public int getType(){ return type; }

    /**
     * Get additional string.
     *
     * @return the string
     */
    public String getAdditional(){ return additional; }
}
