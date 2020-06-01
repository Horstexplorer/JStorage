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

package de.netbeacon.jstorage.server.tools.crypt;

import de.netbeacon.jstorage.server.tools.exceptions.CryptException;
import org.mindrot.jbcrypt.BCrypt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

/**
 * This class takes care of en and decryption of data
 */
public class JS2CryptTool {

    // <base64 encoded salt>.<base64 encoded & encrypted data>
    private static final Pattern js2encryptionpattern = Pattern.compile("^(?:[a-zA-Z0-9+\\/]{4})*(?:|(?:[a-zA-Z0-9+\\/]{3}=)|(?:[a-zA-Z0-9+\\/]{2}==)|(?:[a-zA-Z0-9+\\/]{1}===))\\.(?:[a-zA-Z0-9+\\/]{4})*(?:|(?:[a-zA-Z0-9+\\/]{3}=)|(?:[a-zA-Z0-9+\\/]{2}==)|(?:[a-zA-Z0-9+\\/]{1}===))$");
    private final AtomicInteger status = new AtomicInteger(-1); // -1 - shut down, 0 - ready, 1 not ready - awaiting password
    private String passwordHash;
    private String decryptionPassword;

    private final Logger logger = LoggerFactory.getLogger(JS2CryptTool.class);

    /**
     * Sets up a new tool
     */
    public JS2CryptTool(String configPath){
        setup(configPath);
    }


    /**
     * Used to insert the decryption password
     * <p>
     * If no password has been set before, this will set it
     * If a password has been set before this will verify the password
     *
     * @param password password
     * @throws CryptException on exceptions such as the tool already being ready or the password being invalid
     */
    public void js2encryptionPassword(String password) throws CryptException {
        if(status.get() == -1 || status.get() == 1){
            if(passwordHash == null || passwordHash.isEmpty()){
                // set new password
                passwordHash = BCrypt.hashpw(password, BCrypt.gensalt(32));
                logger.info("JS2Crypt Password Set");
            }else{
                // check password
                boolean accepted = BCrypt.checkpw(password, passwordHash);
                if(!accepted){
                    logger.error("JS2Crypt Password Not Accepted");
                    throw new CryptException(1, "Password Not Accepted");
                }
                logger.info("JS2Crypt Password Accepted");
            }
            this.decryptionPassword = password;
            status.set(0);
        }else{
            throw new CryptException(0, "Password Cant Be Changed In This State");
        }
    }

    /**
     * Used to get the current status of the tool
     * -1 - shut down
     *  0 - ready
     *  1 - not ready - awaiting password
     * @return int
     */
    public int getStatus(){
        return status.get();
    }

    /**
     * Used to check if this tool is ready
     * @return boolean
     */
    public boolean isReady(){
        return status.get() == 0;
    }

    /*                      TEST                        */

    /**
     * Can be used to test if the data is in the correct format
     * <p>
     * <base64 encoded salt>.<base64 encoded + encrypted data>
     *
     * @param test data to test
     * @return boolean
     */
    public static boolean isJS2Encrypted(String test){
        return js2encryptionpattern.matcher(test).matches();
    }

    /*                      CRYPT                       */

    /**
     * Used to decode js2 encrypted data
     *
     * @param js2EncryptedData data as string
     * @return byte[] decrypted data
     * @throws CryptException on exceptions
     */
    public byte[] decode(String js2EncryptedData) throws CryptException{
        if(status.get() != 0){
            throw new CryptException(0, "CryptTool Is Not Ready");
        }
        if(!isJS2Encrypted(js2EncryptedData)){
            throw new CryptException(21, "Data Does Not Match JS2Encryption");
        }
        // get the salt
        String base64Salt = js2EncryptedData.substring(0, js2EncryptedData.indexOf('.'));
        byte[] salt = Base64.decode(base64Salt.getBytes());
        // get the data
        String base64Data = js2EncryptedData.substring(js2EncryptedData.indexOf('.')+1);
        byte[] data = Base64.decode(base64Data.getBytes());
        // try to decrypt
        byte[] decryptedData;
        try{
            decryptedData = Crypt.decrypt(data, decryptionPassword, salt);
        }catch (Exception e){
            logger.error("Something Went Wrong While Decrypting The Data (Hash: "+js2EncryptedData.hashCode()+")", e);
            throw new CryptException(20,"Something Went Wrong While Decrypting The Data (Hash: "+js2EncryptedData.hashCode()+")");
        }
        return decryptedData;
    }

    /**
     * Used to encode data to js2 format
     *
     * @param data which should be encrypted
     * @return String of the js2 encrypted data
     * @throws CryptException on exceptions
     */
    public String encode(byte[] data) throws CryptException{
        if(status.get() != 0){
            throw new CryptException(0, "CryptTool Is Not Ready");
        }
        // generate a new salt
        byte[] salt = Crypt.genSalt();
        // encrypt data
        byte[] encryptedData;
        try{
            encryptedData = Crypt.encrypt(data, decryptionPassword, salt);
        }catch (Exception e){
            logger.error("Something Went Wrong While Encrypting The Data (Hash: "+ Arrays.hashCode(data) +")", e);
            throw new CryptException(10,"Something Went Wrong While Encrypting The Data (Hash: "+Arrays.hashCode(data)+")");
        }
        // encode with base 64
        String base64Salt = new String(Base64.encode(salt));
        String base64Data = new String(Base64.encode(encryptedData));
        // return in js2encryption format
        return base64Salt+'.'+base64Data;
    }

    /*                      Misc                        */

    private void setup(String configPath){

    }

    public void shutdown(){

    }
}
