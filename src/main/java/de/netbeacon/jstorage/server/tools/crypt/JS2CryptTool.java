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
import de.netbeacon.jstorage.server.tools.exceptions.SetupException;
import de.netbeacon.jstorage.server.tools.exceptions.ShutdownException;
import org.json.JSONObject;
import org.mindrot.jbcrypt.BCrypt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

/**
 * This class takes care of en and decryption of data
 */
public class JS2CryptTool {

    // <base64 encoded salt>.<base64 encoded & encrypted data>
    private static final Pattern js2encryptionpattern = Pattern.compile("^(?:[a-zA-Z0-9+\\/]{4})*(?:|(?:[a-zA-Z0-9+\\/]{3}=)|(?:[a-zA-Z0-9+\\/]{2}==)|(?:[a-zA-Z0-9+\\/]{1}===))\\.(?:[a-zA-Z0-9+\\/]{4})*(?:|(?:[a-zA-Z0-9+\\/]{3}=)|(?:[a-zA-Z0-9+\\/]{2}==)|(?:[a-zA-Z0-9+\\/]{1}===))$");
    private final AtomicBoolean ready = new AtomicBoolean(false);
    private String passwordHash;
    private String decryptionPassword;
    private final String configPath;

    private final Logger logger = LoggerFactory.getLogger(JS2CryptTool.class);

    /**
     * Sets up a new tool with the given config file
     *
     * @param configPath path to the config file; if this file does not have to exist (will be created)
     * @throws SetupException on setup throwing an exception
     */
    public JS2CryptTool(String configPath) throws SetupException {
        this.configPath = configPath;
        try{
            setup(configPath, false);
        }catch (CryptException e){
            throw new SetupException("Failed To Set Up JS2CryptTool: "+e.getMessage());
        }
    }

    /**
     * Sets up a new tool with the given config file
     *
     * @param setUpNow used to run initial setup; this only works if the config file is not empty
     * @param configPath path to the config file; if this file does not have to exist (will be created)
     * @throws SetupException on setup throwing an exception
     */
    public JS2CryptTool(String configPath, boolean setUpNow) throws SetupException{
        this.configPath = configPath;
        try{
            setup(configPath, setUpNow);
        }catch (CryptException e){
            throw new SetupException("Failed To Set Up JS2CryptTool: "+e.getMessage());
        }
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
        if(!ready.get()){
            if(passwordHash == null || passwordHash.isEmpty()){
                // set new password
                passwordHash = BCrypt.hashpw(password, BCrypt.gensalt(16));
                logger.debug("JS2Crypt Password Set");
            }else{
                // check password
                boolean accepted = BCrypt.checkpw(password, passwordHash);
                if(!accepted){
                    logger.error("JS2Crypt Password Not Accepted");
                    throw new CryptException(1, "Password Not Accepted");
                }
                logger.debug("JS2Crypt Password Accepted");
            }
            this.decryptionPassword = password;
            // ready only if the correct password has been inserted
            ready.set(true);
        }else{
            throw new CryptException(0, "Password Cant Be Changed In This State");
        }
    }

    /**
     * Used to check if this tool is ready
     * @return boolean
     */
    public boolean isReady(){
        return ready.get();
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
        if(!ready.get()){
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
        if(!ready.get()){
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

    private void setup(String configPath, boolean setupNow) throws CryptException{
        boolean requiresPassword = setupNow;
        try{
            File d = new File(this.configPath.substring(0, this.configPath.lastIndexOf("/")));
            if(!d.exists()){ d.mkdirs(); }
            File f = new File(this.configPath);
            if(!f.exists()){ f.createNewFile(); }
            else{
                // read
                String content = new String(Files.readAllBytes(f.toPath()));
                if(!content.isEmpty()){
                    JSONObject jsonObject = new JSONObject(content);
                    this.passwordHash = jsonObject.getString("passwordHash");
                }
                if(this.passwordHash != null &&!this.passwordHash.isEmpty()){
                    requiresPassword = true;
                }
            }
        }catch (Exception e){
            logger.error("An Error Occurred While Reading The Config File: "+configPath, e);
            throw new CryptException(2, "An Error Occurred While Reading The Config File: "+configPath+" : "+e.getMessage());
        }
        if(requiresPassword){
            // enable read from console
            int i = 0;
            Scanner sc = new Scanner(System.in);
            while(true){
                try{
                    logger.info("Insert JS2Crypt Password:");
                    js2encryptionPassword(sc.nextLine());
                    break;
                }catch (Exception e){
                    i++;
                    logger.error("Error Occurred While Inserting Password", e);
                    if(i >= 3){
                        break;
                    }
                }
            }
            if(i > 3){
                throw new CryptException(2, "Failed To Set Up Password");
            }
        }
    }

    public void shutdown() throws ShutdownException {
        ready.set(false);
        try{
            // build json
            JSONObject jsonObject = new JSONObject()
                    .put("passwordHash", (passwordHash != null)? passwordHash : "");
            // write to file
            File d = new File(this.configPath.substring(0, this.configPath.lastIndexOf("/")));
            if(!d.exists()){ d.mkdirs(); }
            File f = new File(this.configPath);
            BufferedWriter writer = new BufferedWriter(new FileWriter(f));
            writer.write(jsonObject.toString());
            writer.newLine();
            writer.flush();
            writer.close();
        }catch (Exception e){
            logger.error("Shutdown Failed. Data May Be Lost / Not Decipherable", e);
            throw new ShutdownException("Shutdown Failed. Data May Be Lost / Not Decipherable: "+e.getMessage());
        }
    }
}
