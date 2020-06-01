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

package de.netbeacon.jstorage.server.internal.misc;

import de.netbeacon.jstorage.server.tools.crypt.JS2CryptTool;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.*;

public class JS2CryptTest {

    private JS2CryptTool js2CryptTool;

    @BeforeEach
    void setUp() {
        try{
            File f = new File("./jstorage/config/js2crypt");
            if(f.exists()){f.delete();}
            js2CryptTool = new JS2CryptTool("./jstorage/config/js2crypt", false);
            js2CryptTool.js2encryptionPassword("VerySave-MuchEncrypt-Wow");
        }catch (Exception e){
            e.printStackTrace();
            fail();
        }
    }

    @AfterEach
    void tearDown() {
        // js2CryptTool.shutdown(); dont as we dont need to store the password hash - we dont want it to run interactive the next time
        File f = new File("./jstorage/config/js2crypt");
        if(f.exists()){f.delete();}
    }

    @Test
    void js2crypt() {
        String testString = "This ?!Is\t 54A Test String 23With Spe__\\cial 123 C\\hars \nAnd Numbers 012";

        String testJsonString = new JSONObject().put("string", "string").put("int", 123).put("long", 9802L).toString();
        // encryp
        String testStringE = "";
        String testJsonStringE = "";
        try{
            testStringE = js2CryptTool.encode(testString.getBytes());
            testJsonStringE = js2CryptTool.encode(testJsonString.getBytes());
        }catch (Exception e){
            e.printStackTrace();
            fail();
        }
        // check js2encoding
        assertTrue(JS2CryptTool.isJS2Encrypted(testStringE));
        assertTrue(JS2CryptTool.isJS2Encrypted(testJsonStringE));
        // decrypt
        String testStringD = "";
        String testJsonStringD = "";
        try{
            testStringD = new String(js2CryptTool.decode(testStringE));
            testJsonStringD = new String(js2CryptTool.decode(testJsonStringE));
        }catch (Exception e){
            fail();
        }
        // compare
        assertEquals(testString, testStringD);
        assertEquals(testJsonString, testJsonStringD);
    }
}
