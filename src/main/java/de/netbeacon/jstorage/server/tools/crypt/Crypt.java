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

import org.bouncycastle.crypto.BufferedBlockCipher;
import org.bouncycastle.crypto.CipherParameters;
import org.bouncycastle.crypto.InvalidCipherTextException;
import org.bouncycastle.crypto.PBEParametersGenerator;
import org.bouncycastle.crypto.digests.SHA512Digest;
import org.bouncycastle.crypto.engines.AESEngine;
import org.bouncycastle.crypto.generators.PKCS12ParametersGenerator;
import org.bouncycastle.crypto.modes.CBCBlockCipher;
import org.bouncycastle.crypto.paddings.PaddedBufferedBlockCipher;
import org.bouncycastle.crypto.params.ParametersWithIV;

import java.security.SecureRandom;

/**
 * Used to en/decrypt bytes
 */
public class Crypt {

    /**
     * Used to encrypt the input with a given password.
     *
     * <p>
     * This calls {@link Crypt#encrypt(byte[], String, byte[])} with {@link Crypt#nullSalt()} as salt
     * @param bytes input bytes
     * @param password password
     * @return encrypted bytes
     * @throws InvalidCipherTextException on exception
     */
    public static byte[] encrypt(byte[] bytes, String password) throws InvalidCipherTextException {
        return encrypt(bytes, password, nullSalt());
    }

    /**
     * Used to encrypt the input with a given password and salt
     *
     * @param bytes input bytes
     * @param password password
     * @param salt salt
     * @return encrypted bytes
     * @throws InvalidCipherTextException on exception
     */
    public static byte[] encrypt(byte[] bytes, String password, byte[] salt) throws InvalidCipherTextException {
        ParametersWithIV key = (ParametersWithIV) getAESPassKey(password.toCharArray(), salt);
        BufferedBlockCipher cipher = new PaddedBufferedBlockCipher(new CBCBlockCipher(new AESEngine()));
        cipher.init(true, key);
        byte[] result = new byte[cipher.getOutputSize(bytes.length)];
        int len = cipher.processBytes(bytes, 0, bytes.length, result, 0);
        cipher.doFinal(result, len);
        return result;
    }

    /**
     * Used to decrypt the input with a given password.
     * <p>
     * This calls {@link Crypt#decrypt(byte[], String, byte[])} with {@link Crypt#nullSalt()} as salt
     * @param bytes input bytes
     * @param password password
     * @return encrypted bytes
     * @throws InvalidCipherTextException on exception
     */
    public static byte[] decrypt(byte[] bytes, String password) throws InvalidCipherTextException {
        return decrypt(bytes, password, nullSalt());
    }

    /**
     * Used to decrypt the input with a given password.
     *
     * @param bytes input bytes
     * @param password password
     * @param salt salt
     * @return encrypted bytes
     * @throws InvalidCipherTextException on exception
     */
    public static byte[] decrypt(byte[] bytes, String password, byte[] salt) throws InvalidCipherTextException {
        ParametersWithIV key = (ParametersWithIV) getAESPassKey(password.toCharArray(), salt);
        BufferedBlockCipher cipher = new PaddedBufferedBlockCipher(new CBCBlockCipher(new AESEngine()));
        cipher.init(false, key);
        byte[] result = new byte[cipher.getOutputSize(bytes.length)];
        int len = cipher.processBytes(bytes, 0, bytes.length, result, 0);
        cipher.doFinal(result, len);
        return result;
    }

    /**
     * Returns CipherParameters for the given input
     * @param passwd password (char array)
     * @param salt salt (byte array)
     * @return CipherParameters
     */
    private static CipherParameters getAESPassKey(char[] passwd, byte[] salt){
        PBEParametersGenerator generator = new PKCS12ParametersGenerator(new SHA512Digest());
        generator.init(
                PBEParametersGenerator.PKCS12PasswordToBytes(passwd), salt, 1024
        );
        return generator.generateDerivedParameters(128, 128);
    }

    /**
     * Provides default/empty salt
     * @return salt
     */
    private static byte[] nullSalt(){
        return new byte[16];
    }

    /**
     * Provides a random 16 byte salt
     * @return salt
     */
    public static byte[] genSalt(){
        byte[] bytes = new byte[16];
        new SecureRandom().nextBytes(bytes);
        return bytes;
    }
}