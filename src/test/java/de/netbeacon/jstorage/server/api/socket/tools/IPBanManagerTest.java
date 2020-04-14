package de.netbeacon.jstorage.server.api.socket.tools;

import de.netbeacon.jstorage.server.tools.ipban.IPBanManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.*;

class IPBanManagerTest {

    @BeforeEach
    void setUp() {
        try{
            new IPBanManager();
        }catch (Exception e){
            fail();
        }
    }

    @AfterEach
    void tearDown() {
        try{
            IPBanManager.shutdown();
            File f = new File("./jstorage/config/ipbanmanager");
            if(f.exists()){f.delete();}
        }catch (Exception e){fail();}
    }

    @Test
    void ipRecognition(){
        assertTrue(IPBanManager.isIP("1.1.1.1"));
        assertTrue(IPBanManager.isIP("1.123.1.12"));
        assertTrue(IPBanManager.isIP("1.0.0.0"));
        assertTrue(IPBanManager.isIP("255.255.255.255"));
        assertFalse(IPBanManager.isBanned("255.256.255.255"));
        assertFalse(IPBanManager.isBanned("0.1.1.1"));

        assertTrue(IPBanManager.isIP("1:0:0:0:0:0:0:0"));
        assertTrue(IPBanManager.isIP("fd6e:f518:a4bd:c840:ab34:c82a:923e:1ffa"));
        assertTrue(IPBanManager.isIP("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"));
        assertFalse(IPBanManager.isIP("ffff:ffff:ffff:ffff:ffgf:ffff:ffff:ffff"));
    }

    @Test
    void whitelist(){
        assertFalse(IPBanManager.isWhitelisted("1.1.1.1"));
        IPBanManager.addToWhitelist("1.1.1.1");
        assertTrue(IPBanManager.isWhitelisted("1.1.1.1"));
        assertFalse(IPBanManager.banIP("1.1.1.1"));
        assertFalse(IPBanManager.isBanned("1.1.1.1"));
    }

    @Test
    void flaglist(){
        assertEquals(-1, IPBanManager.isFlagged("1.1.1.1"));
        assertFalse(IPBanManager.isBanned("1.1.1.1"));
        assertTrue(IPBanManager.flagIP("1.1.1.1"));
        assertEquals(1, IPBanManager.isFlagged("1.1.1.1"));
        assertTrue(IPBanManager.flagIP("1.1.1.1"));
        assertTrue(IPBanManager.flagIP("1.1.1.1"));
        assertTrue(IPBanManager.flagIP("1.1.1.1"));
        assertTrue(IPBanManager.flagIP("1.1.1.1"));
        assertEquals(5, IPBanManager.isFlagged("1.1.1.1"));
        assertTrue(IPBanManager.isBanned("1.1.1.1"));
    }

    @Test
    void banlist(){
        assertFalse(IPBanManager.isBanned("1.1.1.1"));
        assertTrue(IPBanManager.banIP("1.1.1.1"));
        assertTrue(IPBanManager.isBanned("1.1.1.1"));
        assertTrue(IPBanManager.unbanIP("1.1.1.1"));
        assertFalse(IPBanManager.isBanned("1.1.1.1"));
    }
}