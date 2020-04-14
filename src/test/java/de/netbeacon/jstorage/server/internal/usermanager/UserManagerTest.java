package de.netbeacon.jstorage.server.internal.usermanager;

import de.netbeacon.jstorage.server.internal.usermanager.object.User;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.*;

class UserManagerTest {

    @BeforeEach
    void setUp() {
        try{
            new UserManager();
        }catch (Exception e){
            fail();
        }
    }

    @AfterEach
    void tearDown() {
        try{
            UserManager.shutdown();
            File f = new File("./jstorage/config/usermanager");
            if(f.exists()){f.delete();}
        }catch (Exception e){
            fail();
        }
    }

    @Test
    void getUserByID() {
        try{
            String userid = UserManager.createUser("testUser").getUserID();
            User user = UserManager.getUserByID(userid);
            assertEquals(userid, user.getUserID());
        }catch (Exception e){
            fail();
        }
    }

    @Test
    void createUser() {
        try{
            String userid = UserManager.createUser("testUser").getUserID();
            assertTrue(UserManager.containsUser(userid));
            assertFalse(UserManager.containsUser("notevenanid"));
        }catch (Exception e){
            fail();
        }
    }

    @Test
    void deleteUser() {
        try{
            String userid = UserManager.createUser("testUser").getUserID();
            assertTrue(UserManager.containsUser(userid));
            UserManager.deleteUser(userid);
            assertFalse(UserManager.containsUser(userid));
        }catch (Exception e){
            fail();
        }
    }

    @Test
    void insertUser() {
        try{
            User user = new User("testUser");
            UserManager.insertUser(user);
            assertTrue(UserManager.containsUser(user.getUserID()));
        }catch (Exception e){
            e.printStackTrace();
            fail();
        }
    }

    @Test
    void shutdown() {
        try{
            String userID = UserManager.createUser("testUser").getUserID();
            UserManager.shutdown();
            new UserManager();
            assertTrue(UserManager.containsUser(userID));
        }catch (Exception e){
            e.printStackTrace();
            fail();
        }
    }
}