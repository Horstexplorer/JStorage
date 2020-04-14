package de.netbeacon.jstorage.server.internal.usermanager.object;

import org.json.JSONObject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class UserTest {

    @Test
    void password() {
        User user = new User("testUser");
        assertFalse(user.verifyPassword(""));
        assertFalse(user.verifyPassword("somePassword"));
        user.setPassword("correctPassword");
        assertFalse(user.verifyPassword("wrongPassword"));
        assertFalse(user.verifyPassword("correctpassword"));
        assertTrue(user.verifyPassword("correctPassword"));
    }

    @Test
    void loginToken(){
        User user = new User("testUser");
        user.createNewLoginRandom();
        String validLoginToken = user.getLoginToken();
        assertFalse(user.verifyLoginToken("noteventhecorrectformat"));
        assertFalse(user.verifyLoginToken("correctformat.nobase64"));
        assertFalse(user.verifyLoginToken("Y29ycmVjdGZvcm1hdA==.YmFzZTY0ZW5jb2RlZA=="));
        assertTrue(user.verifyLoginToken(validLoginToken));
    }

    @Test
    void globalPermission() {
        User user = new User("testUser");
        assertFalse(user.hasGlobalPermission(GlobalPermission.Admin));
        user.addGlobalPermission(GlobalPermission.Admin, GlobalPermission.DBAdmin);
        assertTrue(user.hasGlobalPermission(GlobalPermission.Admin) && user.hasGlobalPermission(GlobalPermission.DBAdmin));
    }

    @Test
    void dependentPermission() {
        User user = new User("testUser");
        assertFalse(user.hasDependentPermission("testDataBase", DependentPermission.DBAdmin_Creator));
        user.addDependentPermission("testDataBase", DependentPermission.DBAdmin_Creator, DependentPermission.DBAccess_Modify);
        assertTrue(user.hasDependentPermission("testDataBase", DependentPermission.DBAdmin_Creator) && user.hasDependentPermission("testDataBase", DependentPermission.DBAccess_Modify));
    }

    @Test
    void export(){
        User user = new User("testUser");
        user.createNewLoginRandom();
        user.setPassword("somePassword");
        user.addGlobalPermission(GlobalPermission.Admin);
        user.addDependentPermission("testDataBase", DependentPermission.DBAccess_Modify);
        JSONObject export = user.export();
        assertTrue(export.has("loginRandom") && export.has("globalPermissions") && export.has("userName") && export.has("maxBucketSize") && export.has("passwordHash") && export.has("userID"));
        User user2 = new User("testUser2");
        JSONObject export2 = user2.export();
        assertTrue(export2.has("passwordHash") && export2.has("loginRandom")); // both values should be empty but existing

    }
}