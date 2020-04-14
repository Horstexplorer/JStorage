package de.netbeacon.jstorage.server.internal.usermanager.object;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class EnumTest {

    @Test
    void getByValue() {
        assertEquals(GlobalPermission.Admin, GlobalPermission.getByValue("aDMin"));
        assertEquals(DependentPermission.DBAdmin_Creator, DependentPermission.getByValue("dbAdMin_cReAtOr"));
        assertNull(GlobalPermission.getByValue("undefinedEnum"));
        assertNull(DependentPermission.getByValue("undefinedEnum"));
    }
}