package org.borer.logdb;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class BtreeTest
{
    @Test
    void shouldAssertSetupIsWorking()
    {
        Btree btree = new Btree();
        assertTrue(btree != null);
    }
}