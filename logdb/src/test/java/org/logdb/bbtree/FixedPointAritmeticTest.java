package org.logdb.bbtree;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.logdb.bbtree.FixedPointAritmetic.getPercentageOfQuantity;
import static org.logdb.bbtree.FixedPointAritmetic.isQuantityBiggerThanPercentage;
import static org.logdb.bbtree.FixedPointAritmetic.isQuantitySmallerThanPercentage;

class FixedPointAritmeticTest
{
    @Test
    void shouldCalculateCorrectBiggerThanPercentage()
    {
        assertTrue(isQuantityBiggerThanPercentage(21, 100,  20));
        assertFalse(isQuantityBiggerThanPercentage(20, 100,  20));
        assertFalse(isQuantityBiggerThanPercentage(19, 100,  20));
    }

    @Test
    void shouldCalculateCorrectSmallerThanPercentage()
    {
        assertFalse(isQuantitySmallerThanPercentage(21, 100,  20));
        assertFalse(isQuantitySmallerThanPercentage(20, 100,  20));
        assertTrue(isQuantitySmallerThanPercentage(19, 100,  20));
    }

    @Test
    void shouldCalculateCorrectPercentage()
    {
        assertEquals(152, getPercentageOfQuantity( 152,  100));
        assertEquals(21, getPercentageOfQuantity( 100,  21));
        assertEquals(20, getPercentageOfQuantity( 100,  20));
        assertEquals(19, getPercentageOfQuantity( 100,  19));
    }
}