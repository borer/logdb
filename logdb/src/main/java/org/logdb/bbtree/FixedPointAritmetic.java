package org.logdb.bbtree;

final class FixedPointAritmetic
{
    private FixedPointAritmetic()
    {

    }

    static boolean isQuantityBiggerThanPercentage(final long quantityA, final long quantityB, final int percentage)
    {
        assert percentage >= 0 && percentage <= 100 : "percentage has to be in range [0, 100]. Provided is " + percentage;

        return quantityA * 100 > quantityB * percentage;
    }

    static boolean isQuantitySmallerThanPercentage(final long quantityA, final long quantityB, final int percentage)
    {
        assert percentage >= 0 && percentage <= 100 : "percentage has to be in range [0, 100]. Provided is " + percentage;

        return quantityA * 100 < quantityB * percentage;
    }

    static long getPercentageOfQuantity(final long quantity, final int percentage)
    {
        assert percentage >= 0 && percentage <= 100 : "percentage has to be in range [0, 100]. Provided is " + percentage;

        return Math.abs((quantity * percentage) / 100);
    }
}
