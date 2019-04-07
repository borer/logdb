package org.borer.logdb.bbtree;

/**
 * Index Page layout :
 * ------------------------------------------ 0
 * |                 Header                 |
 * ------------------------------------------ 8
 * |   Number of keys  |  Number of values  |
 * ------------------------------------------ 16
 * |                   Key1                 |
 * ------------------------------------------ 24
 * |                   Key2                 |
 * ------------------------------------------ 32
 * |                  ......                |
 * ------------------------------------------ number of keys * 8 = N
 * |                  ......                |
 * ------------------------------------------ 4072
 * |                   Value3               |
 * ------------------------------------------ 4080
 * |                   Value2               |
 * ------------------------------------------ 4088
 * |                   Value1               |
 * ------------------------------------------ 4096 (end of page)
 *
 * <p>
 * The keys are always 8 bytes.
 *
 * For leaf nodes the values are pointers in the leaf nodes to the page that has the element
 * For internal index nodes the values are pointer in the index file to the page that has the child
 * Values are always 8 bytes (long)
 * </p>
 *
 */
class BTreeNodePage
{
    static final int PAGE_START_OFFSET = 0;

    static final int PAGE_TYPE_OFFSET = 0;

    static final int PAGE_HEADER_OFFSET = PAGE_START_OFFSET;
    static final int PAGE_HEADER_SIZE = Long.BYTES;

    static final int NUMBER_OF_KEY_OFFSET = PAGE_HEADER_OFFSET + PAGE_HEADER_SIZE;
    static final int NUMBER_OF_KEY_SIZE = Integer.BYTES;

    static final int NUMBER_OF_VALUES_OFFSET = PAGE_START_OFFSET + PAGE_HEADER_SIZE + NUMBER_OF_KEY_SIZE;
    static final int NUMBER_OF_VALUES_SIZE = Integer.BYTES;

    static final int HEADER_SIZE_BYTES = PAGE_HEADER_SIZE + PAGE_HEADER_SIZE + NUMBER_OF_KEY_SIZE + NUMBER_OF_VALUES_SIZE;
    static final int KEY_START_OFFSET = HEADER_SIZE_BYTES;

    static final int KEY_SIZE = Long.BYTES;
    static final int VALUE_SIZE = Long.BYTES;
}
