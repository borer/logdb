package org.logdb.bbtree;

import org.logdb.storage.ByteOffset;
import org.logdb.storage.StorageUnits;

/**
 * Index Page layout :
 * ------------------------------------------ 0
 * |                 Header                 |
 * ------------------------------------------ 40
 * |   Number of keys  |  Number of values  |
 * ------------------------------------------ 48
 * |                   Key1                 |
 * ------------------------------------------ 56
 * |                  Value1                |
 * ------------------------------------------ 64
 * |                   Key2                 |
 * ------------------------------------------ 72
 * |                  Value2                |
 * ------------------------------------------ 80
 * |                  ......                |
 * ------------------------------------------ number of keys * 8 = N
 * |                  ......                |
 * ------------------------------------------ 4064
 * |                  LogKey1               |
 * ------------------------------------------ 4072
 * |                 LogValue1              |
 * ------------------------------------------ 4080
 * |                  LogKey1               |
 * ------------------------------------------ 4088
 * |                 LogValue1               |
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

    static final @ByteOffset long PAGE_TYPE_OFFSET = StorageUnits.ZERO_OFFSET;
    static final @ByteOffset int PAGE_IS_ROOT_OFFSET = StorageUnits.offset(1);
    static final int PAGE_FLAGS_SIZE = Integer.BYTES;
    static final @ByteOffset int PAGE_CHECKSUM_OFFSET = StorageUnits.offset(PAGE_START_OFFSET + PAGE_FLAGS_SIZE);
    static final int PAGE_CHECKSUM_SIZE = Integer.BYTES;

    static final @ByteOffset int PAGE_LOG_KEY_VALUE_NUMBERS_OFFSET = StorageUnits.offset(PAGE_CHECKSUM_OFFSET + PAGE_CHECKSUM_SIZE);
    static final int PAGE_LOG_KEY_VALUE_NUMBERS_SIZE = Long.BYTES;
    static final @ByteOffset int PAGE_TIMESTAMP_OFFSET = StorageUnits.offset(PAGE_LOG_KEY_VALUE_NUMBERS_OFFSET + PAGE_LOG_KEY_VALUE_NUMBERS_SIZE);
    static final int PAGE_TIMESTAMP_SIZE = Long.BYTES;
    static final @ByteOffset int PAGE_VERSION_OFFSET = StorageUnits.offset(PAGE_TIMESTAMP_OFFSET + PAGE_TIMESTAMP_SIZE);
    static final int PAGE_VERSION_SIZE = Long.BYTES;
    static final @ByteOffset int PAGE_PREV_OFFSET = StorageUnits.offset(PAGE_VERSION_OFFSET + PAGE_VERSION_SIZE);
    static final int PAGE_PREV_SIZE = Long.BYTES;

    static final @ByteOffset int PAGE_HEADER_OFFSET = StorageUnits.offset(PAGE_START_OFFSET);
    static final int PAGE_HEADER_SIZE = PAGE_FLAGS_SIZE +
            PAGE_CHECKSUM_SIZE +
            PAGE_LOG_KEY_VALUE_NUMBERS_SIZE +
            PAGE_TIMESTAMP_SIZE +
            PAGE_VERSION_SIZE +
            PAGE_PREV_SIZE;

    static final @ByteOffset int NUMBER_OF_KEY_OFFSET = StorageUnits.offset(PAGE_HEADER_OFFSET + PAGE_HEADER_SIZE);
    static final int NUMBER_OF_KEY_SIZE = Integer.BYTES;

    static final @ByteOffset int NUMBER_OF_VALUES_OFFSET = StorageUnits.offset(PAGE_START_OFFSET + PAGE_HEADER_SIZE + NUMBER_OF_KEY_SIZE);
    static final int NUMBER_OF_VALUES_SIZE = Integer.BYTES;

    static final int HEADER_SIZE_BYTES = PAGE_HEADER_SIZE + NUMBER_OF_KEY_SIZE + NUMBER_OF_VALUES_SIZE;
    static final @ByteOffset int KEY_START_OFFSET = StorageUnits.offset(HEADER_SIZE_BYTES);

    static final int KEY_SIZE = Long.BYTES;
    static final int VALUE_SIZE = Long.BYTES;

    static final int MINIMUM_PAGE_SIZE = HEADER_SIZE_BYTES + (2 * (KEY_SIZE + VALUE_SIZE));
}
