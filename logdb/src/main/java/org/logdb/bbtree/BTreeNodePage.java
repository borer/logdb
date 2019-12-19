package org.logdb.bbtree;

import org.logdb.storage.ByteOffset;
import org.logdb.storage.ByteSize;
import org.logdb.storage.StorageUnits;

import static org.logdb.storage.StorageUnits.INT_BYTES_SIZE;
import static org.logdb.storage.StorageUnits.LONG_BYTES_SIZE;
import static org.logdb.storage.StorageUnits.SHORT_BYTES_SIZE;

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
    static final @ByteOffset int PAGE_START_OFFSET = StorageUnits.ZERO_OFFSET;

    static final @ByteOffset int PAGE_TYPE_OFFSET = PAGE_START_OFFSET;
    static final @ByteOffset int PAGE_IS_ROOT_OFFSET = StorageUnits.offset(1);
    static final @ByteSize int PAGE_FLAGS_SIZE = INT_BYTES_SIZE;

    static final @ByteOffset int PAGE_CHECKSUM_OFFSET = StorageUnits.offset(PAGE_START_OFFSET + PAGE_FLAGS_SIZE);
    static final @ByteSize int PAGE_CHECKSUM_SIZE = INT_BYTES_SIZE;

    static final @ByteOffset int PAGE_TIMESTAMP_OFFSET = StorageUnits.offset(PAGE_CHECKSUM_OFFSET + PAGE_CHECKSUM_SIZE);
    static final @ByteSize int PAGE_TIMESTAMP_SIZE = LONG_BYTES_SIZE;

    static final @ByteOffset int PAGE_VERSION_OFFSET = StorageUnits.offset(PAGE_TIMESTAMP_OFFSET + PAGE_TIMESTAMP_SIZE);
    static final @ByteSize int PAGE_VERSION_SIZE = LONG_BYTES_SIZE;

    static final @ByteOffset int PAGE_PREV_OFFSET = StorageUnits.offset(PAGE_VERSION_OFFSET + PAGE_VERSION_SIZE);
    static final @ByteSize int PAGE_PREV_SIZE = LONG_BYTES_SIZE;

    static final @ByteOffset int PAGE_HEADER_OFFSET = StorageUnits.offset(PAGE_START_OFFSET);
    static final @ByteSize int PAGE_HEADER_SIZE = PAGE_FLAGS_SIZE +
            PAGE_CHECKSUM_SIZE +
            PAGE_TIMESTAMP_SIZE +
            PAGE_VERSION_SIZE +
            PAGE_PREV_SIZE;

    static final @ByteOffset int NUMBER_OF_PAIRS_OFFSET = StorageUnits.offset(PAGE_HEADER_OFFSET + PAGE_HEADER_SIZE);
    static final @ByteSize int NUMBER_OF_PAIRS_SIZE = INT_BYTES_SIZE;

    static final @ByteOffset int TOP_KEY_VALUES_HEAP_SIZE_OFFSET = StorageUnits.offset(NUMBER_OF_PAIRS_OFFSET + NUMBER_OF_PAIRS_SIZE);
    static final @ByteSize int TOP_KEY_VALUES_HEAP_SIZE = SHORT_BYTES_SIZE;

    static final @ByteSize int HEADER_SIZE_BYTES = PAGE_HEADER_SIZE +
            NUMBER_OF_PAIRS_SIZE +
            TOP_KEY_VALUES_HEAP_SIZE;
    static final @ByteOffset int CELL_START_OFFSET = StorageUnits.offset(HEADER_SIZE_BYTES);

    static final @ByteSize int CELL_PAGE_OFFSET_SIZE = SHORT_BYTES_SIZE;
    static final @ByteSize int CELL_KEY_LENGTH_SIZE = SHORT_BYTES_SIZE;
    static final @ByteSize int CELL_VALUE_LENGTH_SIZE = SHORT_BYTES_SIZE;
    static final @ByteSize int CELL_SIZE = CELL_PAGE_OFFSET_SIZE + CELL_KEY_LENGTH_SIZE + CELL_VALUE_LENGTH_SIZE;


    static final @ByteSize int KEY_SIZE = LONG_BYTES_SIZE;
    static final @ByteSize int VALUE_SIZE = LONG_BYTES_SIZE;

    static final int MINIMUM_PAGE_SIZE = HEADER_SIZE_BYTES + (2 * (KEY_SIZE + VALUE_SIZE));
}
