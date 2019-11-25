package org.logdb.checksum;

import org.logdb.storage.ByteSize;

public interface Checksum
{
    /**
     * Updates the current checksum with the specified byte.
     *
     * @param b the byte to update the checksum with
     */
    void update(int b);

    /**
     * Updates the current checksum with the specified array of bytes.
     *
     * @param bytes   the byte array to update the checksum with
     * @param offset the start offset of the data
     * @param length the number of bytes to use for the update
     */
    void update(byte[] bytes, int offset, int length);

    /**
     * Returns the current checksum value.
     *
     * @return the current checksum value
     */
    byte[] getValue();

    /**
     * Resets the checksum to its initial value.
     */
    void reset();

    @ByteSize int getValueSize();

    boolean compare(byte[] bytes);
}
