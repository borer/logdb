package org.borer.logdb.bbtree;

import org.borer.logdb.bit.Memory;

public interface BTreeNodeHeap extends BTreeNode
{
    /**
     * Gets the underlying buffer that stores the content of this node. Changes to that buffer will change the node content.
     * @return nodes buffer
     */
    Memory getBuffer();

    void initNodeFromBuffer();

    boolean isInternal();
}
