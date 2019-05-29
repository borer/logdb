package org.logdb.bbtree;

import org.logdb.bit.HeapMemory;

public interface BTreeNodeHeap extends BTreeNode
{
    /**
     * Gets the underlying buffer that stores the content of this node. Changes to that buffer will change the node content.
     * @return nodes buffer
     */
    HeapMemory getBuffer();
}
