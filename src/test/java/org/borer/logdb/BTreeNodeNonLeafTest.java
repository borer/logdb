package org.borer.logdb;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.borer.logdb.TestUtils.createValue;

class BTreeNodeNonLeafTest
{
    private BTreeNodeNonLeaf bTreeNonLeaf;

    @BeforeEach
    void setUp()
    {
        bTreeNonLeaf = new BTreeNodeNonLeaf();
    }

    @Test
    void shouldBeAbleToInsertChild()
    {
        final BTreeNode bTreeNode = new BTreeNodeNonLeaf();
        final ByteBuffer key = createValue("key");

        bTreeNonLeaf.insertChild(key, bTreeNode);
    }

    @Test
    void shouldBeAbleToInsertMultipleChildren()
    {
        for (int i = 0; i < 10; i++)
        {
            final BTreeNode bTreeNode = new BTreeNodeNonLeaf();
            final ByteBuffer key = createValue("key" + i);

            bTreeNonLeaf.insertChild(key, bTreeNode);
        }
    }
}