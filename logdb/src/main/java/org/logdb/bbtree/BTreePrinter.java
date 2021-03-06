package org.logdb.bbtree;

import org.logdb.bit.BinaryHelper;
import org.logdb.storage.PageNumber;

import java.util.Arrays;

//TODO: extract into a visitor pattern
public class BTreePrinter
{
    /**
     * Outputs graphivz format that represents the B+tree.
     * @return a graphivz string formatted representation of the btree
     */
    public static String print(final BTree bTree, final NodesManager nodesManager)
    {
        final BTreeNode currentUncommittedRootNode = bTree.getUncommittedRoot();
        if (currentUncommittedRootNode != null)
        {
            return print(currentUncommittedRootNode, nodesManager);
        }
        else
        {
            final @PageNumber long currentCommittedRootPageNumber = bTree.getCommittedRoot();
            try (BTreeMappedNode  mappedNode = nodesManager.getOrCreateMappedNode())
            {
                mappedNode.initNode(currentCommittedRootPageNumber);
                return print(mappedNode, nodesManager);
            }
        }
    }

    /**
     * Outputs graphivz format that represents the B+tree.
     * @param root the root node to start printing.
     * @return a graphivz string formatted representation of the btree
     */
    static String print(final BTreeNode root, final NodesManager nodesManager)
    {
        final StringBuilder printer = new StringBuilder();
        printer.append("digraph g {\n");
        printer.append("node [shape = record,height=.1];\n");

        print(printer, root, nodesManager);

        printer.append("}\n");

        return printer.toString();
    }

    private static void print(final StringBuilder printer, final BTreeNode node, final NodesManager nodesManager)
    {
        if (node instanceof BTreeNodeNonLeaf)
        {
            printNonLeaf(printer, (BTreeNodeNonLeaf)node, nodesManager);
        }
        else if (node instanceof BTreeNodeLeaf)
        {
            printLeaf(printer, (BTreeNodeLeaf) node);
        }
        else if (node instanceof BTreeMappedNode)
        {
            if (node.getNodeType() == BtreeNodeType.NonLeaf)
            {
                printNonLeaf(printer, (BTreeMappedNode) node, nodesManager);
            }
            else
            {
                printLeaf(printer, (BTreeMappedNode) node);
            }
        }
        else
        {
            throw new UnsupportedOperationException("node type is not supported");
        }
    }

    private static void printLeaf(final StringBuilder printer, final BTreeNodeAbstract node)
    {
        final String id = String.valueOf(node.getPageNumber());
        printer.append(String.format("\"%s\"", id));
        printer.append("[label = \"");

        for (int i = 0; i < node.entries.getNumberOfPairs(); i++)
        {
            final byte[] key = node.getKey(i);
            final byte[] value = node.getValue(i);
            printer.append(String.format(" <%s> |%s| ", getReadableString(key), getReadableString(value)));
        }

        printer.append("\"];\n");
    }

    private static void printNonLeaf(final StringBuilder printer, final BTreeLogNodeAbstract node, final NodesManager nodesManager)
    {
        final String id = String.valueOf(node.getPageNumber());
        final int rightmostIndex = node.entries.getNumberOfPairs() - 1;
        final String lastChildId = getPageUniqueId(rightmostIndex, node);

        printer.append(String.format("\"%s\"", id));
        printer.append("[label = \"");

        if (node.getNumberOfLogPairs() > 0)
        {
            printer.append("{ log | {");
            for (int i = 0; i < node.getNumberOfLogPairs(); i++)
            {
                final byte[] logKey = node.getLogKey(i);
                final byte[] logValue = node.getLogValueAtIndex(i);

                if (i + 1 < node.getNumberOfLogPairs())
                {
                    printer.append(String.format(" %s-%s | ", getReadableString(logKey), getReadableString(logValue)));
                }
                else
                {
                    printer.append(String.format(" %s-%s ", getReadableString(logKey), getReadableString(logValue)));
                }
            }
            printer.append("}}|");
        }

        for (int i = 0; i < rightmostIndex; i++)
        {
            final byte[] key = node.getKey(i);
            final String childId = getPageUniqueId(i, node);
            printer.append(String.format(" <%s> |%s| ", childId, getReadableString(key)));
        }
        printer.append(" <lastChild> |Ls ");
        printer.append("\"];\n");

        for (int i = 0; i < rightmostIndex; i++)
        {
            final String childId = getPageUniqueId(i, node);
            printer.append(String.format("\"%s\":%s -> \"%s\"", id, childId, childId));
            printer.append("\n");
        }

        printer.append(String.format("\"%s\":lastChild -> \"%s\"", id, lastChildId));
        printer.append("\n");

        try (BTreeMappedNode  mappedNode = nodesManager.getOrCreateMappedNode())
        {
            for (int i = 0; i < node.entries.getNumberOfPairs(); i++)
            {
                final BTreeNode child = nodesManager.loadNode(i, node, mappedNode);
                print(printer, child, nodesManager);
            }
        }
    }

    private static String getPageUniqueId(final int index, BTreeNode node)
    {
        return String.valueOf(
                Arrays.equals(node.getValue(index), BTreeNodeNonLeaf.NON_COMMITTED_CHILD)
                        ? node.getChildAt(index).getPageNumber()
                        : getReadableString(node.getValue(index)));
    }

    private static String getReadableString(final byte[] array)
    {
        if (array.length == Long.BYTES)
        {
            return String.valueOf(BinaryHelper.bytesToLong(array));
        }
        else
        {
            return new String(array);
        }
    }
}
