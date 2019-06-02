package org.logdb.bbtree;

import org.logdb.storage.NodesManager;

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
            final long currentCommittedRootPageNumber = bTree.getCommittedRoot();
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

        for (int i = 0; i < node.numberOfKeys; i++)
        {
            final long key = node.getKey(i);
            final long value = node.getValue(i);
            printer.append(String.format(" <%d> |%d| ", key, value));
        }

        printer.append("\"];\n");
    }

    private static void printNonLeaf(final StringBuilder printer, final BTreeNodeAbstract node, final NodesManager nodesManager)
    {
        final String id = String.valueOf(node.getPageNumber());
        final String lastChildId = getPageUniqueId(node.numberOfKeys, node);

        printer.append(String.format("\"%s\"", id));
        printer.append("[label = \"");

        if (node.numberOfLogKeyValues > 0)
        {
            printer.append("{ log | {");
            for (int i = 0; i < node.numberOfLogKeyValues; i++)
            {
                if (i + 1 < node.numberOfLogKeyValues)
                {
                    printer.append(String.format(" %s-%s | ", node.getLogKey(i), node.getLogValue(i)));
                }
                else
                {
                    printer.append(String.format(" %s-%s ", node.getLogKey(i), node.getLogValue(i)));
                }
            }
            printer.append("}}|");
        }

        for (int i = 0; i < node.numberOfKeys; i++)
        {
            final long key = node.getKey(i);
            final String childId = getPageUniqueId(i, node);
            printer.append(String.format(" <%s> |%d| ", childId, key));
        }
        printer.append(" <lastChild> |Ls ");
        printer.append("\"];\n");

        for (int i = 0; i < node.numberOfKeys; i++)
        {
            final String childId = getPageUniqueId(i, node);
            printer.append(String.format("\"%s\":%s -> \"%s\"", id, childId, childId));
            printer.append("\n");
        }

        printer.append(String.format("\"%s\":lastChild -> \"%s\"", id, lastChildId));
        printer.append("\n");

        try (BTreeMappedNode  mappedNode = nodesManager.getOrCreateMappedNode())
        {
            for (int i = 0; i < node.numberOfValues; i++)
            {
                final BTreeNode child = nodesManager.loadNode(i, node, mappedNode);
                print(printer, child, nodesManager);
            }
        }
    }

    private static String getPageUniqueId(final int index, BTreeNode node)
    {
        return String.valueOf(
                node.getValue(index) == BTreeNodeNonLeaf.NON_COMMITTED_CHILD
                ? node.getChildAt(index).getPageNumber()
                : node.getValue(index));
    }
}
