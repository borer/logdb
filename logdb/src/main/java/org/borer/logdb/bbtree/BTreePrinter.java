package org.borer.logdb.bbtree;

import org.borer.logdb.storage.NodesManager;

public class BTreePrinter
{
    /**
     * Outputs graphivz format that represents the B+tree.
     * @param bTree uses the current root node of the btree
     * @return a graphivz string formatted representation of the btree
     */
    public static String print(final BTree bTree, final NodesManager nodesManager)
    {
        return print(bTree.getCurrentRootNode(), nodesManager);
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

    private static void print(final StringBuilder printer, final BTreeNodeLeaf node)
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

    private static void print(final StringBuilder printer, final BTreeNodeNonLeaf node, final NodesManager nodesManager)
    {
        final String id = String.valueOf(node.getPageNumber());
        final String lastChildId = getPageUniqueId(node.numberOfKeys, node);

        printer.append(String.format("\"%s\"", id));
        printer.append("[label = \"");

        for (int i = 0; i < node.numberOfKeys; i++)
        {
            final long key = node.getKey(i);
            printer.append(String.format(" <%d> |%d| ", key, key));
        }
        printer.append(" <lastChild> |Ls ");
        printer.append("\"];\n");

        for (int i = 0; i < node.numberOfKeys; i++)
        {
            final long key = node.getKey(i);
            final String childId = getPageUniqueId(i, node);
            printer.append(String.format("\"%s\":%d -> \"%s\"", id, key, childId));
            printer.append("\n");
        }

        printer.append(String.format("\"%s\":lastChild -> \"%s\"", id, lastChildId));
        printer.append("\n");

        for (int i = 0; i < node.numberOfValues; i++)
        {
            final BTreeNode child = nodesManager.loadNode(i, node);
            print(printer, child, nodesManager);
        }
    }

    private static void print(final StringBuilder printer, final BTreeNode node, final NodesManager nodesManager)
    {
        if (node instanceof BTreeNodeNonLeaf)
        {
            print(printer, (BTreeNodeNonLeaf)node, nodesManager);
        }
        else
        {
            print(printer, (BTreeNodeLeaf) node);
        }
    }

    private static String getPageUniqueId(final int index, BTreeNodeNonLeaf node)
    {
        return String.valueOf(
                node.getValue(index) == BTreeNodeNonLeaf.NON_COMMITTED_CHILD
                ? node.getChildAt(index).getPageNumber()
                : node.getValue(index));
    }
}
