package org.borer.logdb.bbtree;

public class BTreePrinter
{
    /**
     * Outputs graphivz format that represents the B+tree.
     * @param bTree uses the current root node of the btree
     * @return a graphivz string formatted representation of the btree
     */
    public static String print(final BTree bTree)
    {
        return print(bTree.getCurrentRootNode());
    }

    /**
     * Outputs graphivz format that represents the B+tree.
     * @param root the root node to start printing.
     * @return a graphivz string formatted representation of the btree
     */
    public static String print(final BTreeNode root)
    {
        final StringBuilder printer = new StringBuilder();
        printer.append("digraph g {\n");
        printer.append("node [shape = record,height=.1];\n");

        print(printer, root);

        printer.append("}\n");

        return printer.toString();
    }

    private static void print(final StringBuilder printer, final BTreeNodeLeaf node)
    {
        final String id = String.valueOf(node.getId());
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

    private static void print(final StringBuilder printer, final BTreeNodeNonLeaf node)
    {
        final String id = String.valueOf(node.getId());
        final String lastChildId = String.valueOf(node.children[node.numberOfKeys].getId());

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
            final String childId = String.valueOf(node.children[i].getId());
            printer.append(String.format("\"%s\":%d -> \"%s\"", id, key, childId));
            printer.append("\n");
        }

        printer.append(String.format("\"%s\":lastChild -> \"%s\"", id, lastChildId));
        printer.append("\n");

        for (final BTreeNode child : node.children)
        {
            print(printer, child);
        }
    }

    private static void print(final StringBuilder printer, final BTreeNode node)
    {
        if (node instanceof BTreeNodeNonLeaf)
        {
            print(printer, (BTreeNodeNonLeaf)node);
        }
        else
        {
            print(printer, (BTreeNodeLeaf) node);
        }
    }
}
