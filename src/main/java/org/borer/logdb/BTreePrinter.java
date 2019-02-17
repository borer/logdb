package org.borer.logdb;

import java.nio.ByteBuffer;

public class BTreePrinter
{
    /**
     * Outputs graphivz format that represents the B+tree
     * @param bTree uses the current root node of the btree
     * @return a graphivz string formated representation of the btree
     */
    public static String print(final BTree bTree)
    {
        return print(bTree.getCurrentRootNode());
    }

    /**
     * Outputs graphivz format that represents the B+tree
     * @param root the root node to start printing.
     * @return a graphivz string formated representation of the btree
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

        for (int i = 0; i < node.keys.length; i++)
        {
            final String key = new String(node.keys[i].array());
            final String value = new String(node.values[i].array());
            printer.append(String.format(" <%s> |%s| ", key, value));
        }

        printer.append("\"];\n");
    }

    private static void print(final StringBuilder printer, final BTreeNodeNonLeaf node)
    {
        final String id = String.valueOf(node.getId());
        final String lastChildId = String.valueOf(node.children[node.keys.length].getId());

        printer.append(String.format("\"%s\"", id));
        printer.append("[label = \"");

        for (ByteBuffer key : node.keys)
        {
            final String keyLabel = new String(key.array());
            printer.append(String.format(" <%s> |%s| ", keyLabel, keyLabel));
        }
        printer.append(" <lastChild> |Ls ");
        printer.append("\"];\n");

        for (int i = 0; i < node.keys.length; i++)
        {
            final String keyLabel = new String(node.keys[i].array());
            final String childId = String.valueOf(node.children[i].getId());
            printer.append(String.format("\"%s\":%s -> \"%s\"", id, keyLabel, childId));
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
