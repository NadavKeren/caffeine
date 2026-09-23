package com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline.rankmap;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * A red-black tree augmented with subtree sizes, giving order statistics in {@code O(log n)}.
 * <p>
 * The balancing follows the algorithm used by {@link java.util.TreeMap}; the only addition is the
 * {@code size} field on each node, maintained on insertion, deletion and rotation. The shadow
 * rankings need both directions: {@link #rankDescending} answers "how good is this key" and
 * {@link #forEachDescending} dumps the leaderboard prefix that the reach-depth solver walks.
 * <p>
 * Keys must be unique under the supplied comparator; the ranked blocks guarantee that by breaking
 * score ties on the cache key.
 */
@SuppressWarnings("NullAway")
public final class OrderStatisticTree<K> {
    private static final boolean RED = false;
    private static final boolean BLACK = true;

    final private Comparator<? super K> comparator;
    @Nullable private Node<K> root;

    public OrderStatisticTree(Comparator<? super K> comparator) {
        this.comparator = Objects.requireNonNull(comparator);
    }

    public int size() {
        return sizeOf(root);
    }

    public boolean isEmpty() {
        return root == null;
    }

    public void clear() {
        root = null;
    }

    /**
     * Inserts the key, returning {@code false} if an equal key was already present.
     */
    public boolean add(K key) {
        if (root == null) {
            root = new Node<>(key, null);
            return true;
        }

        Node<K> parent = root;
        int cmp;
        while (true) {
            cmp = comparator.compare(key, parent.key);
            if (cmp == 0) {
                return false;
            }

            Node<K> next = cmp < 0 ? parent.left : parent.right;
            if (next == null) {
                break;
            }
            parent = next;
        }

        Node<K> inserted = new Node<>(key, parent);
        if (cmp < 0) {
            parent.left = inserted;
        } else {
            parent.right = inserted;
        }

        for (Node<K> curr = parent; curr != null; curr = curr.parent) {
            ++curr.size;
        }

        fixAfterInsertion(inserted);
        return true;
    }

    /**
     * Removes the key, returning {@code false} if it was not present.
     */
    public boolean remove(K key) {
        Node<K> node = findNode(key);
        if (node == null) {
            return false;
        }

        deleteNode(node);
        return true;
    }

    public boolean contains(K key) {
        return findNode(key) != null;
    }

    /**
     * Returns the number of stored keys that compare strictly less than the argument. The key itself
     * need not be present.
     */
    public int countLess(K key) {
        int less = 0;
        Node<K> curr = root;

        while (curr != null) {
            int cmp = comparator.compare(key, curr.key);
            if (cmp <= 0) {
                curr = curr.left;
            } else {
                less += sizeOf(curr.left) + 1;
                curr = curr.right;
            }
        }

        return less;
    }

    /**
     * Returns the one-based rank of the key counting down from the largest, or {@code 0} when the key
     * is absent. The largest stored key has rank {@code 1}.
     */
    public int rankDescending(K key) {
        if (!contains(key)) {
            return 0;
        }

        return size() - countLess(key);
    }

    /**
     * Returns the key at the given zero-based offset from the largest.
     */
    public K select(int indexFromLargest) {
        if (indexFromLargest < 0 || indexFromLargest >= size()) {
            throw new IndexOutOfBoundsException("index: " + indexFromLargest + " size: " + size());
        }

        int remaining = size() - 1 - indexFromLargest; // the equivalent ascending offset
        Node<K> curr = root;

        while (true) {
            int leftSize = sizeOf(curr.left);
            if (remaining < leftSize) {
                curr = curr.left;
            } else if (remaining == leftSize) {
                return curr.key;
            } else {
                remaining -= leftSize + 1;
                curr = curr.right;
            }
        }
    }

    /**
     * Returns the smallest key, which is the victim under every ranked block's ordering.
     */
    public K min() {
        if (root == null) {
            throw new IllegalStateException("Empty tree");
        }

        return minNode(root).key;
    }

    /**
     * Feeds up to {@code limit} keys to the action, largest first.
     */
    public void forEachDescending(int limit, Consumer<? super K> action) {
        int remaining = Math.min(limit, size());
        Node<K> curr = root;
        var stack = new java.util.ArrayDeque<Node<K>>();

        while (remaining > 0 && (curr != null || !stack.isEmpty())) {
            while (curr != null) {
                stack.push(curr);
                curr = curr.right;
            }

            Node<K> node = stack.pop();
            action.accept(node.key);
            --remaining;
            curr = node.left;
        }
    }

    @Nullable
    private Node<K> findNode(K key) {
        Node<K> curr = root;
        while (curr != null) {
            int cmp = comparator.compare(key, curr.key);
            if (cmp == 0) {
                return curr;
            }
            curr = cmp < 0 ? curr.left : curr.right;
        }

        return null;
    }

    private void deleteNode(Node<K> node) {
        Node<K> target = node;

        if (target.left != null && target.right != null) {
            // Standard two-child deletion: overwrite with the successor's key and delete the successor,
            // which has at most one child. Subtree sizes are unaffected by the key move itself.
            Node<K> successor = minNode(target.right);
            target.key = successor.key;
            target = successor;
        }

        Node<K> replacement = target.left != null ? target.left : target.right;

        for (Node<K> curr = target.parent; curr != null; curr = curr.parent) {
            --curr.size;
        }

        if (replacement != null) {
            replacement.parent = target.parent;
            if (target.parent == null) {
                root = replacement;
            } else if (target == target.parent.left) {
                target.parent.left = replacement;
            } else {
                target.parent.right = replacement;
            }

            target.left = target.right = target.parent = null;

            if (target.color == BLACK) {
                fixAfterDeletion(replacement);
            }
        } else if (target.parent == null) {
            root = null;
        } else {
            // The node stays linked while the colours are repaired, the way TreeMap does it. Its size
            // must already read as zero, otherwise a rotation would recompute an ancestor from a node
            // that the walk above has already discounted.
            target.size = 0;

            if (target.color == BLACK) {
                fixAfterDeletion(target);
            }

            if (target.parent != null) {
                if (target == target.parent.left) {
                    target.parent.left = null;
                } else if (target == target.parent.right) {
                    target.parent.right = null;
                }
                target.parent = null;
            }
        }
    }

    private static <K> Node<K> minNode(Node<K> node) {
        Node<K> curr = node;
        while (curr.left != null) {
            curr = curr.left;
        }

        return curr;
    }

    private static <K> int sizeOf(@Nullable Node<K> node) {
        return node == null ? 0 : node.size;
    }

    private static <K> boolean colorOf(@Nullable Node<K> node) {
        return node == null ? BLACK : node.color;
    }

    @Nullable
    private static <K> Node<K> parentOf(@Nullable Node<K> node) {
        return node == null ? null : node.parent;
    }

    @Nullable
    private static <K> Node<K> leftOf(@Nullable Node<K> node) {
        return node == null ? null : node.left;
    }

    @Nullable
    private static <K> Node<K> rightOf(@Nullable Node<K> node) {
        return node == null ? null : node.right;
    }

    private static <K> void setColor(@Nullable Node<K> node, boolean color) {
        if (node != null) {
            node.color = color;
        }
    }

    private void rotateLeft(@Nullable Node<K> node) {
        if (node == null) {
            return;
        }

        Node<K> right = node.right;
        node.right = right.left;
        if (right.left != null) {
            right.left.parent = node;
        }

        right.parent = node.parent;
        if (node.parent == null) {
            root = right;
        } else if (node.parent.left == node) {
            node.parent.left = right;
        } else {
            node.parent.right = right;
        }

        right.left = node;
        node.parent = right;

        node.size = sizeOf(node.left) + sizeOf(node.right) + 1;
        right.size = sizeOf(right.left) + sizeOf(right.right) + 1;
    }

    private void rotateRight(@Nullable Node<K> node) {
        if (node == null) {
            return;
        }

        Node<K> left = node.left;
        node.left = left.right;
        if (left.right != null) {
            left.right.parent = node;
        }

        left.parent = node.parent;
        if (node.parent == null) {
            root = left;
        } else if (node.parent.right == node) {
            node.parent.right = left;
        } else {
            node.parent.left = left;
        }

        left.right = node;
        node.parent = left;

        node.size = sizeOf(node.left) + sizeOf(node.right) + 1;
        left.size = sizeOf(left.left) + sizeOf(left.right) + 1;
    }

    private void fixAfterInsertion(Node<K> inserted) {
        Node<K> node = inserted;
        node.color = RED;

        while (node != null && node != root && node.parent.color == RED) {
            if (parentOf(node) == leftOf(parentOf(parentOf(node)))) {
                Node<K> uncle = rightOf(parentOf(parentOf(node)));
                if (colorOf(uncle) == RED) {
                    setColor(parentOf(node), BLACK);
                    setColor(uncle, BLACK);
                    setColor(parentOf(parentOf(node)), RED);
                    node = parentOf(parentOf(node));
                } else {
                    if (node == rightOf(parentOf(node))) {
                        node = parentOf(node);
                        rotateLeft(node);
                    }
                    setColor(parentOf(node), BLACK);
                    setColor(parentOf(parentOf(node)), RED);
                    rotateRight(parentOf(parentOf(node)));
                }
            } else {
                Node<K> uncle = leftOf(parentOf(parentOf(node)));
                if (colorOf(uncle) == RED) {
                    setColor(parentOf(node), BLACK);
                    setColor(uncle, BLACK);
                    setColor(parentOf(parentOf(node)), RED);
                    node = parentOf(parentOf(node));
                } else {
                    if (node == leftOf(parentOf(node))) {
                        node = parentOf(node);
                        rotateRight(node);
                    }
                    setColor(parentOf(node), BLACK);
                    setColor(parentOf(parentOf(node)), RED);
                    rotateLeft(parentOf(parentOf(node)));
                }
            }
        }

        root.color = BLACK;
    }

    private void fixAfterDeletion(Node<K> start) {
        Node<K> node = start;

        while (node != root && colorOf(node) == BLACK) {
            if (node == leftOf(parentOf(node))) {
                Node<K> sibling = rightOf(parentOf(node));

                if (colorOf(sibling) == RED) {
                    setColor(sibling, BLACK);
                    setColor(parentOf(node), RED);
                    rotateLeft(parentOf(node));
                    sibling = rightOf(parentOf(node));
                }

                if (colorOf(leftOf(sibling)) == BLACK && colorOf(rightOf(sibling)) == BLACK) {
                    setColor(sibling, RED);
                    node = parentOf(node);
                } else {
                    if (colorOf(rightOf(sibling)) == BLACK) {
                        setColor(leftOf(sibling), BLACK);
                        setColor(sibling, RED);
                        rotateRight(sibling);
                        sibling = rightOf(parentOf(node));
                    }
                    setColor(sibling, colorOf(parentOf(node)));
                    setColor(parentOf(node), BLACK);
                    setColor(rightOf(sibling), BLACK);
                    rotateLeft(parentOf(node));
                    node = root;
                }
            } else {
                Node<K> sibling = leftOf(parentOf(node));

                if (colorOf(sibling) == RED) {
                    setColor(sibling, BLACK);
                    setColor(parentOf(node), RED);
                    rotateRight(parentOf(node));
                    sibling = leftOf(parentOf(node));
                }

                if (colorOf(rightOf(sibling)) == BLACK && colorOf(leftOf(sibling)) == BLACK) {
                    setColor(sibling, RED);
                    node = parentOf(node);
                } else {
                    if (colorOf(leftOf(sibling)) == BLACK) {
                        setColor(rightOf(sibling), BLACK);
                        setColor(sibling, RED);
                        rotateLeft(sibling);
                        sibling = leftOf(parentOf(node));
                    }
                    setColor(sibling, colorOf(parentOf(node)));
                    setColor(parentOf(node), BLACK);
                    setColor(leftOf(sibling), BLACK);
                    rotateRight(parentOf(node));
                    node = root;
                }
            }
        }

        setColor(node, BLACK);
    }

    private static final class Node<K> {
        K key;
        boolean color = BLACK;
        int size = 1;

        @Nullable Node<K> left;
        @Nullable Node<K> right;
        @Nullable Node<K> parent;

        Node(K key, @Nullable Node<K> parent) {
            this.key = key;
            this.parent = parent;
        }
    }
}
