package com.github.benmanes.caffeine.cache.simulator.policy.sketch;

import java.util.List;

public interface CacheBlock {
    /***
     * This function removes the lowest ranked items in the block,
     * in a similar fashion to multiple calls to evict/
     * @param numOfItems - The number of items to evict
     * @return A list of min{numOfItems, size} items that where removed from the block
     */
    List<EntryData> evictMultiple(int numOfItems);

    /***
     * Removes the lowest ranked item in the block according to its internal ordering.
     * @return The lowest ranked item that was removed.
     */
    EntryData evict();

    /***
     * A function to offer multiple possible items to the block
     * @param items - The entry data of the items offered to this block
     * @return A list of all the items <b>not accepted</b> or removed from the block.
     */
    List<EntryData> offerMultiple(List<EntryData> items);

    /***
     * This function offers one item to the block that may be accepted or rejected from the block.
     * If accepted, the block may need to evict another item instead.
     * @param item - The item offered to the block
     * @return If needed, returns either the item offered or the item evicted.
     */
    EntryData offer(EntryData item);

    void increaseCapacity(int amount);
    void decreaseCapacity(int amount);

    boolean contains(long key);

    /***
     * If the key is in the block, returns its data, and records the access to the item.
     * Returns Null if doesn't exist in the block.
     * @param key - The key to be fetched.
     * @return The item data if exists, and Null otherwise.
     */
    EntryData get(long key);

    int capacity();
    int size();

    int getHitCount();
    double getBenefit();

    /***
     * These functions regard the tail of the block as a reference of
     * the lowest ranking items as ghost entries.
     * @return The benefit of the tail of the cache.
     */
    default int getTailHitCount() { return -1; }
    default double getTailBenefit() { return -1; }
}
