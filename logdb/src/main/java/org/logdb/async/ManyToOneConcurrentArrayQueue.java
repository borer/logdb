package org.logdb.async;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.util.function.Consumer;

/**
 * Pad out a cacheline to the left of a producer fields to prevent false sharing.
 */
class ConcurrentArrayQueuePadding1
{
    @SuppressWarnings("unused")
    protected long p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15;
}

/**
 * Value for the producer that are expected to be padded.
 */
class ConcurrentArrayQueueProducer extends ConcurrentArrayQueuePadding1
{
    volatile long tail;
    volatile long sharedHeadCache;
}

/**
 * Pad out a cacheline between the producer and consumer fields to prevent false sharing.
 */
class ConcurrentArrayQueuePadding2 extends ConcurrentArrayQueueProducer
{
    @SuppressWarnings("unused")
    protected long p16, p17, p18, p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30;
}

/**
 * Values for the consumer that are expected to be padded.
 */
class ConcurrentArrayQueueConsumer extends ConcurrentArrayQueuePadding2
{
    volatile long head;
}

/**
 * Pad out a cacheline between the producer and consumer fields to prevent false sharing.
 */
class ConcurrentArrayQueuePadding3 extends ConcurrentArrayQueueConsumer
{
    @SuppressWarnings("unused")
    protected long p31, p32, p33, p34, p35, p36, p37, p38, p39, p40, p41, p42, p43, p44, p45;
}

//TODO:make this less garbadge by pre-allocating all the slots of the array and then just populating them
/**
 * Inspired by Agrona many to one queue.
 * One producer to one consumer concurrent queue that is array backed. The algorithm is a variation of Fast Flow
 * adapted to work with the Java Memory Model on arrays by using {@link sun.misc.Unsafe}.
 *
 * @param <E> type of the elements stored in the {@link java.util.Queue}.
 */
public class ManyToOneConcurrentArrayQueue<E>
        extends ConcurrentArrayQueuePadding3
{
    private static final long TAIL_OFFSET;
    private static final long SHARED_HEAD_CACHE_OFFSET;
    private static final long HEAD_OFFSET;
    private static final int BUFFER_ARRAY_BASE;
    private static final int SHIFT_FOR_SCALE;
    private static final Unsafe UNSAFE;

    static
    {
        try
        {
            final PrivilegedExceptionAction<Unsafe> action =
                    () ->
                    {
                        final Field f = Unsafe.class.getDeclaredField("theUnsafe");
                        f.setAccessible(true);

                        return (Unsafe)f.get(null);
                    };

            UNSAFE = AccessController.doPrivileged(action);
        }
        catch (final Exception e)
        {
            throw new RuntimeException("Unable to load unsafe", e);
        }

        try
        {
            BUFFER_ARRAY_BASE = UNSAFE.arrayBaseOffset(Object[].class);
            SHIFT_FOR_SCALE = calculateShiftForScale(UNSAFE.arrayIndexScale(Object[].class));
            TAIL_OFFSET = UNSAFE.objectFieldOffset(ConcurrentArrayQueueProducer.class.getDeclaredField("tail"));
            SHARED_HEAD_CACHE_OFFSET = UNSAFE.objectFieldOffset(
                    ConcurrentArrayQueueProducer.class.getDeclaredField("sharedHeadCache"));
            HEAD_OFFSET = UNSAFE.objectFieldOffset(ConcurrentArrayQueueConsumer.class.getDeclaredField("head"));
        }
        catch (final Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }

    protected final int capacity;
    protected final E[] buffer;

    @SuppressWarnings("unchecked")
    ManyToOneConcurrentArrayQueue(final int requestedCapacity)
    {
        this.capacity = findNextPositivePowerOfTwo(requestedCapacity);
        this.buffer = (E[])new Object[this.capacity];
    }

    private static long sequenceToBufferOffset(final long sequence, final long mask)
    {
        return BUFFER_ARRAY_BASE + ((sequence & mask) << SHIFT_FOR_SCALE);
    }

    boolean offer(final E e)
    {
        if (null == e)
        {
            throw new NullPointerException("element cannot be null");
        }

        final int capacity = this.capacity;
        long currentHead = sharedHeadCache;
        long bufferLimit = currentHead + capacity;
        long currentTail;
        do
        {
            currentTail = tail;
            if (currentTail >= bufferLimit)
            {
                currentHead = head;
                bufferLimit = currentHead + capacity;
                if (currentTail >= bufferLimit)
                {
                    return false;
                }

                UNSAFE.putOrderedLong(this, SHARED_HEAD_CACHE_OFFSET, currentHead);
            }
        }
        while (!UNSAFE.compareAndSwapLong(this, TAIL_OFFSET, currentTail, currentTail + 1));

        UNSAFE.putOrderedObject(buffer, sequenceToBufferOffset(currentTail, capacity - 1), e);

        return true;
    }

    int drain(final Consumer<E> elementConsumer)
    {
        return drain(elementConsumer, (int)(tail - head));
    }

    @SuppressWarnings("unchecked")
    private int drain(final Consumer<E> elementConsumer, final int limit)
    {
        final Object[] buffer = this.buffer;
        final long mask = this.capacity - 1;
        final long currentHead = head;
        long nextSequence = currentHead;
        final long limitSequence = nextSequence + limit;

        while (nextSequence < limitSequence)
        {
            final long elementOffset = sequenceToBufferOffset(nextSequence, mask);
            final Object item = UNSAFE.getObjectVolatile(buffer, elementOffset);

            if (null == item)
            {
                break;
            }

            UNSAFE.putOrderedObject(buffer, elementOffset, null);
            nextSequence++;
            UNSAFE.putOrderedLong(this, HEAD_OFFSET, nextSequence);
            elementConsumer.accept((E)item);
        }

        return (int)(nextSequence - currentHead);
    }

    public int capacity()
    {
        return capacity;
    }

    public int remainingCapacity()
    {
        return capacity() - size();
    }

    public boolean isEmpty()
    {
        return head == tail;
    }

    public int size()
    {
        long currentHeadBefore;
        long currentTail;
        long currentHeadAfter = head;

        do
        {
            currentHeadBefore = currentHeadAfter;
            currentTail = tail;
            currentHeadAfter = head;
        }
        while (currentHeadAfter != currentHeadBefore);

        return (int)(currentTail - currentHeadAfter);
    }

    /**
     * Fast method of finding the next power of 2 greater than or equal to the supplied value.
     * <p>
     * If the value is &lt;= 0 then 1 will be returned.
     * <p>
     * This method is not suitable for {@link Integer#MIN_VALUE} or numbers greater than 2^30.
     *
     * @param value from which to search for next power of 2
     * @return The next power of 2 or the value itself if it is a power of 2
     */
    private static int findNextPositivePowerOfTwo(final int value)
    {
        return 1 << (Integer.SIZE - Integer.numberOfLeadingZeros(value - 1));
    }

    /**
     * Calculate the shift value to scale a number based on how refs are compressed or not.
     *
     * @param scale of the number reported by Unsafe.
     * @return how many times the number needs to be shifted to the left.
     */
    private static int calculateShiftForScale(final int scale)
    {
        if (4 == scale)
        {
            return 2;
        }
        else if (8 == scale)
        {
            return 3;
        }

        throw new IllegalArgumentException("unknown pointer size for scale=" + scale);
    }
}
