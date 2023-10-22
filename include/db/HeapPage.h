#ifndef DB_HEAPPAGE_H
#define DB_HEAPPAGE_H

#include <db/HeapPageId.h>
#include <db/Tuple.h>
#include <db/Page.h>

namespace db {
    class HeapPageIterator;
    /**
     * Each instance of HeapPage stores data for one page of HeapFiles and
     * implements the Page interface that is used by BufferPool.
     *
     * @see HeapFile
     * @see BufferPool
     *
     */
    class HeapPage : public Page {
        friend class HeapPageIterator;
        HeapPageId pid;
        TupleDesc td;
        uint8_t *header;
        Tuple *tuples;
        int numSlots;

        /**
         * Suck up tuples from the source file.
         */
        void readTuple(Tuple *t, uint8_t *data, int slotId);

    public:
        /**
         * Create a HeapPage from a set of bytes of data read from disk.
         * The format of a HeapPage is a set of header bytes indicating
         * the slots of the page that are in use, some number of tuple slots.
         *  Specifically, the number of tuples is equal to: <p>
         *          floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
         * <p> where tuple size is the size of tuples in this
         * database table, which can be determined via {@link Catalog#getTupleDesc}.
         * The number of 8-bit header words is equal to:
         * <p>
         *      ceiling(no. tuple slots / 8)
         * <p>
         * @see Database#getCatalog
         * @see Catalog#getTupleDesc
         * @see BufferPool#getPageSize()
         */
        HeapPage(const HeapPageId &id, uint8_t *data);


        /** Retrieve the number of tuples on this page.
            @return the number of tuples on this page
        */
        int getNumTuples() const;

        /**
         * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
         * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
         */
        int getHeaderSize() const;

        /**
         * @return the PageId associated with this page.
         */
        const PageId &getId() const override;

        /**
         * Generates a byte array representing the contents of this page.
         * Used to serialize this page to disk.
         * <p>
         * The invariant here is that it should be possible to pass the byte
         * array generated by getPageData to the HeapPage constructor and
         * have it produce an identical HeapPage object.
         *
         * @see #HeapPage
         * @return A byte array correspond to the bytes of this page.
         */
        void *getPageData() const override;

        /**
         * Static method to generate a byte array corresponding to an empty
         * HeapPage.
         * Used to add new, empty pages to the file. Passing the results of
         * this method to the HeapPage constructor will create a HeapPage with
         * no valid tuples in it.
         *
         * @return The returned ByteArray.
         */
        static void *createEmptyPageData();

        /**
         * Returns the number of empty slots on this page.
         */
        int getNumEmptySlots() const;

        /**
         * Returns true if associated slot on this page is filled.
         */
        bool isSlotUsed(int i) const;

        // Begin and End methods for iterators
        HeapPageIterator begin() const;

        HeapPageIterator end() const;
    };

    /**
     * @return an iterator over all tuples on this page
     * (note that this iterator shouldn't return tuples in empty slots!)
     */
    class HeapPageIterator {
        int slot;
        const HeapPage *page;
    public:
        HeapPageIterator(int slot, const HeapPage *page);

        bool operator!=(const HeapPageIterator &other) const;

        bool operator==(const HeapPageIterator &other) const;

        Tuple &operator*() const;

        HeapPageIterator &operator++();
    };
}

#endif
