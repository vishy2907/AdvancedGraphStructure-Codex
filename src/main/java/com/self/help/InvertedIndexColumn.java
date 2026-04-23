package com.self.help;

import org.roaringbitmap.RoaringBitmap;

class InvertedIndexColumn {
    // Array index = Dict ID from Dictionary
    // Value = Bitmap of Row IDs
    private RoaringBitmap[] bitmaps;

    public InvertedIndexColumn() {
        // Starting with 64 slots for unique values
        this.bitmaps = new RoaringBitmap[64];
    }

    public void addRowToValue(int dictId, int rowId) {
        // Ensure the array can accommodate the Dict ID
        if (dictId >= bitmaps.length) {
            expand(dictId);
        }

        if (bitmaps[dictId] == null) {
            bitmaps[dictId] = new RoaringBitmap();
        }
        bitmaps[dictId].add(rowId);
    }

    private void expand(int requiredDictId) {
        int newCapacity = bitmaps.length;

        // Double the capacity until it can hold the required Dict ID
        while (newCapacity <= requiredDictId) {
            newCapacity *= 2;
        }

        RoaringBitmap[] newArray = new RoaringBitmap[newCapacity];
        // High-performance copy of the bitmap references
        System.arraycopy(bitmaps, 0, newArray, 0, bitmaps.length);
        this.bitmaps = newArray;
    }

    public RoaringBitmap getRowsForValue(int dictId) {
        // If the query asks for a Dict ID outside current range, return empty
        if (dictId < 0 || dictId >= bitmaps.length || bitmaps[dictId] == null) {
            return new RoaringBitmap();
        }
        return bitmaps[dictId];
    }

    public void add(int encodedFromId, int rowId) {
        addRowToValue(encodedFromId, rowId);
    }
}
