package net.ndolgov.timeseriescompression;

/**
 * Time series compression algorithm described in "www.vldb.org/pvldb/vol8/p1816-teller.pdf". It was produced by
 * translating from "https://github.com/facebookincubator/beringei/tree/master/beringei/lib/{BitUtil,TimeSeriesStream,TimeSeriesStream-inl,}"
 * to Java.<P>
 *
 * Minor changes: initial timestamp takes 8 bytes, a byte buffer instead of folly::fbstring, uint64_t -> long, uint32_t -> int.
 */
public final class CompressedTimeSeries {
    private static final int LEADING_ZEROS_LENGTH_BITS = 5;
    private static final int BLOCK_SIZE_LENGTH_BITS = 6;
    private static final int MAX_LEADING_ZEROS_LENGTH = (1 << LEADING_ZEROS_LENGTH_BITS) - 1;
    private static final int BLOCK_SIZE_ADJUSTMENT = 1;
    private static final int DEFAULT_DELTA = 60;
    private static final int BITS_FOR_FIRST_TIMESTAMP = 63;

    // Store a delta of delta for the values other than the first one in one of the following ways
    private static final Encodings[] TIMESTAMP_ENCODINGS = {
                                  // '0' = delta of delta did not change
        new Encodings(7, 2, 2),   // '10' followed by a value length of 7
        new Encodings(9, 6, 3),   // '110' followed by a value length of 9
        new Encodings(12, 14, 4), // '1110' followed by a value length of 12
        new Encodings(32, 15, 4)  // '1111' followed by a value length of 32
    };

    private final byte[] buffer;
    private int bitPos; // current #buffer offset, [bit]
    private int bitsInValue; // bits to read or write in the next #buffer access operation
    
    private long previousValue;
    private long previousTimestamp;
    private long previousTimestampDelta;
    private byte previousLeadingZeros;
    private byte previousTrailingZeros;

    public CompressedTimeSeries(byte[] buffer) {
        this.buffer = buffer;
    }

    /**
     * @return the number of actually used bytes in the buffer
     */
    public int size() {
        final int wholeBytes = bitPos >> 7;
        return ((bitPos & 0x7) > 0) ? wholeBytes + 1 : wholeBytes;
    }

    public boolean append(long time, double value) {
        appendTimestamp(time);
        appendValue(value);
        return true;
    }

    private void appendTimestamp(long timestamp) {
        if (bitPos == 0) {
            write(timestamp, BITS_FOR_FIRST_TIMESTAMP); // Store the first value as is
            previousTimestamp = timestamp;
            previousTimestampDelta = DEFAULT_DELTA;
            return;
        }

        final long delta = timestamp - previousTimestamp;
        long deltaOfDelta = delta - previousTimestampDelta;

        if (deltaOfDelta == 0) {
            previousTimestamp = timestamp;
            write(0, 1);
            return;
        }

        if (deltaOfDelta > 0) {
            deltaOfDelta--; // There are no zeros. Shift by one to fit in x number of bits
        }

        final long absDeltaOfDelta = Math.abs(deltaOfDelta);

        boolean overflow = true;
        for (int i = 0; i < 4; i++) {
            if (absDeltaOfDelta < ((long)1 << (TIMESTAMP_ENCODINGS[i].bitsForValue - 1))) {
                write(TIMESTAMP_ENCODINGS[i].controlValue, TIMESTAMP_ENCODINGS[i].controlValueBitLength);

                // Make this value between [0, 2^timestampEncodings[i].bitsForValue - 1]
                final long encodedDeltaOfDelta = deltaOfDelta + ((long)1 << (TIMESTAMP_ENCODINGS[i].bitsForValue - 1));

                write(encodedDeltaOfDelta, TIMESTAMP_ENCODINGS[i].bitsForValue);
                overflow = false;
                break;
            }
        }

        if (overflow) {
            throw new IllegalStateException("No encoding found to accommodate DoD: " + absDeltaOfDelta);
        }

        previousTimestamp = timestamp;
        previousTimestampDelta = delta;
    }

    private void appendValue(double value) {
        final long longValue = Double.doubleToLongBits(value);
        final long xorWithPrevious = previousValue ^ longValue;

        if (xorWithPrevious == 0) {
            write(0, 1);
            return;
        }

        write(1, 1); // not the same

        int leadingZeros = Long.numberOfLeadingZeros(xorWithPrevious);
        if (leadingZeros > MAX_LEADING_ZEROS_LENGTH) {
            leadingZeros = MAX_LEADING_ZEROS_LENGTH;
        }

        final int trailingZeros = Long.numberOfTrailingZeros(xorWithPrevious);

        final int blockSize = 64 - leadingZeros - trailingZeros;
        final int expectedSize = LEADING_ZEROS_LENGTH_BITS + BLOCK_SIZE_LENGTH_BITS + blockSize;
        final int previousBlockInformationSize = 64 - previousTrailingZeros - previousLeadingZeros;

        if (leadingZeros >= previousLeadingZeros &&
            trailingZeros >= previousTrailingZeros &&
            previousBlockInformationSize < expectedSize) {
            write(1, 1); // Control bit for using previous block information.

            final long blockValue = xorWithPrevious >> previousTrailingZeros;
            write(blockValue, previousBlockInformationSize);
        } else {
            write(0, 1); // Control bit for not using previous block information.

            write(leadingZeros, LEADING_ZEROS_LENGTH_BITS);

            write(blockSize - BLOCK_SIZE_ADJUSTMENT, BLOCK_SIZE_LENGTH_BITS); // To fit in 6 bits. There will never be a zero size block

            final long blockValue = xorWithPrevious >> trailingZeros;
            write(blockValue, blockSize);

            previousTrailingZeros = (byte) trailingZeros;
            previousLeadingZeros = (byte) leadingZeros;
        }

        previousValue = longValue;
    }

    public long readFirstTimeStamp() {
        previousTimestampDelta = DEFAULT_DELTA;
        previousTimestamp = read(BITS_FOR_FIRST_TIMESTAMP);
        return previousTimestamp;
    }

    public long readNextTimestamp() {
        final int type = findTheFirstZeroBit(4);
        if (type > 0) { // Delta of delta is non zero. Calculate the new delta. `index` will be used to find the right length for the value that is read.
            final int index = type - 1;
            long decodedValue = read(TIMESTAMP_ENCODINGS[index].bitsForValue);

            decodedValue -= ((long)1 << (TIMESTAMP_ENCODINGS[index].bitsForValue - 1)); // [0,255] becomes [-128,127]
            if (decodedValue >= 0) {
                decodedValue++; // [-128,127] becomes [-128,128] without the zero in the middle
            }

            previousTimestampDelta += decodedValue;
        }

        previousTimestamp += previousTimestampDelta;
        return previousTimestamp;
    }

    public double readNextValue() {
        final int nonZeroValue = (int) read(1);

        if (nonZeroValue == 0) {
            return Double.longBitsToDouble(previousValue);
        }

        final int usePreviousBlockInformation = (int) read(1);

        long xorValue;
        if (usePreviousBlockInformation != 0) {
            xorValue = read(64 - previousLeadingZeros - previousTrailingZeros);
            xorValue <<= previousTrailingZeros;
        } else {
            final byte leadingZeros = (byte) read(LEADING_ZEROS_LENGTH_BITS);
            final long blockSize = read(BLOCK_SIZE_LENGTH_BITS) + BLOCK_SIZE_ADJUSTMENT;
            previousTrailingZeros = (byte) (64 - blockSize - leadingZeros);
            xorValue = read((int) blockSize);
            xorValue <<= previousTrailingZeros;
            previousLeadingZeros = leadingZeros;
        }

        final long value = xorValue ^ previousValue;
        previousValue = value;

        return Double.longBitsToDouble(value);
    }

    private void addValueToBitString(long value) {
        final int bitsAvailable = ((bitPos & 0x7) > 0) ? (8 - (bitPos & 0x7)) : 0;
        int bytePos = bitPos >> 3; // the next buffer offset to write to, [byte]
        bitPos += bitsInValue;

        if (bitsInValue <= bitsAvailable) { // Everything fits inside the last byte
            buffer[bytePos] += (value << (bitsAvailable - bitsInValue));
            return;
        }

        int bitsLeft = bitsInValue;
        if (bitsAvailable > 0) { // Fill up the last byte
            buffer[bytePos++] += (value >> (bitsInValue - bitsAvailable));
            bitsLeft -= bitsAvailable;
        }

        while (bitsLeft >= 8) { // Enough bits for a dedicated byte
            byte ch = (byte) ((value >> (bitsLeft - 8)) & 0xFF);
            buffer[bytePos++] = ch;
            bitsLeft -= 8;
        }

        if (bitsLeft != 0) { // Start a new byte with the rest of the bits
            byte ch = (byte) ((value & ((1 << bitsLeft) - 1)) << (8 - bitsLeft));
            buffer[bytePos] = ch;
        }
    }

    private long readValueFromBitString() {
        long value = 0;

        for (int i = 0; i < bitsInValue; i++) {
            value <<= 1;
            long bit = (buffer[bitPos >> 3] >> (7 - (bitPos & 0x7))) & 1;
            value += bit;
            bitPos++;
        }

        return value;
    }

    private int findTheFirstZeroBit(int limit) {
        int bits = 0;

        while (bits < limit) {
            bitsInValue = 1;
            final int bit = (int) readValueFromBitString();
            if (bit == 0) {
                return bits;
            }

            bits++;
        }

        return bits;
    }

    private void write(long value, int bitsInValue) {
        this.bitsInValue = bitsInValue;
        addValueToBitString(value);
    }

    private long read(int bitsInValue) {
        this.bitsInValue = bitsInValue;
        return readValueFromBitString();
    }
    
    private static final class Encodings {
        final int bitsForValue;
        final int controlValue;
        final int controlValueBitLength;

        private Encodings(int bitsForValue, int controlValue, int controlValueBitLength) {
            this.bitsForValue = bitsForValue;
            this.controlValue = controlValue;
            this.controlValueBitLength = controlValueBitLength;
        }
    }
}
