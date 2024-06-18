package com.google.devtools.build.lib.runtime;

import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Represents both stdout and stderr streams in such a way that the relative ordering of data sent on each stream
 * is preserved.
 */
public final class CombinedOutput {
    // TODO: maxBufferedLength, maxChunkSize

    public static final byte STDOUT = 1;
    public static final byte STDERR = 2;

    private static final int CHUNK_HEADER_SIZE = 5;
    private static final int BUFFER_INITIAL_SIZE = 4096;

    private final int maxBufferedLength;
    private final int maxChunkSize;

    private final ComponentStream stdout = new ComponentStream(STDOUT);
    private final ComponentStream stderr = new ComponentStream(STDERR);

    private byte[] buf;
    private long count;

    // The event streamer that is supposed to flush stdout/stderr.
    private BuildEventStreamer streamer;

    public CombinedOutput(int maxBufferedLength, int maxChunkSize) {
        this.maxBufferedLength = maxBufferedLength;
        this.maxChunkSize = Math.max(maxChunkSize, maxBufferedLength);
        this.buf = new byte[BUFFER_INITIAL_SIZE];
    }

    private final class ComponentStream extends OutputStream {
        private final byte fileNo;

        ComponentStream(byte fileNo) {
            this.fileNo = fileNo;
        }

        @Override public void write(int oneByte) throws IOException {
            // We don't want this single-byte variant to be used because it's inefficient (each byte would
            // have its own header attached). So just throw an exception here.
            throw new IOException("unexpected single-byte write call to ComponentStream");
        }

        @Override
        public void write(byte[] buf, int offset, int n) throws IOException {
            CombinedOutput.this.write(this.fileNo, buf, offset, n);
        }
    }

    /** Returns the stdout component stream. */
    public OutputStream getStdout() {
        return stdout;
    }

    /** Returns the stderr component stream. */
    public OutputStream getStderr() {
        return stderr;
    }

    public static class Chunk {
        public final int fileNo;
        public final String content;

        private Chunk(int fileNo, String content) {
            this.fileNo = fileNo;
            this.content = content;
        }
    }

    /**
     * Consumes all buffered chunks.
     */
    public synchronized Iterable<Chunk> getChunks() {
        ArrayList<Chunk> chunks = new ArrayList<>();
        int i = 0;
        while (count > 0) {
            if (count < CHUNK_HEADER_SIZE) {
                throw new RuntimeException("invalid buffer state: buffered byte count " + count + " is less than header size " + CHUNK_HEADER_SIZE);
            }

            // Read header
            byte fileNo = buf[i];
            i += 1;

            ByteBuffer buffer = ByteBuffer.wrap(buf, i, 4);
            buffer.order(ByteOrder.BIG_ENDIAN);
            int payloadLength = buffer.getInt();
            i += 4;

            // Note: this will throw an exception if the remaining byte count is less than the payload length.
            String content = new String(buf, i, payloadLength, UTF_8);
            i += payloadLength;

            // Split content into multiple chunks to avoid exceeding maxChunkSize.
            // Note: we chunk here according to string length, not byte length, for simplicity.
            // So in practice, we may slightly exceed the max chunk size due to multi-byte UTF-8 sequences.
            while (!content.isEmpty()) {
                int chunkSize = Math.min(content.length(), maxChunkSize);
                chunks.add(new Chunk(fileNo, content.substring(0, chunkSize)));
                content = content.substring(chunkSize);
            }

            count -= (CHUNK_HEADER_SIZE + payloadLength);
        }
        count = 0;
        if (buf.length > BUFFER_INITIAL_SIZE) {
            buf = new byte[BUFFER_INITIAL_SIZE];
        }
        return chunks;
    }

    /**
     * Writes the given contents to the combined stream, tagged with the given file number.
     * This function is only called by one of the two component streams.
     */
    private void write(byte fileNo, byte[] payload, int payloadOffset, int payloadLength) {
        // As we have to do the flushing outside the synchronized block, we have to expect
        // other writes to come immediately after flushing, so we have to do the check inside
        // a while loop.
        boolean didWrite = false;
        while (!didWrite) {
            boolean shouldFlush = false;
            synchronized (this) {
                // Frame the payload with a 5-byte header:
                // - 1 byte for the file number (1 or 2)
                // - 4 bytes for the payload length (big-endian 32-bit int).
                byte[] header = new byte[5];
                header[0] = fileNo;
                ByteBuffer lengthBuf = ByteBuffer.wrap(header, 1, 4);
                lengthBuf.order(ByteOrder.BIG_ENDIAN);
                lengthBuf.putInt(payloadLength);
                int frameLength = header.length + payloadLength;

                // If writing the buf would put us over maxBufferedLength, we should flush before buffering more
                // data (unless the buffer is already empty, in which case flushing won't help).
                if (this.count + (long) frameLength < maxBufferedLength || this.count == 0) {
                    this.resizeBuffer(frameLength);
                    System.arraycopy(header, 0, this.buf, (int) this.count, header.length);
                    System.arraycopy(payload, payloadOffset, this.buf, (int) this.count + header.length, payloadLength);
                    this.count += frameLength;
                    didWrite = true;
                }
                // Compute shouldFlush inside the synchronized block because it references this.count,
                // which can change during concurrent calls to write().
                shouldFlush = this.count >= maxBufferedLength;
            }
            if (shouldFlush && streamer != null) {
                streamer.flush();
            }
        }
    }

    /**
     * Resizes the buffer to allow holding up to the given number of additional bytes.
     */
    private void resizeBuffer(int count) {
        if (this.count + (long) count < (long) buf.length) {
            return;
        }
        // We need to increase the buffer; if within the permissible range range for array
        // sizes, we at least double it, otherwise we only increase as far as needed.
        long newsize;
        if (2 * (long) buf.length + count < (long) Integer.MAX_VALUE) {
            newsize = 2 * (long) buf.length + count;
        } else {
            newsize = this.count + count;
        }
        byte[] newbuf = new byte[(int) newsize];
        System.arraycopy(buf, 0, newbuf, 0, (int) this.count);
        this.buf = newbuf;
    }
}
