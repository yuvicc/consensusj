package org.consensusj.bitcoin.db.schema;

import org.bitcoinj.base.Sha256Hash;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * Schema definitions for Bitcoin blockchain RocksDB database.
 *
 * This class defines the column families and key formats used to
 * store Bitcoin blockchain data efficiently in RocksDB.
 *
 * Most data is stored in key-only rows (empty values) to optimize space.
 *
 * IMPORTANT: Block headers in the 'headers' CF are stored with the 80-byte
 * serialized header as the KEY and an empty value. They are NOT indexed by
 * height in the database. To get a header by height, you must load all headers
 * into memory and build a height index by following the chain from the tip.
 */
public class BlockchainSchema {

    /**
     * Column family names used in the database
     */
    public static final class ColumnFamilies {
        /** Transaction outputs' index - maps script hash prefix to block height */
        public static final String FUNDING = "funding";

        /** Transaction inputs' index - maps outpoint prefix to spending height */
        public static final String SPENDING = "spending";

        /** Transaction ID index - maps txid prefix to confirmed block height */
        public static final String TXID = "txid";

        /** Block headers - stored as 80-byte keys */
        public static final String HEADERS = "headers";

        /** Configuration storage */
        public static final String CONFIG = "config";

        /** Default column family (required by RocksDB) */
        public static final String DEFAULT = "default";

        /** All column families in order */
        public static final String[] ALL = {
            DEFAULT, FUNDING, SPENDING, TXID, HEADERS, CONFIG
        };
    }

    /**
     * Special keys used in various column families
     */
    public static final class Keys {
        /** Chain tip key in headers CF */
        public static final byte[] CHAIN_TIP = new byte[] { 'T' };

        /** Config key in config CF */
        public static final byte[] CONFIG = new byte[] { 'C' };
    }

    /**
     * Prefix length for hash-based lookups (8 bytes)
     * 8-byte prefixes are used for both script hashes and transaction IDs
     */
    public static final int PREFIX_LENGTH = 8;

    /**
     * Block header size (80 bytes)
     * Bitcoin block headers are always 80 bytes when serialized
     */
    public static final int HEADER_SIZE = 80;

    /**
     * Creates a funding key: SHA256(script)[:8] + height
     * @param scriptHashPrefix First 8 bytes of SHA256(script)
     * @param height Block height
     * @return 12-byte key (8-byte prefix + 4-byte height)
     */
    public static byte[] fundingKey(byte[] scriptHashPrefix, int height) {
        if (scriptHashPrefix.length != PREFIX_LENGTH) {
            throw new IllegalArgumentException("Script hash prefix must be " + PREFIX_LENGTH + " bytes");
        }
        ByteBuffer buffer = ByteBuffer.allocate(PREFIX_LENGTH + 4);
        buffer.put(scriptHashPrefix);
        buffer.putInt(height);
        return buffer.array();
    }

    /**
     * Creates a spending key: txid[:8] + vout + height
     * @param txidPrefix First 8 bytes of transaction ID
     * @param vout Output index
     * @param height Block height where spending occurred
     * @return 14-byte key (8-byte txid prefix + 2-byte vout + 4-byte height)
     */
    public static byte[] spendingKey(byte[] txidPrefix, short vout, int height) {
        if (txidPrefix.length != PREFIX_LENGTH) {
            throw new IllegalArgumentException("Txid prefix must be " + PREFIX_LENGTH + " bytes");
        }
        ByteBuffer buffer = ByteBuffer.allocate(PREFIX_LENGTH + 2 + 4);
        buffer.put(txidPrefix);
        buffer.putShort(vout);
        buffer.putInt(height);
        return buffer.array();
    }

    /**
     * Creates a txid lookup key: txid[:8]
     * @param txid Full transaction ID
     * @return 8-byte prefix
     */
    public static byte[] txidKey(Sha256Hash txid) {
        return Arrays.copyOfRange(txid.getBytes(), 0, PREFIX_LENGTH);
    }

    /**
     * Creates a txid lookup key from raw bytes
     * @param txidBytes Transaction ID bytes
     * @return 8-byte prefix
     */
    public static byte[] txidKey(byte[] txidBytes) {
        return Arrays.copyOfRange(txidBytes, 0, PREFIX_LENGTH);
    }

    /**
     * DEPRECATED: This method does not match the database schema.
     *
     * Headers are stored with the 80-byte header as the KEY, not the height.
     * This method is kept for backward compatibility but should not be used.
     *
     * @deprecated Headers are not indexed by height in RocksDB.
     *             Load all headers and build an in-memory index instead.
     * @param height Block height
     * @return 4-byte height in big-endian format (INCORRECT for this schema)
     */
    @Deprecated
    public static byte[] headerKey(int height) {
        return ByteBuffer.allocate(4)
                .order(ByteOrder.BIG_ENDIAN)
                .putInt(height)
                .array();
    }

    /**
     * DEPRECATED: This method does not match the database schema.
     *
     * @deprecated Headers are stored as 80-byte keys, not 4-byte heights.
     * @param key 4-byte header key
     * @return Block height
     */
    @Deprecated
    public static int decodeHeaderHeight(byte[] key) {
        return ByteBuffer.wrap(key).order(ByteOrder.BIG_ENDIAN).getInt();
    }

    /**
     * Decodes block height from a funding or spending key value
     * @param value RocksDB value containing block height
     * @return Block height
     */
    public static int decodeHeight(byte[] value) {
        if (value.length != 4) {
            throw new IllegalArgumentException("Height value must be 4 bytes, got " + value.length);
        }
        return ByteBuffer.wrap(value).getInt();
    }

    /**
     * Encodes block height as 4-byte value
     * @param height Block height
     * @return 4-byte array
     */
    public static byte[] encodeHeight(int height) {
        return ByteBuffer.allocate(4).putInt(height).array();
    }

    /**
     * Extracts the script hash prefix from a funding key
     * @param fundingKey 12-byte funding key
     * @return 8-byte script hash prefix
     */
    public static byte[] extractScriptHashPrefix(byte[] fundingKey) {
        return Arrays.copyOfRange(fundingKey, 0, PREFIX_LENGTH);
    }

    /**
     * Extracts the height from a funding key
     * @param fundingKey 12-byte funding key
     * @return Block height
     */
    public static int extractFundingHeight(byte[] fundingKey) {
        return ByteBuffer.wrap(fundingKey, PREFIX_LENGTH, 4).getInt();
    }

    /**
     * Extracts the txid prefix from a spending key
     * @param spendingKey 14-byte spending key
     * @return 8-byte txid prefix
     */
    public static byte[] extractTxidPrefix(byte[] spendingKey) {
        return Arrays.copyOfRange(spendingKey, 0, PREFIX_LENGTH);
    }

    /**
     * Extracts the vout from a spending key
     * @param spendingKey 14-byte spending key
     * @return Output index
     */
    public static short extractVout(byte[] spendingKey) {
        return ByteBuffer.wrap(spendingKey, PREFIX_LENGTH, 2).getShort();
    }

    /**
     * Extracts the height from a spending key
     * @param spendingKey 14-byte spending key
     * @return Block height
     */
    public static int extractSpendingHeight(byte[] spendingKey) {
        return ByteBuffer.wrap(spendingKey, PREFIX_LENGTH + 2, 4).getInt();
    }

    /**
     * Creates a funding value: txid[:8] + vout
     * @param txidPrefix First 8 bytes of transaction ID
     * @param vout Output index
     * @return 10-byte value (8-byte txid prefix + 2-byte vout)
     */
    public static byte[] fundingValue(byte[] txidPrefix, short vout) {
        if (txidPrefix.length != PREFIX_LENGTH) {
            throw new IllegalArgumentException("Txid prefix must be " + PREFIX_LENGTH + " bytes");
        }
        ByteBuffer buffer = ByteBuffer.allocate(PREFIX_LENGTH + 2);
        buffer.put(txidPrefix);
        buffer.putShort(vout);
        return buffer.array();
    }

    /**
     * Creates a spending value: spending_txid[:8] + vin
     * @param spendingTxidPrefix First 8 bytes of spending transaction ID
     * @param vin Input index in spending transaction
     * @return 12-byte value (8-byte spending txid prefix + 4-byte vin)
     */
    public static byte[] spendingValue(byte[] spendingTxidPrefix, int vin) {
        if (spendingTxidPrefix.length != PREFIX_LENGTH) {
            throw new IllegalArgumentException("Spending txid prefix must be " + PREFIX_LENGTH + " bytes");
        }
        ByteBuffer buffer = ByteBuffer.allocate(PREFIX_LENGTH + 4);
        buffer.put(spendingTxidPrefix);
        buffer.putInt(vin);
        return buffer.array();
    }
}
