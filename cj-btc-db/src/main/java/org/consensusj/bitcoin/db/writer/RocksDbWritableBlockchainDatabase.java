package org.consensusj.bitcoin.db.writer;

import org.bitcoinj.base.Sha256Hash;
import org.bitcoinj.core.Block;
import org.bitcoinj.core.NetworkParameters;
import org.consensusj.bitcoin.db.schema.BlockchainSchema;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * RocksDB-based implementation of {@link WritableBlockchainDatabase} for reading and writing blockchain databases.
 *
 * This class provides read-write access to Bitcoin blockchain data stored in the RocksDB format.
 * It opens an blockchain RocksDB database and allows both querying and updating the blockchain data.
 *
 * All write operations use RocksDB WriteBatch for atomicity across multiple column families.
 * The database must be explicitly flushed using {@link #flush()} to commit pending writes.
 *
 * Example usage:
 * <pre>{@code
 * Path dbPath = Path.of("/path/to/blockchain/db");
 * try (RocksDbWritableBlockchainDatabase db = new RocksDbWritableBlockchainDatabase(dbPath, NetworkParameters.fromID(NetworkParameters.ID_MAINNET))) {
 *     // Write operations
 *     db.putBlockHeader(height, block);
 *     db.putTransactionHeight(txid, height);
 *     db.flush();  // Commit all writes
 * }
 * }</pre>
 */
public class RocksDbWritableBlockchainDatabase implements WritableBlockchainDatabase {
    private static final Logger log = LoggerFactory.getLogger(RocksDbWritableBlockchainDatabase.class);

    private final RocksDB db;
    private final List<ColumnFamilyHandle> columnFamilyHandles;
    private final ColumnFamilyHandle fundingCF;
    private final ColumnFamilyHandle spendingCF;
    private final ColumnFamilyHandle txidCF;
    private final ColumnFamilyHandle headersCF;
    private final ColumnFamilyHandle configCF;
    private final NetworkParameters networkParameters;
    private final WriteBatch writeBatch;
    private final WriteOptions writeOptions;
    private final Map<Integer, Block> headersByHeight;
    private final Map<Sha256Hash, Block> headersByHash;

    /**
     * Opens an existing blockchain RocksDB database in read-write mode
     *
     * @param dbPath Path to the blockchain database directory
     * @param networkParameters Bitcoin network parameters for serializing/deserializing blocks
     * @throws RocksDBException if the database cannot be opened
     */
    public RocksDbWritableBlockchainDatabase(Path dbPath, NetworkParameters networkParameters) throws RocksDBException {
        this.networkParameters = networkParameters;
        this.columnFamilyHandles = new ArrayList<>();
        this.writeBatch = new WriteBatch();
        this.writeOptions = new WriteOptions();
        this.headersByHeight = new HashMap<>();
        this.headersByHash = new HashMap<>();

        // Load RocksDB library
        RocksDB.loadLibrary();

        // Create column family descriptors
        List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
        for (String cfName : BlockchainSchema.ColumnFamilies.ALL) {
            columnFamilyDescriptors.add(new ColumnFamilyDescriptor(cfName.getBytes()));
        }

        // Open database in read-write mode
        DBOptions options = new DBOptions()
                .setCreateIfMissing(false)
                .setCreateMissingColumnFamilies(false);

        try {
            this.db = RocksDB.open(options, dbPath.toString(), columnFamilyDescriptors, columnFamilyHandles);

            // Map column family handles
            this.fundingCF = findColumnFamily(BlockchainSchema.ColumnFamilies.FUNDING);
            this.spendingCF = findColumnFamily(BlockchainSchema.ColumnFamilies.SPENDING);
            this.txidCF = findColumnFamily(BlockchainSchema.ColumnFamilies.TXID);
            this.headersCF = findColumnFamily(BlockchainSchema.ColumnFamilies.HEADERS);
            this.configCF = findColumnFamily(BlockchainSchema.ColumnFamilies.CONFIG);

            log.info("Opened writable blockchain database at {} with {} column families", dbPath, columnFamilyHandles.size());

            // Load all headers into memory and build the chain
            loadHeaders();
            log.info("Loaded {} block headers into memory", headersByHeight.size());
        } catch (RocksDBException e) {
            // Clean up on failure
            columnFamilyHandles.forEach(ColumnFamilyHandle::close);
            writeBatch.close();
            writeOptions.close();
            options.close();
            throw e;
        }
    }

    /**
     * Creates a new blockchain RocksDB database with all required column families
     *
     * @param dbPath Path where the new database should be created
     * @param networkParameters Bitcoin network parameters
     * @return A new writable database instance
     * @throws RocksDBException if the database cannot be created
     */
    public static RocksDbWritableBlockchainDatabase create(Path dbPath, NetworkParameters networkParameters) throws RocksDBException {
        // Load RocksDB library
        RocksDB.loadLibrary();

        List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
        List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

        // Create column family descriptors
        for (String cfName : BlockchainSchema.ColumnFamilies.ALL) {
            columnFamilyDescriptors.add(new ColumnFamilyDescriptor(cfName.getBytes()));
        }

        // Create database with all column families
        DBOptions options = new DBOptions()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true);

        try (RocksDB db = RocksDB.open(options, dbPath.toString(), columnFamilyDescriptors, columnFamilyHandles)) {
            columnFamilyHandles.forEach(ColumnFamilyHandle::close);
            log.info("Created new blockchain database at {}", dbPath);
        } finally {
            options.close();
        }

        // Now open it in read-write mode
        return new RocksDbWritableBlockchainDatabase(dbPath, networkParameters);
    }

    private ColumnFamilyHandle findColumnFamily(String name) {
        for (int i = 0; i < BlockchainSchema.ColumnFamilies.ALL.length; i++) {
            if (BlockchainSchema.ColumnFamilies.ALL[i].equals(name)) {
                return columnFamilyHandles.get(i);
            }
        }
        throw new IllegalStateException("Column family not found: " + name);
    }

    /**
     * Loads all block headers from the database into memory and builds the chain.
     * Headers are stored with the 80-byte header as the key.
     * This method reads all headers, then follows the chain from the tip backwards
     * to assign heights to each header.
     */
    private void loadHeaders() throws RocksDBException {
        // First, load all headers into a map by hash
        Map<Sha256Hash, Block> headerMap = new HashMap<>();

        try (RocksIterator iterator = db.newIterator(headersCF)) {
            iterator.seekToFirst();

            while (iterator.isValid()) {
                byte[] key = iterator.key();

                // Skip the chain tip key
                if (key.length == 1 && key[0] == 'T') {
                    iterator.next();
                    continue;
                }

                // Only process keys that are 80 bytes (block headers)
                if (key.length == BlockchainSchema.HEADER_SIZE) {
                    try {
                        Block block = networkParameters.getDefaultSerializer().makeBlock(java.nio.ByteBuffer.wrap(key));
                        Sha256Hash hash = block.getHash();
                        headerMap.put(hash, block);
                    } catch (org.bitcoinj.core.ProtocolException e) {
                        log.warn("Failed to deserialize header, skipping: {}", e.getMessage());
                    }
                }

                iterator.next();
            }
        }

        log.debug("Read {} headers from database", headerMap.size());

        // Get the chain tip
        Optional<Sha256Hash> tipHashOpt = getChainTipHash();
        if (!tipHashOpt.isPresent()) {
            log.warn("No chain tip found, cannot build height index");
            return;
        }

        Sha256Hash tipHash = tipHashOpt.get();
        log.debug("Chain tip: {}", tipHash);

        // Follow the chain backwards from the tip to assign heights
        Sha256Hash currentHash = tipHash;
        int height = 0;

        // First, count the chain length by following backwards to find the height
        List<Sha256Hash> chain = new ArrayList<>();
        while (headerMap.containsKey(currentHash)) {
            Block header = headerMap.get(currentHash);
            chain.add(currentHash);
            currentHash = header.getPrevBlockHash();
        }

        // Now assign heights (chain is in reverse order, so reverse it)
        height = chain.size() - 1;
        for (int i = chain.size() - 1; i >= 0; i--) {
            Sha256Hash hash = chain.get(i);
            Block header = headerMap.get(hash);
            int assignedHeight = height - i;
            headersByHeight.put(assignedHeight, header);
            headersByHash.put(hash, header);
        }

        log.debug("Built chain with {} headers, tip at height {}", headersByHeight.size(), height);
    }

    // Read operations (implementing BlockchainDatabase interface)

    @Override
    public Optional<Integer> getChainTipHeight() {
        if (headersByHeight.isEmpty()) {
            return Optional.empty();
        }
        // Find the maximum height in the map
        return Optional.of(headersByHeight.keySet().stream().max(Integer::compareTo).orElse(0));
    }

    @Override
    public Optional<Sha256Hash> getChainTipHash() {
        try {
            byte[] tipHash = db.get(headersCF, BlockchainSchema.Keys.CHAIN_TIP);
            if (tipHash == null) {
                return Optional.empty();
            }
            // Electrs stores hashes in internal byte order, BitcoinJ expects reversed
            byte[] reversedHash = new byte[tipHash.length];
            for (int i = 0; i < tipHash.length; i++) {
                reversedHash[i] = tipHash[tipHash.length - 1 - i];
            }
            return Optional.of(Sha256Hash.wrap(reversedHash));
        } catch (RocksDBException e) {
            log.error("Error getting chain tip hash", e);
            return Optional.empty();
        }
    }

    @Override
    public Optional<Block> getBlockHeader(int height) {
        return Optional.ofNullable(headersByHeight.get(height));
    }

    @Override
    public Optional<Integer> getTransactionHeight(Sha256Hash txid) {
        try {
            byte[] key = BlockchainSchema.txidKey(txid);
            byte[] heightBytes = db.get(txidCF, key);

            if (heightBytes == null) {
                return Optional.empty();
            }

            return Optional.of(BlockchainSchema.decodeHeight(heightBytes));
        } catch (RocksDBException e) {
            log.error("Error getting transaction height for {}", txid, e);
            return Optional.empty();
        }
    }

    @Override
    public List<Integer> getFundingHeights(byte[] scriptHashPrefix) {
        if (scriptHashPrefix.length != BlockchainSchema.PREFIX_LENGTH) {
            throw new IllegalArgumentException("Script hash prefix must be " + BlockchainSchema.PREFIX_LENGTH + " bytes");
        }

        List<Integer> heights = new ArrayList<>();
        try (RocksIterator iterator = db.newIterator(fundingCF)) {
            iterator.seek(scriptHashPrefix);

            while (iterator.isValid()) {
                byte[] key = iterator.key();
                if (!startsWithPrefix(key, scriptHashPrefix)) {
                    break;
                }

                int height = BlockchainSchema.extractFundingHeight(key);
                heights.add(height);
                iterator.next();
            }
        }

        return heights;
    }

    @Override
    public List<Integer> getSpendingHeights(Sha256Hash txid, int vout) {
        byte[] txidPrefix = BlockchainSchema.txidKey(txid);
        List<Integer> heights = new ArrayList<>();

        try (RocksIterator iterator = db.newIterator(spendingCF)) {
            byte[] prefixKey = BlockchainSchema.spendingKey(txidPrefix, (short) vout, 0);
            iterator.seek(java.util.Arrays.copyOfRange(prefixKey, 0, BlockchainSchema.PREFIX_LENGTH + 2));

            while (iterator.isValid()) {
                byte[] key = iterator.key();

                byte[] keyTxidPrefix = BlockchainSchema.extractTxidPrefix(key);
                short keyVout = BlockchainSchema.extractVout(key);

                if (!java.util.Arrays.equals(keyTxidPrefix, txidPrefix) || keyVout != vout) {
                    break;
                }

                int height = BlockchainSchema.extractSpendingHeight(key);
                heights.add(height);
                iterator.next();
            }
        }

        return heights;
    }

    @Override
    public boolean isOpen() {
        return db != null && db.isOwningHandle();
    }

    @Override
    public DatabaseStats getStats() {
        long fundingKeys = estimateNumKeys(fundingCF);
        long spendingKeys = estimateNumKeys(spendingCF);
        long txidKeys = estimateNumKeys(txidCF);
        long headerKeys = estimateNumKeys(headersCF);
        long totalSize = 0;

        try {
            String sizeStr = db.getProperty("rocksdb.total-sst-files-size");
            totalSize = Long.parseLong(sizeStr);
        } catch (RocksDBException e) {
            log.warn("Could not get database size", e);
        }

        return new DatabaseStats(fundingKeys, spendingKeys, txidKeys, headerKeys, totalSize);
    }

    // Write operations (implementing WritableBlockchainDatabase interface)

    @Override
    public void putBlockHeader(int height, Block block) throws RocksDBException {
        // In this schema, headers are stored with the 80-byte header as the key and empty value
        byte[] key = block.serialize();
        byte[] value = new byte[0]; // Empty value
        writeBatch.put(headersCF, key, value);

        // Update in-memory index
        headersByHeight.put(height, block);
        headersByHash.put(block.getHash(), block);
    }

    @Override
    public void updateChainTip(int height, Sha256Hash hash) throws RocksDBException {
        // Electrs stores hashes in internal byte order (reversed from BitcoinJ)
        byte[] hashBytes = hash.getBytes();
        byte[] reversedHash = new byte[hashBytes.length];
        for (int i = 0; i < hashBytes.length; i++) {
            reversedHash[i] = hashBytes[hashBytes.length - 1 - i];
        }
        writeBatch.put(headersCF, BlockchainSchema.Keys.CHAIN_TIP, reversedHash);
    }

    @Override
    public void putTransactionHeight(Sha256Hash txid, int height) throws RocksDBException {
        byte[] key = BlockchainSchema.txidKey(txid);
        byte[] value = BlockchainSchema.encodeHeight(height);
        writeBatch.put(txidCF, key, value);
    }

    @Override
    public void putFunding(byte[] scriptHashPrefix, int height, byte[] txidPrefix, short vout) throws RocksDBException {
        if (scriptHashPrefix.length != BlockchainSchema.PREFIX_LENGTH) {
            throw new IllegalArgumentException("Script hash prefix must be " + BlockchainSchema.PREFIX_LENGTH + " bytes");
        }
        if (txidPrefix.length != BlockchainSchema.PREFIX_LENGTH) {
            throw new IllegalArgumentException("Txid prefix must be " + BlockchainSchema.PREFIX_LENGTH + " bytes");
        }

        byte[] key = BlockchainSchema.fundingKey(scriptHashPrefix, height);
        byte[] value = BlockchainSchema.fundingValue(txidPrefix, vout);
        writeBatch.put(fundingCF, key, value);
    }

    @Override
    public void putSpending(byte[] txidPrefix, short vout, int height, byte[] spendingTxidPrefix, int vin) throws RocksDBException {
        if (txidPrefix.length != BlockchainSchema.PREFIX_LENGTH) {
            throw new IllegalArgumentException("Txid prefix must be " + BlockchainSchema.PREFIX_LENGTH + " bytes");
        }
        if (spendingTxidPrefix.length != BlockchainSchema.PREFIX_LENGTH) {
            throw new IllegalArgumentException("Spending txid prefix must be " + BlockchainSchema.PREFIX_LENGTH + " bytes");
        }

        byte[] key = BlockchainSchema.spendingKey(txidPrefix, vout, height);
        byte[] value = BlockchainSchema.spendingValue(spendingTxidPrefix, vin);
        writeBatch.put(spendingCF, key, value);
    }

    @Override
    public void putConfig(String key, byte[] value) throws RocksDBException {
        writeBatch.put(configCF, key.getBytes(), value);
    }

    @Override
    public void deleteBlockHeader(int height) throws RocksDBException {
        // Get the block from in-memory index to find its hash
        Block block = headersByHeight.get(height);
        if (block != null) {
            // Delete using the 80-byte header as the key
            byte[] key = block.serialize();
            writeBatch.delete(headersCF, key);

            // Remove from in-memory index
            headersByHeight.remove(height);
            headersByHash.remove(block.getHash());
        }
    }

    @Override
    public void deleteTransactionHeight(Sha256Hash txid) throws RocksDBException {
        byte[] key = BlockchainSchema.txidKey(txid);
        writeBatch.delete(txidCF, key);
    }

    @Override
    public void deleteFunding(byte[] scriptHashPrefix, int height, byte[] txidPrefix, short vout) throws RocksDBException {
        if (scriptHashPrefix.length != BlockchainSchema.PREFIX_LENGTH) {
            throw new IllegalArgumentException("Script hash prefix must be " + BlockchainSchema.PREFIX_LENGTH + " bytes");
        }

        byte[] key = BlockchainSchema.fundingKey(scriptHashPrefix, height);
        writeBatch.delete(fundingCF, key);
    }

    @Override
    public void deleteSpending(byte[] txidPrefix, short vout, int height, byte[] spendingTxidPrefix, int vin) throws RocksDBException {
        if (txidPrefix.length != BlockchainSchema.PREFIX_LENGTH) {
            throw new IllegalArgumentException("Txid prefix must be " + BlockchainSchema.PREFIX_LENGTH + " bytes");
        }

        byte[] key = BlockchainSchema.spendingKey(txidPrefix, vout, height);
        writeBatch.delete(spendingCF, key);
    }

    @Override
    public void flush() throws RocksDBException {
        db.write(writeOptions, writeBatch);
        writeBatch.clear();
        log.debug("Flushed write batch to database");
    }

    @Override
    public void close() {
        try {
            // Flush any pending writes before closing
            if (writeBatch.count() > 0) {
                log.warn("Closing database with {} pending writes - flushing", writeBatch.count());
                flush();
            }
        } catch (RocksDBException e) {
            log.error("Error flushing pending writes on close", e);
        }

        log.info("Closing writable blockchain database");
        writeBatch.close();
        writeOptions.close();
        columnFamilyHandles.forEach(ColumnFamilyHandle::close);
        if (db != null) {
            db.close();
        }
    }

    // Helper methods

    private boolean startsWithPrefix(byte[] key, byte[] prefix) {
        if (key.length < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (key[i] != prefix[i]) {
                return false;
            }
        }
        return true;
    }

    private long estimateNumKeys(ColumnFamilyHandle cf) {
        try {
            String numKeysStr = db.getProperty(cf, "rocksdb.estimate-num-keys");
            return Long.parseLong(numKeysStr);
        } catch (RocksDBException e) {
            log.warn("Could not estimate number of keys", e);
            return 0;
        }
    }

}
