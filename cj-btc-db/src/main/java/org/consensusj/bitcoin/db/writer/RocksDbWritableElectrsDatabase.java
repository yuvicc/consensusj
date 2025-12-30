package org.consensusj.bitcoin.db.writer;

import org.bitcoinj.base.Sha256Hash;
import org.bitcoinj.core.Block;
import org.bitcoinj.core.NetworkParameters;
import org.consensusj.bitcoin.db.schema.ElectrsSchema;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * RocksDB-based implementation of {@link WritableElectrsDatabase} for reading and writing electrs databases.
 *
 * This class provides read-write access to Bitcoin blockchain data stored in the electrs format.
 * It opens an electrs RocksDB database and allows both querying and updating the blockchain data.
 *
 * All write operations use RocksDB WriteBatch for atomicity across multiple column families.
 * The database must be explicitly flushed using {@link #flush()} to commit pending writes.
 *
 * Example usage:
 * <pre>{@code
 * Path dbPath = Path.of("/path/to/electrs/db");
 * try (RocksDbWritableElectrsDatabase db = new RocksDbWritableElectrsDatabase(dbPath, NetworkParameters.fromID(NetworkParameters.ID_MAINNET))) {
 *     // Write operations
 *     db.putBlockHeader(height, block);
 *     db.putTransactionHeight(txid, height);
 *     db.flush();  // Commit all writes
 * }
 * }</pre>
 */
public class RocksDbWritableElectrsDatabase implements WritableElectrsDatabase {
    private static final Logger log = LoggerFactory.getLogger(RocksDbWritableElectrsDatabase.class);

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

    /**
     * Opens an existing electrs RocksDB database in read-write mode
     *
     * @param dbPath Path to the electrs database directory
     * @param networkParameters Bitcoin network parameters for serializing/deserializing blocks
     * @throws RocksDBException if the database cannot be opened
     */
    public RocksDbWritableElectrsDatabase(Path dbPath, NetworkParameters networkParameters) throws RocksDBException {
        this.networkParameters = networkParameters;
        this.columnFamilyHandles = new ArrayList<>();
        this.writeBatch = new WriteBatch();
        this.writeOptions = new WriteOptions();

        // Load RocksDB library
        RocksDB.loadLibrary();

        // Create column family descriptors
        List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
        for (String cfName : ElectrsSchema.ColumnFamilies.ALL) {
            columnFamilyDescriptors.add(new ColumnFamilyDescriptor(cfName.getBytes()));
        }

        // Open database in read-write mode
        DBOptions options = new DBOptions()
                .setCreateIfMissing(false)
                .setCreateMissingColumnFamilies(false);

        try {
            this.db = RocksDB.open(options, dbPath.toString(), columnFamilyDescriptors, columnFamilyHandles);

            // Map column family handles
            this.fundingCF = findColumnFamily(ElectrsSchema.ColumnFamilies.FUNDING);
            this.spendingCF = findColumnFamily(ElectrsSchema.ColumnFamilies.SPENDING);
            this.txidCF = findColumnFamily(ElectrsSchema.ColumnFamilies.TXID);
            this.headersCF = findColumnFamily(ElectrsSchema.ColumnFamilies.HEADERS);
            this.configCF = findColumnFamily(ElectrsSchema.ColumnFamilies.CONFIG);

            log.info("Opened writable electrs database at {} with {} column families", dbPath, columnFamilyHandles.size());
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
     * Creates a new electrs RocksDB database with all required column families
     *
     * @param dbPath Path where the new database should be created
     * @param networkParameters Bitcoin network parameters
     * @return A new writable database instance
     * @throws RocksDBException if the database cannot be created
     */
    public static RocksDbWritableElectrsDatabase create(Path dbPath, NetworkParameters networkParameters) throws RocksDBException {
        // Load RocksDB library
        RocksDB.loadLibrary();

        List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
        List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

        // Create column family descriptors
        for (String cfName : ElectrsSchema.ColumnFamilies.ALL) {
            columnFamilyDescriptors.add(new ColumnFamilyDescriptor(cfName.getBytes()));
        }

        // Create database with all column families
        DBOptions options = new DBOptions()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true);

        try (RocksDB db = RocksDB.open(options, dbPath.toString(), columnFamilyDescriptors, columnFamilyHandles)) {
            columnFamilyHandles.forEach(ColumnFamilyHandle::close);
            log.info("Created new electrs database at {}", dbPath);
        } finally {
            options.close();
        }

        // Now open it in read-write mode
        return new RocksDbWritableElectrsDatabase(dbPath, networkParameters);
    }

    private ColumnFamilyHandle findColumnFamily(String name) {
        for (int i = 0; i < ElectrsSchema.ColumnFamilies.ALL.length; i++) {
            if (ElectrsSchema.ColumnFamilies.ALL[i].equals(name)) {
                return columnFamilyHandles.get(i);
            }
        }
        throw new IllegalStateException("Column family not found: " + name);
    }

    // Read operations (implementing ElectrsDatabase interface)

    @Override
    public Optional<Integer> getChainTipHeight() {
        try {
            byte[] tipData = db.get(headersCF, ElectrsSchema.Keys.CHAIN_TIP);
            if (tipData == null) {
                return Optional.empty();
            }
            return Optional.of(findHighestHeaderHeight());
        } catch (RocksDBException e) {
            log.error("Error getting chain tip height", e);
            return Optional.empty();
        }
    }

    @Override
    public Optional<Sha256Hash> getChainTipHash() {
        try {
            byte[] tipHash = db.get(headersCF, ElectrsSchema.Keys.CHAIN_TIP);
            if (tipHash == null) {
                return Optional.empty();
            }
            return Optional.of(Sha256Hash.wrap(tipHash));
        } catch (RocksDBException e) {
            log.error("Error getting chain tip hash", e);
            return Optional.empty();
        }
    }

    @Override
    public Optional<Block> getBlockHeader(int height) {
        try {
            byte[] key = ElectrsSchema.headerKey(height);
            byte[] headerBytes = db.get(headersCF, key);

            if (headerBytes == null) {
                return Optional.empty();
            }

            Block block = networkParameters.getDefaultSerializer().makeBlock(java.nio.ByteBuffer.wrap(headerBytes));
            return Optional.of(block);
        } catch (Exception e) {
            log.error("Error getting block header for height {}", height, e);
            return Optional.empty();
        }
    }

    @Override
    public Optional<Integer> getTransactionHeight(Sha256Hash txid) {
        try {
            byte[] key = ElectrsSchema.txidKey(txid);
            byte[] heightBytes = db.get(txidCF, key);

            if (heightBytes == null) {
                return Optional.empty();
            }

            return Optional.of(ElectrsSchema.decodeHeight(heightBytes));
        } catch (RocksDBException e) {
            log.error("Error getting transaction height for {}", txid, e);
            return Optional.empty();
        }
    }

    @Override
    public List<Integer> getFundingHeights(byte[] scriptHashPrefix) {
        if (scriptHashPrefix.length != ElectrsSchema.PREFIX_LENGTH) {
            throw new IllegalArgumentException("Script hash prefix must be " + ElectrsSchema.PREFIX_LENGTH + " bytes");
        }

        List<Integer> heights = new ArrayList<>();
        try (RocksIterator iterator = db.newIterator(fundingCF)) {
            iterator.seek(scriptHashPrefix);

            while (iterator.isValid()) {
                byte[] key = iterator.key();
                if (!startsWithPrefix(key, scriptHashPrefix)) {
                    break;
                }

                int height = ElectrsSchema.extractFundingHeight(key);
                heights.add(height);
                iterator.next();
            }
        }

        return heights;
    }

    @Override
    public List<Integer> getSpendingHeights(Sha256Hash txid, int vout) {
        byte[] txidPrefix = ElectrsSchema.txidKey(txid);
        List<Integer> heights = new ArrayList<>();

        try (RocksIterator iterator = db.newIterator(spendingCF)) {
            byte[] prefixKey = ElectrsSchema.spendingKey(txidPrefix, (short) vout, 0);
            iterator.seek(java.util.Arrays.copyOfRange(prefixKey, 0, ElectrsSchema.PREFIX_LENGTH + 2));

            while (iterator.isValid()) {
                byte[] key = iterator.key();

                byte[] keyTxidPrefix = ElectrsSchema.extractTxidPrefix(key);
                short keyVout = ElectrsSchema.extractVout(key);

                if (!java.util.Arrays.equals(keyTxidPrefix, txidPrefix) || keyVout != vout) {
                    break;
                }

                int height = ElectrsSchema.extractSpendingHeight(key);
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

    // Write operations (implementing WritableElectrsDatabase interface)

    @Override
    public void putBlockHeader(int height, Block block) throws RocksDBException {
        byte[] key = ElectrsSchema.headerKey(height);
        byte[] value = block.serialize();
        writeBatch.put(headersCF, key, value);
    }

    @Override
    public void updateChainTip(int height, Sha256Hash hash) throws RocksDBException {
        writeBatch.put(headersCF, ElectrsSchema.Keys.CHAIN_TIP, hash.getBytes());
    }

    @Override
    public void putTransactionHeight(Sha256Hash txid, int height) throws RocksDBException {
        byte[] key = ElectrsSchema.txidKey(txid);
        byte[] value = ElectrsSchema.encodeHeight(height);
        writeBatch.put(txidCF, key, value);
    }

    @Override
    public void putFunding(byte[] scriptHashPrefix, int height, byte[] txidPrefix, short vout) throws RocksDBException {
        if (scriptHashPrefix.length != ElectrsSchema.PREFIX_LENGTH) {
            throw new IllegalArgumentException("Script hash prefix must be " + ElectrsSchema.PREFIX_LENGTH + " bytes");
        }
        if (txidPrefix.length != ElectrsSchema.PREFIX_LENGTH) {
            throw new IllegalArgumentException("Txid prefix must be " + ElectrsSchema.PREFIX_LENGTH + " bytes");
        }

        byte[] key = ElectrsSchema.fundingKey(scriptHashPrefix, height);
        byte[] value = ElectrsSchema.fundingValue(txidPrefix, vout);
        writeBatch.put(fundingCF, key, value);
    }

    @Override
    public void putSpending(byte[] txidPrefix, short vout, int height, byte[] spendingTxidPrefix, int vin) throws RocksDBException {
        if (txidPrefix.length != ElectrsSchema.PREFIX_LENGTH) {
            throw new IllegalArgumentException("Txid prefix must be " + ElectrsSchema.PREFIX_LENGTH + " bytes");
        }
        if (spendingTxidPrefix.length != ElectrsSchema.PREFIX_LENGTH) {
            throw new IllegalArgumentException("Spending txid prefix must be " + ElectrsSchema.PREFIX_LENGTH + " bytes");
        }

        byte[] key = ElectrsSchema.spendingKey(txidPrefix, vout, height);
        byte[] value = ElectrsSchema.spendingValue(spendingTxidPrefix, vin);
        writeBatch.put(spendingCF, key, value);
    }

    @Override
    public void putConfig(String key, byte[] value) throws RocksDBException {
        writeBatch.put(configCF, key.getBytes(), value);
    }

    @Override
    public void deleteBlockHeader(int height) throws RocksDBException {
        byte[] key = ElectrsSchema.headerKey(height);
        writeBatch.delete(headersCF, key);
    }

    @Override
    public void deleteTransactionHeight(Sha256Hash txid) throws RocksDBException {
        byte[] key = ElectrsSchema.txidKey(txid);
        writeBatch.delete(txidCF, key);
    }

    @Override
    public void deleteFunding(byte[] scriptHashPrefix, int height, byte[] txidPrefix, short vout) throws RocksDBException {
        if (scriptHashPrefix.length != ElectrsSchema.PREFIX_LENGTH) {
            throw new IllegalArgumentException("Script hash prefix must be " + ElectrsSchema.PREFIX_LENGTH + " bytes");
        }

        byte[] key = ElectrsSchema.fundingKey(scriptHashPrefix, height);
        writeBatch.delete(fundingCF, key);
    }

    @Override
    public void deleteSpending(byte[] txidPrefix, short vout, int height, byte[] spendingTxidPrefix, int vin) throws RocksDBException {
        if (txidPrefix.length != ElectrsSchema.PREFIX_LENGTH) {
            throw new IllegalArgumentException("Txid prefix must be " + ElectrsSchema.PREFIX_LENGTH + " bytes");
        }

        byte[] key = ElectrsSchema.spendingKey(txidPrefix, vout, height);
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

        log.info("Closing writable electrs database");
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

    private int findHighestHeaderHeight() {
        int maxHeight = 0;
        try (RocksIterator iterator = db.newIterator(headersCF)) {
            iterator.seekToLast();
            if (iterator.isValid()) {
                byte[] key = iterator.key();
                if (key.length == 4) {
                    maxHeight = ElectrsSchema.decodeHeaderHeight(key);
                }
            }
        }
        return maxHeight;
    }
}
