package org.consensusj.bitcoin.db.reader;

import org.bitcoinj.base.Sha256Hash;
import org.bitcoinj.core.Block;
import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.core.ProtocolException;
import org.consensusj.bitcoin.db.schema.ElectrsSchema;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * RocksDB-based implementation of {@link ElectrsDatabase} for reading electrs databases.
 *
 * This class provides read-only access to Bitcoin blockchain data stored in the electrs format.
 * It opens an existing electrs RocksDB database and allows querying by script hash, transaction ID,
 * and block height.
 *
 * Example usage:
 * <pre>{@code
 * Path dbPath = Path.of("/path/to/electrs/db");
 * try (RocksDbElectrsDatabase db = new RocksDbElectrsDatabase(dbPath, NetworkParameters.fromID(NetworkParameters.ID_MAINNET))) {
 *     Optional<Integer> height = db.getTransactionHeight(txid);
 *     Optional<Block> header = db.getBlockHeader(700000);
 * }
 * }</pre>
 */
public class RocksDbElectrsDatabase implements ElectrsDatabase {
    private static final Logger log = LoggerFactory.getLogger(RocksDbElectrsDatabase.class);

    private final RocksDB db;
    private final List<ColumnFamilyHandle> columnFamilyHandles;
    private final ColumnFamilyHandle fundingCF;
    private final ColumnFamilyHandle spendingCF;
    private final ColumnFamilyHandle txidCF;
    private final ColumnFamilyHandle headersCF;
    private final ColumnFamilyHandle configCF;
    private final NetworkParameters networkParameters;

    /**
     * Opens an existing electrs RocksDB database in read-only mode
     *
     * @param dbPath Path to the electrs database directory
     * @param networkParameters Bitcoin network parameters for deserializing blocks
     * @throws RocksDBException if the database cannot be opened
     */
    public RocksDbElectrsDatabase(Path dbPath, NetworkParameters networkParameters) throws RocksDBException {
        this.networkParameters = networkParameters;
        this.columnFamilyHandles = new ArrayList<>();

        // Load RocksDB library
        RocksDB.loadLibrary();

        // Create column family descriptors
        List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
        for (String cfName : ElectrsSchema.ColumnFamilies.ALL) {
            columnFamilyDescriptors.add(new ColumnFamilyDescriptor(cfName.getBytes()));
        }

        // Open database in read-only mode
        DBOptions options = new DBOptions()
                .setCreateIfMissing(false)
                .setCreateMissingColumnFamilies(false);

        try {
            this.db = RocksDB.openReadOnly(options, dbPath.toString(), columnFamilyDescriptors, columnFamilyHandles);

            // Map column family handles
            this.fundingCF = findColumnFamily(ElectrsSchema.ColumnFamilies.FUNDING);
            this.spendingCF = findColumnFamily(ElectrsSchema.ColumnFamilies.SPENDING);
            this.txidCF = findColumnFamily(ElectrsSchema.ColumnFamilies.TXID);
            this.headersCF = findColumnFamily(ElectrsSchema.ColumnFamilies.HEADERS);
            this.configCF = findColumnFamily(ElectrsSchema.ColumnFamilies.CONFIG);

            log.info("Opened electrs database at {} with {} column families", dbPath, columnFamilyHandles.size());
        } catch (RocksDBException e) {
            // Clean up on failure
            columnFamilyHandles.forEach(ColumnFamilyHandle::close);
            options.close();
            throw e;
        }
    }

    private ColumnFamilyHandle findColumnFamily(String name) {
        for (int i = 0; i < ElectrsSchema.ColumnFamilies.ALL.length; i++) {
            if (ElectrsSchema.ColumnFamilies.ALL[i].equals(name)) {
                return columnFamilyHandles.get(i);
            }
        }
        throw new IllegalStateException("Column family not found: " + name);
    }

    @Override
    public Optional<Integer> getChainTipHeight() {
        try {
            byte[] tipData = db.get(headersCF, ElectrsSchema.Keys.CHAIN_TIP);
            if (tipData == null) {
                return Optional.empty();
            }
            // Chain tip value is the block hash, we need to look up its height
            // For now, we'll iterate headers to find the highest height
            // TODO: Optimize this with a separate tracking mechanism
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

            Block block = networkParameters.getDefaultSerializer().makeBlock(ByteBuffer.wrap(headerBytes));
            return Optional.of(block);
        } catch (RocksDBException | ProtocolException e) {
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
            // Seek to the first key with this prefix
            iterator.seek(scriptHashPrefix);

            // Iterate while keys match the prefix
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
            // Create a prefix key for this txid+vout combination
            byte[] prefixKey = ElectrsSchema.spendingKey(txidPrefix, (short) vout, 0);
            // Seek to keys matching txid+vout
            iterator.seek(Arrays.copyOfRange(prefixKey, 0, ElectrsSchema.PREFIX_LENGTH + 2));

            while (iterator.isValid()) {
                byte[] key = iterator.key();

                // Check if this key matches our txid+vout
                byte[] keyTxidPrefix = ElectrsSchema.extractTxidPrefix(key);
                short keyVout = ElectrsSchema.extractVout(key);

                if (!Arrays.equals(keyTxidPrefix, txidPrefix) || keyVout != vout) {
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

    @Override
    public void close() {
        log.info("Closing electrs database");
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
                // Skip the chain tip key 'T'
                if (key.length == 4) {
                    maxHeight = ElectrsSchema.decodeHeaderHeight(key);
                }
            }
        }
        return maxHeight;
    }
}
