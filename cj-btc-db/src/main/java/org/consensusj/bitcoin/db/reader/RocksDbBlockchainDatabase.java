package org.consensusj.bitcoin.db.reader;

import org.bitcoinj.base.Sha256Hash;
import org.bitcoinj.core.Block;
import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.core.ProtocolException;
import org.consensusj.bitcoin.db.schema.BlockchainSchema;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * RocksDB-based implementation of {@link BlockchainDatabase} for reading blockchain databases.
 *
 * This class provides read-only access to Bitcoin blockchain data stored in the RocksDB format.
 * It opens an existing blockchain RocksDB database and allows querying by script hash, transaction ID,
 * and block height.
 *
 * Example usage:
 * <pre>{@code
 * Path dbPath = Path.of("/path/to/blockchain/db");
 * try (RocksDbBlockchainDatabase db = new RocksDbBlockchainDatabase(dbPath, NetworkParameters.fromID(NetworkParameters.ID_MAINNET))) {
 *     Optional<Integer> height = db.getTransactionHeight(txid);
 *     Optional<Block> header = db.getBlockHeader(700000);
 * }
 * }</pre>
 */
public class RocksDbBlockchainDatabase implements BlockchainDatabase {
    private static final Logger log = LoggerFactory.getLogger(RocksDbBlockchainDatabase.class);

    private final RocksDB db;
    private final List<ColumnFamilyHandle> columnFamilyHandles;
    private final ColumnFamilyHandle fundingCF;
    private final ColumnFamilyHandle spendingCF;
    private final ColumnFamilyHandle txidCF;
    private final ColumnFamilyHandle headersCF;
    private final ColumnFamilyHandle configCF;
    private final NetworkParameters networkParameters;
    private final Map<Integer, Block> headersByHeight;
    private final Map<Sha256Hash, Block> headersByHash;

    /**
     * Opens an existing blockchain RocksDB database in read-only mode
     *
     * @param dbPath Path to the blockchain database directory
     * @param networkParameters Bitcoin network parameters for deserializing blocks
     * @throws RocksDBException if the database cannot be opened
     */
    public RocksDbBlockchainDatabase(Path dbPath, NetworkParameters networkParameters) throws RocksDBException {
        if (networkParameters == null) {
            throw new IllegalArgumentException("networkParameters cannot be null");
        }
        this.networkParameters = networkParameters;
        this.columnFamilyHandles = new ArrayList<>();
        this.headersByHeight = new HashMap<>();
        this.headersByHash = new HashMap<>();

        log.info("Constructor: networkParameters = {}", this.networkParameters);

        // Load RocksDB library
        RocksDB.loadLibrary();

        // Create column family descriptors
        List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
        for (String cfName : BlockchainSchema.ColumnFamilies.ALL) {
            columnFamilyDescriptors.add(new ColumnFamilyDescriptor(cfName.getBytes()));
        }

        // Open database in read-only mode
        DBOptions options = new DBOptions()
                .setCreateIfMissing(false)
                .setCreateMissingColumnFamilies(false);

        try {
            this.db = RocksDB.openReadOnly(options, dbPath.toString(), columnFamilyDescriptors, columnFamilyHandles);

            // Map column family handles
            this.fundingCF = findColumnFamily(BlockchainSchema.ColumnFamilies.FUNDING);
            this.spendingCF = findColumnFamily(BlockchainSchema.ColumnFamilies.SPENDING);
            this.txidCF = findColumnFamily(BlockchainSchema.ColumnFamilies.TXID);
            this.headersCF = findColumnFamily(BlockchainSchema.ColumnFamilies.HEADERS);
            this.configCF = findColumnFamily(BlockchainSchema.ColumnFamilies.CONFIG);

            log.info("Opened blockchain database at {} with {} column families", dbPath, columnFamilyHandles.size());

            // Load all headers into memory and build the chain
            loadHeaders();
            log.info("Loaded {} block headers into memory", headersByHeight.size());
        } catch (RocksDBException e) {
            // Clean up on failure
            columnFamilyHandles.forEach(ColumnFamilyHandle::close);
            options.close();
            throw e;
        }
    }

    private ColumnFamilyHandle findColumnFamily(String name) {
        for (int i = 0; i < BlockchainSchema.ColumnFamilies.ALL.length; i++) {
            if (BlockchainSchema.ColumnFamilies.ALL[i].equals(name)) {
                return columnFamilyHandles.get(i);
            }
        }
        throw new IllegalStateException("Column family not found: " + name);
    }

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

    /**
     * Loads all block headers from the database into memory and builds the chain.
     * Headers are stored with the 80-byte header as the key.
     * This method reads all headers, then follows the chain from the tip backwards
     * to assign heights to each header.
     */
    private void loadHeaders() throws RocksDBException {
        log.info("loadHeaders: networkParameters = {}", this.networkParameters);
        if (this.networkParameters == null) {
            throw new IllegalStateException("networkParameters is null in loadHeaders!");
        }

        // First, load all headers into a map by hash
        Map<Sha256Hash, Block> headerMap = new HashMap<>();

        int keyCount = 0;
        int validHeaderCount = 0;
        try (RocksIterator iterator = db.newIterator(headersCF)) {
            iterator.seekToFirst();

            while (iterator.isValid()) {
                byte[] key = iterator.key();
                keyCount++;

                // Skip the chain tip key
                if (key.length == 1 && key[0] == 'T') {
                    log.info("Skipping chain tip key");
                    iterator.next();
                    continue;
                }

                // Only process keys that are 80 bytes (block headers)
                if (key.length == BlockchainSchema.HEADER_SIZE) {
                    try {
                        Block block = this.networkParameters.getDefaultSerializer().makeBlock(ByteBuffer.wrap(key));
                        Sha256Hash hash = block.getHash();
                        headerMap.put(hash, block);
                        validHeaderCount++;
                    } catch (ProtocolException e) {
                        log.warn("Failed to deserialize header, skipping: {}", e.getMessage());
                    }
                } else {
                    log.warn("Skipping key with unexpected length: {} bytes", key.length);
                }

                iterator.next();
            }
        }

        log.info("Scanned {} keys, found {} valid headers", keyCount, validHeaderCount);
        log.info("Read {} headers from database", headerMap.size());

        // Get the chain tip
        Optional<Sha256Hash> tipHashOpt = getChainTipHash();
        if (!tipHashOpt.isPresent()) {
            log.warn("No chain tip found, cannot build height index");
            return;
        }

        Sha256Hash tipHash = tipHashOpt.get();
        log.info("Chain tip: {}", tipHash);
        log.info("Header map contains tip? {}", headerMap.containsKey(tipHash));

        // Debug: print first few hashes in the map
        log.info("First 5 hashes in header map:");
        int count = 0;
        for (Sha256Hash hash : headerMap.keySet()) {
            log.info("  Hash {}: {}", count, hash);
            count++;
            if (count >= 5) break;
        }

        // Follow the chain backwards from the tip to assign heights
        Sha256Hash currentHash = tipHash;
        int height = 0;

        // First, count the chain length by following backwards to find the height
        List<Sha256Hash> chain = new ArrayList<>();
        int chainBuildCount = 0;
        while (headerMap.containsKey(currentHash)) {
            Block header = headerMap.get(currentHash);
            chain.add(currentHash);
            currentHash = header.getPrevBlockHash();
            chainBuildCount++;
            if (chainBuildCount % 100 == 0) {
                log.info("Built {} chain links so far...", chainBuildCount);
            }
        }

        log.info("Built chain with {} links, last hash not found: {}", chain.size(), currentHash);

        // Now assign heights (chain is in reverse order, so reverse it)
        height = chain.size() - 1;
        for (int i = chain.size() - 1; i >= 0; i--) {
            Sha256Hash hash = chain.get(i);
            Block header = headerMap.get(hash);
            int assignedHeight = height - i;
            headersByHeight.put(assignedHeight, header);
            headersByHash.put(hash, header);
        }

        log.info("Built chain with {} headers, tip at height {}", headersByHeight.size(), height);
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
            // Seek to the first key with this prefix
            iterator.seek(scriptHashPrefix);

            // Iterate while keys match the prefix
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
            // Create a prefix key for this txid+vout combination
            byte[] prefixKey = BlockchainSchema.spendingKey(txidPrefix, (short) vout, 0);
            // Seek to keys matching txid+vout
            iterator.seek(Arrays.copyOfRange(prefixKey, 0, BlockchainSchema.PREFIX_LENGTH + 2));

            while (iterator.isValid()) {
                byte[] key = iterator.key();

                // Check if this key matches our txid+vout
                byte[] keyTxidPrefix = BlockchainSchema.extractTxidPrefix(key);
                short keyVout = BlockchainSchema.extractVout(key);

                if (!Arrays.equals(keyTxidPrefix, txidPrefix) || keyVout != vout) {
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

    @Override
    public void close() {
        log.info("Closing blockchain database");
        columnFamilyHandles.forEach(ColumnFamilyHandle::close);
        if (db != null) {
            db.close();
        }
    }

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
