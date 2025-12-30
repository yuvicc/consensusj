package org.consensusj.bitcoin.db.writer;

import org.bitcoinj.base.Sha256Hash;
import org.bitcoinj.core.Block;
import org.consensusj.bitcoin.db.reader.BlockchainDatabase;

/**
 * Writable interface to an Bitcoin blockchain RocksDB database.
 *
 * This interface extends {@link BlockchainDatabase} with write operations for maintaining
 * a blockchain database. All write operations use RocksDB WriteBatch
 * for atomicity across multiple column families.
 *
 * Example usage:
 * <pre>{@code
 * Path dbPath = Path.of("/path/to/blockchain/db");
 * try (WritableBlockchainDatabase db = new RocksDbWritableBlockchainDatabase(dbPath, networkParams)) {
 *     // Write a block header
 *     db.putBlockHeader(height, block);
 *
 *     // Update chain tip
 *     db.updateChainTip(tipHeight, tipHash);
 *
 *     // Index a transaction
 *     db.putTransactionHeight(txid, height);
 * }
 * }</pre>
 */
public interface WritableBlockchainDatabase extends BlockchainDatabase {

    /**
     * Stores a block header at the given height
     * @param height Block height
     * @param block Block header to store
     * @throws Exception if the write fails
     */
    void putBlockHeader(int height, Block block) throws Exception;

    /**
     * Updates the chain tip to the given height and hash
     * @param height New chain tip height
     * @param hash New chain tip hash
     * @throws Exception if the write fails
     */
    void updateChainTip(int height, Sha256Hash hash) throws Exception;

    /**
     * Records the confirmed block height for a transaction
     * @param txid Transaction ID
     * @param height Block height where transaction was confirmed
     * @throws Exception if the write fails
     */
    void putTransactionHeight(Sha256Hash txid, int height) throws Exception;

    /**
     * Records a funding transaction for a script hash
     * @param scriptHashPrefix First 8 bytes of SHA256(script)
     * @param height Block height where funding occurred
     * @param txidPrefix First 8 bytes of transaction ID
     * @param vout Output index
     * @throws Exception if the write fails
     */
    void putFunding(byte[] scriptHashPrefix, int height, byte[] txidPrefix, short vout) throws Exception;

    /**
     * Records a spending transaction for an output
     * @param txidPrefix First 8 bytes of the transaction ID being spent
     * @param vout Output index being spent
     * @param height Block height where spending occurred
     * @param spendingTxidPrefix First 8 bytes of the spending transaction ID
     * @param vin Input index in the spending transaction
     * @throws Exception if the write fails
     */
    void putSpending(byte[] txidPrefix, short vout, int height, byte[] spendingTxidPrefix, int vin) throws Exception;

    /**
     * Stores a configuration key-value pair
     * @param key Configuration key
     * @param value Configuration value
     * @throws Exception if the write fails
     */
    void putConfig(String key, byte[] value) throws Exception;

    /**
     * Deletes a block header at the given height (for reorg handling)
     * @param height Block height to delete
     * @throws Exception if the delete fails
     */
    void deleteBlockHeader(int height) throws Exception;

    /**
     * Deletes a transaction height entry (for reorg handling)
     * @param txid Transaction ID to delete
     * @throws Exception if the delete fails
     */
    void deleteTransactionHeight(Sha256Hash txid) throws Exception;

    /**
     * Deletes a funding entry (for reorg handling)
     * @param scriptHashPrefix First 8 bytes of SHA256(script)
     * @param height Block height
     * @param txidPrefix First 8 bytes of transaction ID
     * @param vout Output index
     * @throws Exception if the delete fails
     */
    void deleteFunding(byte[] scriptHashPrefix, int height, byte[] txidPrefix, short vout) throws Exception;

    /**
     * Deletes a spending entry (for reorg handling)
     * @param txidPrefix First 8 bytes of the transaction ID being spent
     * @param vout Output index being spent
     * @param height Block height
     * @param spendingTxidPrefix First 8 bytes of the spending transaction ID
     * @param vin Input index
     * @throws Exception if the delete fails
     */
    void deleteSpending(byte[] txidPrefix, short vout, int height, byte[] spendingTxidPrefix, int vin) throws Exception;

    /**
     * Commits all pending writes atomically
     * This is useful when batching multiple operations together.
     * @throws Exception if the commit fails
     */
    void flush() throws Exception;
}
