package org.consensusj.bitcoin.db.reader;

import org.bitcoinj.base.Sha256Hash;
import org.bitcoinj.core.Block;

import java.io.Closeable;
import java.util.List;
import java.util.Optional;

/**
 * Read-only interface to a Bitcoin blockchain RocksDB database.
 *
 * This interface provides access to Bitcoin blockchain data stored in RocksDB.
 * It supports querying by script hash (for address history), transaction ID, and block height.
 *
 * @see org.consensusj.bitcoin.db.schema.BlockchainSchema
 */
public interface BlockchainDatabase extends Closeable {

    /**
     * Gets the current chain tip height
     * @return Block height of the chain tip, or empty if not available
     */
    Optional<Integer> getChainTipHeight();

    /**
     * Gets the current chain tip block hash
     * @return Block hash of the chain tip, or empty if not available
     */
    Optional<Sha256Hash> getChainTipHash();

    /**
     * Gets a block header by height
     * @param height Block height
     * @return Block header, or empty if not found
     */
    Optional<Block> getBlockHeader(int height);

    /**
     * Gets the confirmed block height for a transaction
     * Uses the txid prefix index to look up the height.
     *
     * @param txid Transaction ID
     * @return Block height where transaction was confirmed, or empty if not found
     */
    Optional<Integer> getTransactionHeight(Sha256Hash txid);

    /**
     * Gets all block heights where outputs were sent to a script hash
     * @param scriptHashPrefix First 8 bytes of SHA256(script)
     * @return List of block heights containing funding transactions
     */
    List<Integer> getFundingHeights(byte[] scriptHashPrefix);

    /**
     * Gets all block heights where outputs from a transaction were spent
     * @param txid Transaction ID
     * @param vout Output index
     * @return List of block heights containing spending transactions
     */
    List<Integer> getSpendingHeights(Sha256Hash txid, int vout);

    /**
     * Checks if the database exists and is readable
     * @return true if database can be opened
     */
    boolean isOpen();

    /**
     * Gets database statistics
     * @return Statistics about the database (size, number of keys, etc.)
     */
    DatabaseStats getStats();

    /**
     * Database statistics
     */
    record DatabaseStats(
        long fundingKeys,
        long spendingKeys,
        long txidKeys,
        long headerKeys,
        long totalSize
    ) {}
}
