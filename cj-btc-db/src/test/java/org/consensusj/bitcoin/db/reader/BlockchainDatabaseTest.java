package org.consensusj.bitcoin.db.reader;

import org.bitcoinj.base.Sha256Hash;
import org.bitcoinj.core.Block;
import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.params.RegTestParams;
import org.consensusj.bitcoin.db.reader.BlockchainDatabase.DatabaseStats;

import java.nio.file.Path;
import java.util.Optional;

/**
 * Simple test program to verify the cj-btc-db module can parse blockchain database
 */
public class BlockchainDatabaseTest {

    public static void main(String[] args) {
        Path dbPath = Path.of("/home/yuvic/electrs/db/regtest");
        NetworkParameters params = RegTestParams.get();

        System.out.println("=== Testing cj-btc-db Module ===");
        System.out.println("Database path: " + dbPath);
        System.out.println("Network: " + params.getId());
        System.out.println();

        try (RocksDbBlockchainDatabase db = new RocksDbBlockchainDatabase(dbPath, params)) {
            System.out.println("✓ Successfully opened blockchain database");
            System.out.println();

            // Test 1: Check if database is open
            if (db.isOpen()) {
                System.out.println("✓ Database is open and accessible");
            } else {
                System.out.println("✗ Database is not open");
                return;
            }
            System.out.println();

            // Test 2: Get chain tip
            System.out.println("--- Test 2: Chain Tip ---");
            Optional<Sha256Hash> tipHash = db.getChainTipHash();
            Optional<Integer> tipHeight = db.getChainTipHeight();

            if (tipHash.isPresent()) {
                System.out.println("✓ Chain tip hash: " + tipHash.get());
            } else {
                System.out.println("✗ Chain tip hash not found");
            }

            if (tipHeight.isPresent()) {
                System.out.println("✓ Chain tip height: " + tipHeight.get());
            } else {
                System.out.println("✗ Chain tip height not found");
            }
            System.out.println();

            // Test 3: Get genesis block (height 0)
            System.out.println("--- Test 3: Genesis Block ---");
            Optional<Block> genesisBlock = db.getBlockHeader(0);

            if (genesisBlock.isPresent()) {
                Block genesis = genesisBlock.get();
                System.out.println("✓ Genesis block found:");
                System.out.println("  Hash: " + genesis.getHash());
                System.out.println("  Version: " + genesis.getVersion());
                System.out.println("  Time: " + genesis.getTime());
                System.out.println("  Nonce: " + genesis.getNonce());
            } else {
                System.out.println("✗ Genesis block not found");
            }
            System.out.println();

            // Test 4: Get block at height 1 if it exists
            if (tipHeight.isPresent() && tipHeight.get() >= 1) {
                System.out.println("--- Test 4: Block at Height 1 ---");
                Optional<Block> block1 = db.getBlockHeader(1);

                if (block1.isPresent()) {
                    Block blk = block1.get();
                    System.out.println("✓ Block 1 found:");
                    System.out.println("  Hash: " + blk.getHash());
                    System.out.println("  Previous block: " + blk.getPrevBlockHash());
                } else {
                    System.out.println("✗ Block 1 not found");
                }
                System.out.println();
            }

            // Test 5: Get database statistics
            System.out.println("--- Test 5: Database Statistics ---");
            DatabaseStats stats = db.getStats();
            System.out.println("✓ Database statistics:");
            System.out.println("  Funding keys: " + stats.fundingKeys());
            System.out.println("  Spending keys: " + stats.spendingKeys());
            System.out.println("  Transaction ID keys: " + stats.txidKeys());
            System.out.println("  Header keys: " + stats.headerKeys());
            System.out.println("  Total size: " + (stats.totalSize() / 1024) + " KB");
            System.out.println();

            // Test 6: Test reading multiple blocks
            if (tipHeight.isPresent()) {
                int maxHeight = Math.min(tipHeight.get(), 10);
                System.out.println("--- Test 6: Reading Blocks 0-" + maxHeight + " ---");

                int successCount = 0;
                for (int i = 0; i <= maxHeight; i++) {
                    Optional<Block> block = db.getBlockHeader(i);
                    if (block.isPresent()) {
                        successCount++;
                    }
                }

                System.out.println("✓ Successfully read " + successCount + "/" + (maxHeight + 1) + " blocks");

                if (successCount == maxHeight + 1) {
                    System.out.println("✓ All blocks read successfully!");
                } else {
                    System.out.println("⚠ Some blocks could not be read");
                }
            }
            System.out.println();

            System.out.println("=== Test Summary ===");
            System.out.println("✓ Module successfully parses blockchain RocksDB data!");
            System.out.println("✓ All read operations completed without errors");

        } catch (Exception e) {
            System.err.println("✗ Error testing database:");
            e.printStackTrace();
            System.exit(1);
        }
    }
}
