# Testing cj-btc-db with Real Electrs Database

This guide explains how to test the `cj-btc-db` module with a real electrs database.

## Quick Start

### Option 1: Use an Existing Electrs Database

If you already have electrs running:

```bash
# Set the environment variable to point to your electrs database
export ELECTRS_DB_PATH=/path/to/your/electrs/db/mainnet

# Run the tests
./gradlew :cj-btc-db:test
```

### Option 2: Set Up Electrs for Testing (Recommended)

For thorough testing, set up electrs with a small blockchain (regtest or testnet):

#### Step 1: Install Electrs

```bash
# Clone electrs (romanz version, not Blockstream)
git clone https://github.com/romanz/electrs
cd electrs

# Build electrs
cargo build --release
```

#### Step 2: Configure for RegTest

Create a config file `electrs-regtest.toml`:

```toml
# Network
network = "regtest"

# Bitcoin Core RPC
daemon_rpc_addr = "127.0.0.1:18443"
daemon_dir = "/path/to/bitcoin/regtest/datadir"

# Electrs database
db_dir = "/path/to/electrs-regtest-db"

# Monitoring
monitoring_addr = "127.0.0.1:4224"
electrum_rpc_addr = "127.0.0.1:50001"

# Logging
log_filters = "INFO"
```

#### Step 3: Start Bitcoin Core in RegTest Mode

```bash
# Start bitcoind in regtest mode
bitcoind -regtest -daemon -txindex=0

# Generate some blocks for testing
bitcoin-cli -regtest generatetoaddress 200 bcrt1q... # your regtest address
```

#### Step 4: Run Electrs

```bash
./target/release/electrs --conf electrs-regtest.toml
```

Wait for electrs to sync (should be quick for regtest with 200 blocks).

#### Step 5: Run Tests

```bash
cd /path/to/consensusj

# Point to your electrs database
export ELECTRS_DB_PATH=/path/to/electrs-regtest-db

# Run the tests
./gradlew :cj-btc-db:test
```

## What the Tests Do

The test suite (`ElectrsDatabaseSpec.groovy`) verifies:

1. **Database Opening**: Can open electrs database in read-only mode
2. **Chain Tip Queries**: Read current blockchain tip (height and hash)
3. **Block Headers**: Read block headers by height (including genesis block)
4. **Transaction Lookups**: Find block height for transaction IDs
5. **Database Statistics**: Get database size and key counts

## Test Output Example

```
Chain tip: height=850123, hash=00000000000000000002a7c4c1e48d76c5a37902165a270156b7a8d72728a054

Block 100: hash=0000000000573993a3c9e41ce34708801fae1e676fb5a1e7c16ce4d8f3f1a08d,
           prev=000000000002d01c1fccc21636b607dfd930d31d01c3a62104612a1719011250

Transaction 4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b
           is at height 0

Database Statistics:
  Funding keys: 1234567
  Spending keys: 987654
  Transaction ID keys: 500000
  Header keys: 850123
  Total size: 45678 MB
```

## Manual Testing with Groovy Shell

You can also test interactively using Groovy:

```bash
# Start Groovy shell with classpath
./gradlew :cj-btc-db:groovysh

# Then in the shell:
groovy:000> import org.consensusj.bitcoin.db.reader.*
groovy:001> import org.bitcoinj.core.*
groovy:002> import java.nio.file.Path
groovy:003>
groovy:004> dbPath = Path.of('/path/to/electrs/db/mainnet')
groovy:005> db = new RocksDbElectrsDatabase(dbPath, NetworkParameters.fromID('main'))
groovy:006>
groovy:007> // Query chain tip
groovy:008> println db.chainTipHash
groovy:009> println db.chainTipHeight
groovy:010>
groovy:011> // Read genesis block
groovy:012> genesis = db.getBlockHeader(0)
groovy:013> println genesis.get().hash
groovy:014>
groovy:015> db.close()
```

## Verifying Schema Compatibility

To verify that the Java implementation reads the same data as electrs:

1. Use electrs CLI to query data:
```bash
# Get block header at height 100
echo '{"jsonrpc":"2.0","method":"blockchain.block.header","params":[100],"id":1}' | \
  nc localhost 50001
```

2. Query the same data from Java:
```java
db.getBlockHeader(100)
```

3. Compare the results - the block hashes should match exactly.

## Common Issues

### Database Not Found
```
Error: Database path does not exist or is not readable
```
**Solution**: Verify `ELECTRS_DB_PATH` points to the correct directory containing the RocksDB files.

### Column Family Not Found
```
Error: Column family not found: funding
```
**Solution**: Your database might be from Blockstream's electrs fork which uses a different schema. This library only supports romanz/electrs.

### Network Mismatch
```
Error: Cannot deserialize block - invalid magic bytes
```
**Solution**: Make sure you're using the correct `NetworkParameters` for your database:
- Mainnet: `NetworkParameters.fromID(NetworkParameters.ID_MAINNET)`
- Testnet: `NetworkParameters.fromID(NetworkParameters.ID_TESTNET)`
- Regtest: `NetworkParameters.fromID(NetworkParameters.ID_REGTEST)`

## Performance Testing

To test read performance:

```bash
# Run with timing
time ./gradlew :cj-btc-db:test --tests ElectrsDatabaseSpec

# Or create a performance test that reads many blocks
```

## Next Steps

Once basic tests pass:
1. Test with mainnet database for production readiness
2. Add write operations and test round-trip (write then read)
3. Create integration tests with Electrum protocol queries
4. Benchmark read performance vs electrs
