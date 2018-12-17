package bithunter.extractor.redis;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.bitcoinj.core.Block;
import org.bitcoinj.core.Transaction;
import org.bitcoinj.core.TransactionInput;
import org.bitcoinj.core.TransactionOutPoint;
import org.bitcoinj.core.TransactionOutput;
import org.bitcoinj.params.MainNetParams;
import org.bitcoinj.script.Script;

import com.google.common.cache.Cache;

@SuppressWarnings("rawtypes")
public class BlockJO implements JO {
    private static final BigDecimal TXO_UNIT = BigDecimal.valueOf(1e8d);

    private static MainNetParams params = MainNetParams.get();

    private static Function<String, Integer> heightProvider;
    public final List<TransactionJO> txs = new ArrayList<>();
    public final String blockId;
    public final Date timestamp;
    public final long height;
    public final long nbits;
    @Deprecated
    public final double difficulty;
    public final Set<Class<? extends QueryBuilder>> needToRun = new HashSet<>();
    private final Block rawBlock;
    private final CompletableFuture<Void> f = new CompletableFuture<>();
    private SimpleDateFormat sdf = new SimpleDateFormat();

    BlockJO(Block block) {
        this.rawBlock = block;

        this.blockId = block.getHashAsString();
        this.timestamp = block.getTime();
        this.height = heightProvider.apply(blockId);
        this.nbits = block.getDifficultyTarget();
        this.difficulty = 0;
//        this.difficulty = rawBlock.getDifficultyTarget();

        // coinbase sure
        txs.add(new TransactionJO(block.getTransactions().get(0), true));
        txs.addAll(block.getTransactions().stream().skip(1).map(tx -> new TransactionJO(tx, false))
                .collect(Collectors.toList()));

    }

    public static void setHeightProvider(Function<String, Integer> heightProvider) {
        BlockJO.heightProvider = heightProvider;
    }

    private static String bytesToHex(byte[] in) {
        final StringBuilder builder = new StringBuilder();
        for (byte b : in) {
            builder.append(String.format("%02x", b));
        }
        return builder.toString();
    }

    @Override
    public Map<String, String> describe() {
        return new TreeMap<String, String>() {
            {
                // put("rawBlock", rawBlock.toString());
                put("blockId", blockId);
                put("timestamp", sdf.format(timestamp));
                put("height", Long.toString(height));
                put("nbits", Long.toString(nbits));

                // put("difficulty", Double.toString(difficulty));
            }
        };
    }

    @Override
    public String getKey() {
        return "BLK_" + blockId;
    }

    @Override
    public List<Index> getIndex() {
        return Collections.singletonList(new Index("BLK_H_" + height, blockId));
    }

    @Override
    public void saved(Class<? extends QueryBuilder> clazz, Exception e) {
    }

    void onFinish(BiFunction func) {
        f.handleAsync(func);
    }

    public synchronized void complete(Class<? extends QueryBuilder> clazz, Exception e) {
        if (e != null) {
            f.completeExceptionally(e);
            return;
        }

        needToRun.remove(clazz);
        // if(needToRun.contains(QueryBuilder.BlockQueryBuilder.class) &&
        // needToRun.size() == 1)
        if (needToRun.contains(QueryBuilder.BlockQueryBuilder.class) && needToRun.size() == 1) {
            f.complete(null);
        }
    }

    enum Direction {
        Input, Output
    }

    public static class TxoKey {
        private String outputTxId;
        private int outputIdx;


        public TxoKey(String outputTxId, int outputIdx) {
            this.outputTxId = outputTxId;
            this.outputIdx = outputIdx;
        }

        public String getOutputTxId() {
            return outputTxId;
        }

        public int getOutputIdx() {
            return outputIdx;
        }

        @Override
        public String toString() {
            return "TXO_" + outputTxId + "_" + outputIdx;
        }
    }

    public class TransactionJO implements JO {
        public final boolean isCoinbase;
        public final String txid;
        public final Date timestamp;
        public final String blockId;
        final Transaction rawTx;
        private final List<TxoJO> txos;

        private TransactionJO(Transaction rawTx, boolean isCoinbase) {
            this.rawTx = rawTx;
            this.isCoinbase = isCoinbase;

            this.txid = rawTx.getHashAsString();
            this.blockId = rawBlock.getHashAsString();
            this.timestamp = rawTx.getUpdateTime();
            List<TxoJO> txos = new ArrayList<>();
            int i = 0;
			if (!isCoinbase)
				for (TransactionInput in : rawTx.getInputs()) {// ?
					try {
						txos.add(new TxoJO(in, i++));
					} catch (Throwable ex) {
						// System.out.println("Ignored(Maybe coinbase) -> " +
						// in);
						ex.printStackTrace();
					}
				}
            for (TransactionOutput out : rawTx.getOutputs()) {
                try {
                    txos.add(new TxoJO(out));
                } catch (Throwable ex) {

                }
            }
            this.txos = txos;
        }

        @Override
        public Map<String, String> describe() {
            return new TreeMap<String, String>() {
                {
                    put("txId", txid);
                    put("timestamp", sdf.format(timestamp));
                    put("blockId", blockId);
                    put("isCoinbase", Boolean.toString(isCoinbase));
                }
            };
        }

        @Override
        public String getKey() {
            return "TX_" + txid;
        }

        @Override
        public List<Index> getIndex() {
            return Collections.singletonList(new Index.SetIndex("BLK_TX_" + blockId, txid));
        }

        @Override
        public void saved(Class<? extends QueryBuilder> clazz, Exception e) {
            BlockJO.this.complete(clazz, e);
        }

        public List<TxoJO> getTxos() {
            return txos;
        }
    }

    public class TxoJO implements JO {
        public final Direction type;

        public final String outputTxId;
        public final int outputIdx;
        public final String scriptType;
        public final String script;
        public final BigDecimal value;
        public List<String> addresses;
        public String inputTxId;
        public int inputIdx = -1;
        private List<AddressJO> addressJOList;

        TxoJO(TransactionInput in, int i) {
            this.type = Direction.Input;
            this.inputTxId = in.getParentTransaction().getHashAsString();
            this.inputIdx = i;
            TransactionOutPoint outpoint = in.getOutpoint();
            this.outputTxId = outpoint.getHash().toString();
            this.outputIdx = (int) outpoint.getIndex();

            TxoKey key = new TxoKey(this.outputTxId, this.outputIdx);
            Map<String, String> map = null;
			try {
				map = Main.txoCache.get(key);
			} catch (ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
//            if(map)
            map.put("shouldRemove", "true");
            this.value = new BigDecimal(map.get("value"));
            this.script = map.get("script");
            this.scriptType = map.get("scriptType");

            Map<String, String> prev = Main.txoCache.getIfPresent(key);
            if (prev != null) {
                this.addresses = Arrays.asList(prev.getOrDefault("addresses", "").split(","));
            }

            loadAddresses();
        }

        TxoJO(TransactionOutput out) {
            this.type = Direction.Output;
            this.outputTxId = out.getParentTransaction().getHashAsString();
            this.outputIdx = out.getIndex();
            this.value = BigDecimal.valueOf(out.getValue().value);
            Script script = out.getScriptPubKey();

            this.script = bytesToHex(out.getScriptBytes());
            this.scriptType = script.getScriptType().toString();

            // load Addresses from out
            switch (this.scriptType) {
                case "P2PKH":
                    this.addresses = Collections.singletonList(out.getAddressFromP2PKHScript(params).toString());
                    break;
                case "PUB_KEY":
                    // FIXME the pubkey didn't convert to address
                    this.addresses = Collections.singletonList(bytesToHex(out.getScriptPubKey().getPubKey()));
                    break;
                case "P2SH":
                    this.addresses = Collections.singletonList(out.getAddressFromP2SH(params).toString());
            }


            loadAddresses();
        }

        public List<String> getAddresses() {
            return addresses;
        }

        private void loadAddresses() {

            if (this.addresses != null) {
                addressJOList = this.addresses.stream().map(s -> new AddressJO(this, s)).collect(Collectors.toList());
            }
        }

        public List<AddressJO> getAddressJOList() {
            if (addressJOList == null)
                return Collections.emptyList();
            return addressJOList;
        }

        @Override
        public Map<String, String> describe() {
            return new HashMap<String, String>() {
                {
                    put("inputTxId", inputTxId);
                    put("inputIdx", Integer.toString(inputIdx));
                    put("outputTxId", outputTxId);
                    put("outputIdx", Integer.toString(outputIdx));
                    put("value", value.toString());
                    put("script", script);
                    put("scriptType", scriptType);
                    if (addresses != null)
                        put("addresses", addresses.stream().collect(Collectors.joining(",")));
                }
            };
        }

        @Override
        public TxoKey getKey() {
            return new TxoKey(outputTxId, outputIdx);
        }

        @Override
        public List<Index> getIndex() {
            if (type == Direction.Input)
                return Collections.singletonList(new Index(String.format("INPUTTX_TXO_%s_%d", inputTxId, inputIdx),
                        String.format("%s_%d", outputTxId, outputIdx)));
            return JO.super.getIndex();
        }

        @Override
        public void saved(Class<? extends QueryBuilder> clazz, Exception e) {
            BlockJO.this.complete(clazz, e);
        }
    }

    public class AddressJO implements JO {
        public final TxoJO txoJo;
        final String address;
        final long height;
        final String recentTxId;

        AddressJO(TxoJO txoJo, String address) {
            this.txoJo = txoJo;
            this.address = address;

            if (txoJo.type == Direction.Input) {
                this.recentTxId = txoJo.inputTxId;
            } else {
                this.recentTxId = txoJo.outputTxId;
            }

            this.height = BlockJO.this.height;
        }

        @Override
        public Map<String, String> describe() {
            return new TreeMap<String, String>() {
                {
                    put("address", address);
                    put("height", Long.toString(height));
                    put("recentTxId", recentTxId);
                    put("direction", txoJo.type.toString());
                }
            };
        }

        public Direction getDirection() {
            return txoJo.type;
        }

        public String getAddress() {
            return address;
        }

        public long getHeight() {
            return height;
        }

        public String getRecentTxId() {
            return recentTxId;
        }

        @Override
        public String getKey() {
            return "ADDR_" + address;
        }

        @Override
        public List<Index> getIndex() {

            return Collections.singletonList(
                    new Index.SetIndex("ADDR_TXO_" + (txoJo.type == Direction.Input ? "IN" : "OUT") + address,
                            txoJo.outputTxId + "_" + txoJo.outputIdx));
        }



        @Override
        public void saved(Class<? extends QueryBuilder> clazz, Exception e) {
            BlockJO.this.complete(clazz, e);
        }
    }
}
