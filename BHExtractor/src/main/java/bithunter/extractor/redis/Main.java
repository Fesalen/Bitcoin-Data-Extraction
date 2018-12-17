package bithunter.extractor.redis;

import bithunter.extractor.redis.BlockJO.TxoKey;
import com.google.common.cache.*;
import org.apache.commons.dbutils.QueryRunner;
import org.bitcoinj.core.Block;
import org.bitcoinj.core.Context;
import org.bitcoinj.params.MainNetParams;
import org.bitcoinj.utils.BlockFileLoader;
import wf.bitcoin.javabitcoindrpcclient.BitcoinJSONRPCClient;
import wf.bitcoin.javabitcoindrpcclient.BitcoindRpcClient;
import wf.bitcoin.javabitcoindrpcclient.BitcoindRpcClient.RawTransaction.Out;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class Main {
    private static WakeUpConn wakeUpConn = new WakeUpConn();

    static final LoadingCache<String, BitcoindRpcClient.RawTransaction> txCache = CacheBuilder.newBuilder()
            .expireAfterAccess(3, TimeUnit.HOURS)
            .build(new CacheLoader<String, BitcoindRpcClient.RawTransaction>() {
                BitcoinJSONRPCClient bitcoin;

                {
                    try {
                        bitcoin = new BitcoinJSONRPCClient(
                                new URL("http://wangjizhang:wangjizhangmima@10.21.238.250:8332"));
                    } catch (MalformedURLException e) {
                        e.printStackTrace();
                    }
                }

                @Override
                public BitcoindRpcClient.RawTransaction load(String txId) throws Exception {
                    try {
                        System.out.println(new Date() + " Loading TX_" + txId);
                        return bitcoin.getRawTransaction(txId);
                    } finally {
                        System.out.println(new Date() + " TX_" + txId + " loaded");
                    }
                }
            });
    static final LoadingCache<TxoKey, Map<String, String>> txoCache = CacheBuilder.newBuilder()
            .expireAfterAccess(5, TimeUnit.MINUTES)
            .expireAfterWrite(1, TimeUnit.HOURS)
            .removalListener(new RemovalListener<TxoKey, Map<String, String>>() {
                PreparedStatement psTxo;

                {
                    try {
                        Connection connection = DBUtil.getDataSource().getConnection();
                        wakeUpConn.register(connection);
                        psTxo = connection.prepareStatement(
                                "INSERT IGNORE INTO txo("
                                        + "output_txid, output_idx, value, script, script_type, addresses"
                                        + ") values(?,?,?,?,?,?)");
                    } catch (Exception e) {

                    }
                }

                public void onRemoval(RemovalNotification<TxoKey, Map<String, String>> notification) {//需要从cashe转移到MySql的cache
                    try {
                        psTxo.setString(1, notification.getKey().getOutputTxId());
                        psTxo.setInt(2, notification.getKey().getOutputIdx());
                        psTxo.setBigDecimal(3, new BigDecimal(notification.getValue().get("value")));
                        psTxo.setString(4, notification.getValue().get("script"));
                        psTxo.setString(5, notification.getValue().get("scriptType"));
                        psTxo.setString(6, notification.getValue().get("addresses"));
                        psTxo.execute();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            })
            .build(new CacheLoader<TxoKey, Map<String, String>>() {
                PreparedStatement psTxo;

                {//初始化prepareStatement
                    try {
                        Connection connection = DBUtil.getDataSource().getConnection();
                        wakeUpConn.register(connection);
                        psTxo = connection.prepareStatement(
                                "SELECT value, script, script_type FROM txo WHERE output_txid=? AND output_idx=?");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

                @Override
                public Map<String, String> load(TxoKey key) throws Exception {

                    Map<String, String> ret = new TreeMap<>();
                    ResultSet rs;
                    synchronized (psTxo) {
                        psTxo.setString(1, key.getOutputTxId());
                        psTxo.setInt(2, key.getOutputIdx());
                        rs = psTxo.executeQuery();
                    }

                    if (rs.next()) {
                        ret.put("value", rs.getString("value"));
                        ret.put("script", rs.getString("script"));
                        ret.put("scriptType", rs.getString("script_type"));
                    } else {
                        System.out.println("Query Wallet" + key);
                        Out out = txCache.get(key.getOutputTxId()).vOut().get(key.getOutputIdx());
//                        BitcoinJSONRPCClient bitcoin = new BitcoinJSONRPCClient(
//                                new URL("http://wangjizhang:wangjizhangmima@10.21.238.250:8332"));
//                        Out out = bitcoin.getRawTransaction(key.getOutputTxId()).vOut().get(key.getOutputIdx());
                        ret.put("addresses", out.scriptPubKey().addresses().stream().collect(Collectors.joining(",")));
                        ret.put("value", String.valueOf(new BigDecimal(out.value()).multiply(BigDecimal.valueOf(1e8d))));
                        ret.put("script", out.scriptPubKey().hex());
                        ret.put("scriptType", out.scriptPubKey().type());
                        System.out.println("Got " + key);
                    }
                    rs.close();

                    return ret;

                }

            });

    protected static final LoadingCache<String, AddressRoll> addressCache = CacheBuilder.newBuilder()
            .expireAfterAccess(5, TimeUnit.MINUTES)
            .expireAfterWrite(1, TimeUnit.HOURS)
            .removalListener(new RemovalListener<String, AddressRoll>() {
                PreparedStatement psTxo;

                {
                    try {
                        Connection connection = DBUtil.getDataSource().getConnection();
                        wakeUpConn.register(connection);
                        psTxo = connection.prepareStatement(
                                "REPLACE INTO address("
                                        + "address, first_output_txid, first_input_txid, first_output_height, first_input_height, latest_txid"
                                        + ") values(?,?,?,?,?,?)");
                    } catch (Exception e) {

                    }
                }

                public void onRemoval(RemovalNotification<String, AddressRoll> notification) {
                    try {
                        psTxo.setString(1, notification.getKey());
                        psTxo.setString(2, notification.getValue().getFirstOutputTxid());
                        psTxo.setString(3, notification.getValue().getFirstInputTxid());
                        psTxo.setLong(4, notification.getValue().getFirstOutputHeight());
                        psTxo.setLong(5, notification.getValue().getFirstInputHeight());
                        psTxo.setString(6, notification.getValue().getLatestTxid());
                        try {

                            psTxo.execute();
                        } catch (Exception e) {
                            e.printStackTrace();
                            System.err.println(notification);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            })
            .build(new CacheLoader<String, AddressRoll>() {
                PreparedStatement psTxo;

                {
                    try {
                        Connection connection = DBUtil.getDataSource().getConnection();
                        wakeUpConn.register(connection);
                        psTxo = connection.prepareStatement(
                                "SELECT first_output_txid, first_input_txid, first_output_height, first_input_height, latest_txid FROM address WHERE address=?");
                    } catch (Exception e) {

                    }
                }

                @Override
                public AddressRoll load(String key) throws Exception {

                    AddressRoll ret = new AddressRoll();
                    ResultSet rs;
                    synchronized (psTxo) {
                        psTxo.setString(1, key);
                        rs = psTxo.executeQuery();
                    }

                    if (rs.next()) {
                        ret.setAddress(key);
                        ret.setFirstOutputTxid(rs.getString("first_output_txid"));
                        ret.setFirstInputTxid(rs.getString("first_input_txid"));
                        ret.setFirstOutputHeight(rs.getInt("first_output_height"));
                        ret.setFirstInputHeight(rs.getInt("first_input_height"));
                        ret.setLatestTxid(rs.getString("latest_txid"));
                    }
                    rs.close();
                    return ret;

                }

            });

//    protected static final Cache<TxoKey, List<String>> addressCache = CacheBuilder.newBuilder()
//            .build(new CacheLoader<TxoKey, List<String>>() {
//                @Override
//                public List<String> load(TxoKey key) throws Exception {
//                    return null;
//                }
//            });


    private static final List<Class<? extends QueryBuilder>> tasks = Arrays.asList(QueryBuilder.BlockQueryBuilder.class,
            QueryBuilder.TransactionQueryBuilder.class, QueryBuilder.TransactionQueryBuilder.class,
            QueryBuilder.TxoQueryBuilder.class, QueryBuilder.TxoQueryBuilder.class,
            QueryBuilder.TxoOutQueryBuilder.class, QueryBuilder.AddressQueryBuilder.class);
    private static String filePath;
    // private BlockingQueue<BlockJO> emptyBlockJOS = new
    // ArrayBlockingQueue<>(500);
    private BlockingQueue<BlockJO> blockJOS = new ArrayBlockingQueue<>(500);
    private BlockingQueue<List<BlockJO.TransactionJO>> transactionJOS = new ArrayBlockingQueue<>(500);
    private BlockingQueue<List<BlockJO.TxoJO>> utxoJOS = new ArrayBlockingQueue<>(500);
    private BlockingQueue<List<BlockJO.TxoJO>> txoJOS = new ArrayBlockingQueue<>(500);
    private BlockingQueue<List<BlockJO.AddressJO>> addressJOS = new ArrayBlockingQueue<>(500);
    private BlockingQueue<BlockJO> finishedBlockJOS = new ArrayBlockingQueue<>(500);
    private Map<BlockingQueue, Class<? extends QueryBuilder>> classMap = new HashMap<BlockingQueue, Class<? extends QueryBuilder>>() {
        {
            for (Class cls : tasks) {
                put(getQueueByClassName(cls.getSimpleName()), cls);
            }
        }

    };
    private List<Integer> existsBlockHeight = Collections.emptyList();

    public static void main(String[] args) throws MalformedURLException, URISyntaxException, SQLException {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        Main.filePath = args[0];

        ScheduledExecutorService wakeUpConnTh = Executors.newScheduledThreadPool(1);
        wakeUpConnTh.scheduleWithFixedDelay(wakeUpConn, 1, 2, TimeUnit.HOURS);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.err.println(addressCache.stats());
            txoCache.invalidateAll();
            addressCache.invalidateAll();
            wakeUpConnTh.shutdown();
            System.err.println("bye");
        }, "SHUTDOWN"));

        new Main().run();
    }

    private static Connection getConnection() throws SQLException {
        return DBUtil.getDataSource().getConnection();
    }

    private int getCheckpoint() {
        int startAt = 1;
        try (Connection conn = getConnection()) {
            Statement stat = conn.createStatement();
            ResultSet rs = stat
                    .executeQuery(" SELECT ifnull(max(height),0)+1" + " FROM (" + "  SELECT height, (@h:=@h+1) h"
                            + "  FROM block, (select @h:=0) c " + "  ORDER BY height" + " ) a" + " WHERE height = h");
            if (rs.next()) {
                startAt = rs.getInt(1);
            }
            rs.close();
            rs = stat.executeQuery("SELECT height FROM block WHERE height >=" + startAt);
            List<Integer> exists = new ArrayList<>();
            while (rs.next())
                exists.add(rs.getInt(1));
            stat.close();
            exists.sort(Integer::compareTo);
            this.existsBlockHeight = exists;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return startAt;
    }


    private BlockingQueue getQueueByClassName(String clazz) {

        switch (clazz) {
            case "BlockQueryBuilder":
                return finishedBlockJOS;
            case "TransactionQueryBuilder":
                return transactionJOS;
            case "TxoQueryBuilder":
                return txoJOS;
            case "TxoOutQueryBuilder":
                return utxoJOS;
//            case "UtxoQueryBuilder":
//                return utxoJOS;
            case "AddressQueryBuilder":
                return addressJOS;
            default:
                return null;
        }
    }

    private void run() throws MalformedURLException, URISyntaxException, SQLException {
        final String ID = new SimpleDateFormat("yyyyMMdd_hhmmss").format(new Date());
        RuntimeMXBean rtb = ManagementFactory.getRuntimeMXBean();
        System.out.println(rtb.getName() + " queue->" + ID);
//		BlockJO.setHeightProvider();
        Connection connection = DBUtil.getDataSource().getConnection();
        wakeUpConn.register(connection);
        PreparedStatement ps = connection.prepareStatement("SELECT height FROM blockid_h WHERE block_id=?");
        BlockJO.setHeightProvider((blkId) -> {
            synchronized (ps) {
                try {
                    ps.setString(1, blkId);
                    ResultSet rs = ps.executeQuery();
                    if (rs.next()) {
                        return rs.getInt(1);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            return -1;
        });

        ExecutorService th = Executors.newCachedThreadPool();

        try {
            PrintWriter writer = new PrintWriter(new FileOutputStream("qstatus" + ID));
            Executors.newScheduledThreadPool(1).scheduleAtFixedRate(() -> {
                Thread.currentThread().setName("qstatus");

                writer.println();
                writer.println("***************************************************************************");
                // writer.println("emptyBlockJOS len -> " +
                // emptyBlockJOS.size());
                writer.println("blockJOS len         -> " + blockJOS.size());
                writer.println("transactionJOS len   -> " + transactionJOS.size());
                writer.println("utxoJOS len          -> " + utxoJOS.size());
                writer.println("txoJOS len           -> " + txoJOS.size());
                writer.println("addressJOS len       -> " + addressJOS.size());
                writer.println("finishedBlockJOS len -> " + finishedBlockJOS.size());
                writer.println();
                writer.flush();
            }, 10, 10, TimeUnit.SECONDS);
        } catch (FileNotFoundException e3) {
            // TODO Auto-generated catch block
            e3.printStackTrace();
        }

        Set<String> existBlocks = new HashSet<>();
        try {
            new QueryRunner().query(connection, "SELECT block_id FROM block", (rs) -> {
                while (rs.next()) {
                    existBlocks.add(rs.getString(1));
                }
                return null;
            });
        } catch (SQLException e2) {
            // TODO Auto-generated catch block
            e2.printStackTrace();
        }

        // -> emptyBlockJOS.
        th.execute(() -> {
            Thread.currentThread().setName("-> emptyBlockJOS");
            long i = getCheckpoint();
            Iterator<Integer> it = existsBlockHeight.iterator();
            int nExist = 0;

            // List<File> blockChainFiles = Arrays.asList(
            // new File(filePath).listFiles((File dir, String name) ->
            // Pattern.matches("^blk000[0-5]\\d+\\.dat$", name)));

            MainNetParams params = MainNetParams.get();
            new Context(params);
            for (int blk_idx = 220; true; blk_idx++) {
//            for (int blk_idx = 720; true; blk_idx++) {
                String filename = filePath + String.format("\\blk%05d.dat", blk_idx);
                System.out.println("working on " + filename);
                File f = new File(filename);
                if (!f.exists()) {
                    break;
                }
                BlockFileLoader bfl = new BlockFileLoader(params, Collections.singletonList(f));

                for (Block rawBlock : bfl) {

                    BlockJO block = new BlockJO(rawBlock);
                    if (existBlocks.contains(block.blockId)) {
                        existBlocks.remove(block.blockId);
                        System.out.println("skipped block#" + block.blockId);
                        continue;
                    }

                    block.needToRun.addAll(tasks);
                    BlockJO finalB = block;
                    block.onFinish((_s, e) -> {
                        if (e == null) {
                            try {
                                finishedBlockJOS.put(finalB);
                            } catch (InterruptedException e1) {
                                e1.printStackTrace();
                            }
                        } else {
                            System.exit(1);
                        }
                        return null;
                    });
                    try {
                        blockJOS.put(block);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }

            // TODO hello world
        });

        // blockJOS -> transactionJOS, txoJOS, txoJOS2, utxoJOS, addressTxoJOS,
        // addressJOS
        th.execute(() -> {
            Thread.currentThread().setName("distribute blockJOS");
            while (true) {
                BlockJO block = null;
                try {
                    block = blockJOS.take();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    break;
                }

                try {
                    putObject(transactionJOS, block, block.txs);
                } catch (InterruptedException e) {
                    e.printStackTrace();

                }

                List<BlockJO.TxoJO> localTxoJOS = block.txs.stream().map(BlockJO.TransactionJO::getTxos)
                        .flatMap(List::stream).collect(Collectors.toList());
                try {
                    List<BlockJO.TxoJO> vin = localTxoJOS.stream().filter(i -> i.type == BlockJO.Direction.Input)
                            .collect(Collectors.toList());
                    List<BlockJO.TxoJO> vout = localTxoJOS.stream().filter(i -> i.type == BlockJO.Direction.Output)
                            .collect(Collectors.toList());
                    putObject(txoJOS, block, vin);
                    putObject(utxoJOS, block, vout);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                List<BlockJO.AddressJO> localAddressTxoJOS = localTxoJOS.stream().map(BlockJO.TxoJO::getAddressJOList)
                        .flatMap(List::stream).collect(Collectors.toList());
                try {
                    putObject(addressJOS, block, localAddressTxoJOS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        tasks.stream().map(clazz -> {
            if (QueryBuilder.JdbcQueryBuilder.class.isAssignableFrom(clazz))
                return QueryBuilder.getQueryBuilder(clazz);
            try {
                return clazz.newInstance();
            } catch (InstantiationException | IllegalAccessException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return null;
        }).filter(Objects::nonNull)
                .map((QueryBuilder q) -> q.getRunnable(getQueueByClassName(q.getClass().getSimpleName())))
                .filter(Objects::nonNull).forEach(th::execute);

    }

    private void putObject(BlockingQueue queue, BlockJO blk, Object obj) throws InterruptedException {
        if (obj instanceof Collection) {
            Collection objs = (Collection) obj;
            if (objs.isEmpty()) {
                blk.complete(classMap.get(queue), null);
                return;
            }
        }
        queue.put(obj);
    }
}
