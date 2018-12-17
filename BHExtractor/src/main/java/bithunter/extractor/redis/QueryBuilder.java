package bithunter.extractor.redis;

import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;

import bithunter.extractor.redis.BlockJO.TxoJO;

public interface QueryBuilder<T> {
	Runnable getRunnable(BlockingQueue<T> queue);

	static QueryBuilder getQueryBuilder(Class<? extends QueryBuilder> clazz) {
		try {
			return clazz.newInstance();
		} catch (InstantiationException | IllegalAccessException e) {
			e.printStackTrace();
		}
		return null;
	}

	abstract class JdbcQueryBuilder<T> implements QueryBuilder<T> {
		abstract String getSql();

		abstract Object[] getParameters(Object obj);

		private void addObject(JO obj) throws SQLException {
			DBUtil.execute(getSql(), getParameters(obj));
		}

		private void addObjects(Collection<JO> objects) throws SQLException {
			DBUtil.batch(getSql(), objects.parallelStream().map(this::getParameters).toArray(Object[][]::new));
		}

		public Runnable getRunnable(BlockingQueue<T> queue) {
			return () -> {
				Thread.currentThread().setName(this.getClass().getName());
				T obj;
				JO jo = null;
				try {
					while (true) {
						obj = queue.take();

						if (obj instanceof JO) {
							jo = (JO) obj;
							this.addObject(jo);
						} else if (obj instanceof Collection) {
							Collection<JO> objs = (Collection<JO>) obj;
							this.addObjects(objs);
							jo = objs.stream().findFirst().get();
						}
						Objects.requireNonNull(jo).saved(this.getClass(), null);
					}
				} catch (InterruptedException | SQLException e) {
					e.printStackTrace();
					if (jo != null) {
						jo.saved(this.getClass(), e);
					}
				}
			};
		}
	}

	class BlockQueryBuilder extends JdbcQueryBuilder<BlockJO> {

		@Override
		String getSql() {
			return "REPLACE INTO block(block_id, timestamp, height, nbits)" + " VALUES(?,?,?,?)";
		}

		@Override
		Object[] getParameters(Object r) {
			BlockJO obj = (BlockJO) r;
			return new Object[] { obj.blockId, obj.timestamp, obj.height, obj.nbits };
		}
	}

	class TransactionQueryBuilder extends JdbcQueryBuilder<BlockJO.TransactionJO> {

		@Override
		String getSql() {
			return "REPLACE INTO transaction(txid, block_id, timestamp, is_coinbase)" + " VALUES(?, ?, ?, ?)";
		}

		@Override
		Object[] getParameters(Object r) {
			BlockJO.TransactionJO obj = (BlockJO.TransactionJO) r;
			return new Object[] { obj.txid, obj.blockId, obj.timestamp, obj.isCoinbase };
		}
	}

	/**
	 * Save Spent TXO to RDB
	 */
	class TxoQueryBuilder extends JdbcQueryBuilder<BlockJO.TxoJO> {

		@Override
		String getSql() {
			return "REPLACE INTO txo(output_txid, output_idx, value, script, script_type, addresses, input_txid, input_idx)"
					+ " VALUES (?,?,?,?,?,?,?,?)";
		}

		@Override
		Object[] getParameters(Object r) {
			BlockJO.TxoJO obj = (BlockJO.TxoJO) r;
			return new Object[] { obj.outputTxId, obj.outputIdx, obj.value, obj.script, obj.scriptType,
					obj.addresses != null ? obj.addresses.stream().collect(Collectors.joining(",")) : null,

					obj.inputTxId, obj.inputIdx };
		}
	}

	class TxoOutQueryBuilder implements QueryBuilder<List<TxoJO>> {

		@Override
		public Runnable getRunnable(BlockingQueue<List<TxoJO>> queue) {
			// TODO Auto-generated method stub
			return () -> {
				Thread.currentThread().setName(this.getClass().getName());
				try {
					while (true) {
						List<TxoJO> obj = queue.take();
						
						obj.forEach(txo -> {
							Main.txoCache.put(txo.getKey(), txo.describe());
						});
						
						obj.stream().findFirst().ifPresent(o -> o.saved(TxoOutQueryBuilder.class, null));
					}
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			};
		}

	}

	class AddressQueryBuilder implements QueryBuilder<List<BlockJO.AddressJO>>{
		public Runnable getRunnable(BlockingQueue<List<BlockJO.AddressJO>> queue){
			return () -> {
				Thread.currentThread().setName(this.getClass().getName());
				try {
					while (true){
						List<BlockJO.AddressJO> obj = queue.take();
						obj.forEach(address -> {
							AddressRoll addressRoll = Main.addressCache.getIfPresent(address.getAddress());
							if(addressRoll == null) {
								addressRoll = new AddressRoll();
								addressRoll.setAddress(address.getAddress());
							}
							addressRoll.update(address);
							Main.addressCache.put(address.getAddress(), addressRoll);
						});
						obj.stream().findFirst().ifPresent(o -> o.saved(AddressQueryBuilder.class, null));
					}
				}catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			};
		}
	}
}
