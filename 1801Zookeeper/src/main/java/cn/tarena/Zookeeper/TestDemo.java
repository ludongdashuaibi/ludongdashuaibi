package cn.tarena.Zookeeper;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.junit.jupiter.api.Test;

import com.sun.org.apache.bcel.internal.generic.NEW;
/**
 * 通过代码连接zookeeper服务器
 * @author CGB
 *
 */
public class TestDemo {
	@Test
	public void testConnect() throws Exception {
		CountDownLatch cdl = new CountDownLatch(1);
		
		//1参：zk服务器的地址，形式-》IP：port
		//2参：客户端连接服务器的会话超时时间.如果在指定时间内未连接成功，这抛出：连接超时异常
		//3参：监听者.
		ZooKeeper zk = new ZooKeeper("176.19.1.58:2181",30000,new Watcher() {
			
			
			@Override
			public void process(WatchedEvent event) {
				//--KeeperState.SyncConnected 表示连接成功事件
				if(event.getState()==KeeperState.SyncConnected) {
					System.out.println("连接成功");
					//闭锁递减
					cdl.countDown();
				}
				
			}
			
		});
		//产生阻塞
		cdl.await();
	}
	
	@Test
	public void testCreate() throws Exception {
		CountDownLatch cdl = new CountDownLatch(1);
		
		//1参：zk服务器的地址，形式-》IP：port
		//2参：客户端连接服务器的会话超时时间.如果在指定时间内未连接成功，这抛出：连接超时异常
		//3参：监听者.
		ZooKeeper zk = new ZooKeeper("176.19.1.58:2181",30000,new Watcher() {
			
			
			@Override
			public void process(WatchedEvent event) {
				//--KeeperState.SyncConnected 表示连接成功事件
				if(event.getState()==KeeperState.SyncConnected) {
					System.out.println("连接成功");
					//闭锁递减
					cdl.countDown();
				}
				
			}
			
		});
		//产生阻塞
		cdl.await();
//		//--1参：路径2参：数据，要求是字节数组
//		//--3参：权限
//		//--4参：节点类型
//		zk.create("/park03","hello1801".getBytes(),Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		
		
		//--更新节点
		//--1参：更新的路径 2参：要更新的数据，要求是字节数组
		//--3参：数据版本号,要求和当前节点间dataVersion保持一致，否则报错
		//--每更新一次，dataVersion都递增1
		//--如果无论如何都更新，可以将version的数字等于-1
		//zk.setData("/park02", "helloxs".getBytes(),-1);
		
		//--3.删除节点
		//zk.delete("/park010000000005", -1);
		
		
		//--4.查看指定节点的数据
		//--1参：路径 2参：监听者 3参：节点状态信息，比如创建的事务id，时间戳等，一般不用
		//byte [] data = zk.getData("/park02",null,null);
		//System.out.println(new String(data));
		
		//--5.获取指定节点的子节点
		//--获取的子节点的名字，不是全路径，所以有时需要自己拼接全路径
		List<String> paths = zk.getChildren("/park02", null);
		for(String path :paths) {
			System.out.println(path);
		}
		while(true);
	}
	
	/**
	 * 监听指定节点 数据发生变化的事件
	 * @throws Exception
	 */
	@Test
	public void testWatchDataChanged() throws Exception {
		CountDownLatch cdl = new CountDownLatch(1);
		
		//1参：zk服务器的地址，形式-》IP：port
		//2参：客户端连接服务器的会话超时时间.如果在指定时间内未连接成功，这抛出：连接超时异常
		//3参：监听者.
		ZooKeeper zk = new ZooKeeper("176.19.1.58:2181",30000,new Watcher() {
			
			
			@Override
			public void process(WatchedEvent event) {
				//--KeeperState.SyncConnected 表示连接成功事件
				if(event.getState()==KeeperState.SyncConnected) {
					System.out.println("连接成功");
					//闭锁递减
					cdl.countDown();
				}
				
			}
			
		});
		
		cdl.await();
		//--1.监听节点数据发生变化的事件。通过zk.getData来监听
		//--zk的监听机制，默认是一次性监听，要实现永久监听，需要自己来改造
		for(;;) {
			CountDownLatch cdll = new CountDownLatch(1);
			zk.getData("/park02",new Watcher() {

				@Override
				public void process(WatchedEvent event) {
					if(event.getType()==EventType.NodeDataChanged) {
						System.out.println("数据发生变化。。。。");
						cdll.countDown();
					}
					
				}
				
			}, null);
			cdll.await();
		}
		

	}
	
	
	
	/*
	 * 监听自己的剑发生变化的事件
	 */
	@Test
	public void testWatchChildChanged() throws Exception {
		CountDownLatch cdl = new CountDownLatch(1);
		
		//1参：zk服务器的地址，形式-》IP：port
		//2参：客户端连接服务器的会话超时时间.如果在指定时间内未连接成功，这抛出：连接超时异常
		//3参：监听者.
		ZooKeeper zk = new ZooKeeper("176.19.1.58:2181",30000,new Watcher() {
			
			
			@Override
			public void process(WatchedEvent event) {
				//--KeeperState.SyncConnected 表示连接成功事件
				if(event.getState()==KeeperState.SyncConnected) {
					System.out.println("连接成功");
					//闭锁递减
					cdl.countDown();
				}
				
			}
			
		});
		
		cdl.await();
		
		zk.getChildren("/park02", new Watcher() {

			@Override
			public void process(WatchedEvent event) {
				if(event.getType()==EventType.NodeChildrenChanged) {
					System.out.println("子节点发生变化");
				}
				
			}
			
		});
		
		
		while(true);
		
	}
	
	
	/*
	 * 监听指定节点被删除的事件
	 */
	@Test
	public void testWatchNodeDelete() throws Exception {
		CountDownLatch cdl = new CountDownLatch(1);
		
		//1参：zk服务器的地址，形式-》IP：port
		//2参：客户端连接服务器的会话超时时间.如果在指定时间内未连接成功，这抛出：连接超时异常
		//3参：监听者.
		ZooKeeper zk = new ZooKeeper("176.19.1.58:2181",30000,new Watcher() {
			
			
			@Override
			public void process(WatchedEvent event) {
				//--KeeperState.SyncConnected 表示连接成功事件
				if(event.getState()==KeeperState.SyncConnected) {
					System.out.println("连接成功");
					//闭锁递减
					cdl.countDown();
				}
				
			}
			
		});
		
		cdl.await();
		
		//--此监听的作用：一般是监听某台服务器是否宕机
		//--因为每台服务器都会注册自己的临时及诶单，所以宕机后，临时界定被删除，则可以监听到删除事件
		zk.exists("/park02",new Watcher() {

			@Override
			public void process(WatchedEvent event) {
				if(event.getType()==EventType.NodeDeleted) {
					System.out.println("此节点已被删除");
				}
				
			}
			
		});
		
		while(true);
		
	}
}
