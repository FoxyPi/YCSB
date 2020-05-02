package site.ycsb.db;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.ConnectionObserver;
import net.spy.memcached.FailureMode;
import net.spy.memcached.MemcachedConnection;
import net.spy.memcached.MemcachedNode;
import net.spy.memcached.OperationFactory;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.ops.NoopOperation;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationException;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.TapOperation;
import net.spy.memcached.ops.VBucketAware;
import net.spy.memcached.protocol.binary.TapAckOperationImpl;

public class UDPMemcachedConnection extends MemcachedConnection {

  private static final String OVERALL_REQUEST_METRIC = "[MEM] Request Rate: All";
  private static final String OVERALL_AVG_BYTES_READ_METRIC = "[MEM] Average Bytes read from OS per read";
  private static final String OVERALL_AVG_TIME_ON_WIRE_METRIC = "[MEM] Average Time on wire for operations (µs)";
  private static final String OVERALL_RESPONSE_METRIC = "[MEM] Response Rate: All (Failure + Success + Retry)";
  private static final String OVERALL_RESPONSE_FAIL_METRIC = "[MEM] Response Rate: Failure";
  private static final String OVERALL_RESPONSE_SUCC_METRIC = "[MEM] Response Rate: Success";
  private static final String OVERALL_AVG_BYTES_WRITE_METRIC = "[MEM] Average Bytes written to OS per write";
  private static final int DOUBLE_CHECK_EMPTY = 256;
  private static final int EXCESSIVE_EMPTY = 0x1000000;
  private static final String OVERALL_RESPONSE_RETRY_METRIC =
    "[MEM] Response Rate: Retry";

  private static ConnectionFactory connectionFactory;
  private static int bufSize;
  private static final Collection<ConnectionObserver> connObservers =
    new ConcurrentLinkedQueue<ConnectionObserver>();

  private int emptySelects = 0;

  private final List<Operation> retryOps;

  private final OperationFactory opFact;

  private final boolean verifyAliveOnConnect;

  private final ExecutorService listenerExecutorService;


  public UDPMemcachedConnection(final int bufSize, final ConnectionFactory f, final List<InetSocketAddress> a,
      final Collection<ConnectionObserver> obs, final FailureMode fm, final OperationFactory opfactory)
      throws IOException {

    super(bufSize, getConnectionFactory(f), a, initializeObservers(obs), fm, opfactory);
    retryOps = Collections.synchronizedList(new ArrayList<Operation>());
    this.opFact = opfactory;
    this.bufSize = bufSize;

    String verifyAlive = System.getProperty("net.spy.verifyAliveOnConnect");
    if(verifyAlive != null && verifyAlive.equals("true")) {
      verifyAliveOnConnect = true;
    } else {
      verifyAliveOnConnect = false;
    }

    listenerExecutorService = f.getListenerExecutorService();
  }

  private void handleReadsAndWrites(final SelectionKey sk,
          final MemcachedNode node) throws IOException {
    if (sk.isValid()) {
      if (sk.isReadable()) {
        handleReads(node);
      }
      if (sk.isWritable()) {
        handleWrites(node);
      }
    }
  }

  private void connected(final MemcachedNode node) {
    assert node.getChannel().isConnected() : "Not connected.";
    int rt = node.getReconnectCount();
    node.connected();

    System.out.println("ConnObservers: " + connObservers);

    for (ConnectionObserver observer : connObservers) {
      observer.connectionEstablished(node.getSocketAddress(), rt);
    }
  }

  @Override
  protected List<MemcachedNode> createConnections(final Collection<InetSocketAddress> addrs) throws IOException {
        List<MemcachedNode> connections = new ArrayList<MemcachedNode>(addrs.size());
        
    for (SocketAddress sa : addrs) {
      DatagramChannel ch = DatagramChannel.open();
      ch.configureBlocking(false);
      MemcachedNode qa = ((UDPDefaultConnFactory)connectionFactory).createMemcachedNode(sa, ch, bufSize);
      qa.setConnection(this);
      int ops = 0;
      
      try{
        ch.connect(sa);
        if(ch.isConnected()){
          connected(qa);
        } else{
          ops = SelectionKey.OP_CONNECT;
        }

        selector.wakeup();
        qa.setSk(ch.register(selector,ops,qa));
        
        assert ch.isConnected()
            || qa.getSk().interestOps() == SelectionKey.OP_CONNECT
            : "Not connected, and not wanting to connect";
      } catch (SocketException e){
        queueReconnect(qa);
      }

      connections.add(qa);
    };
  
    return connections;
  }

  private static final Collection<ConnectionObserver> 
    initializeObservers(Collection<ConnectionObserver> obs){
      connObservers.addAll(obs);
      return obs;
  }

  private static final ConnectionFactory getConnectionFactory(ConnectionFactory f){
      connectionFactory = f;
      return f;
  }

  @Override
  protected void addOperation(String key, Operation o) {
      MemcachedNode placeIn = null;
      MemcachedNode primary = locator.getPrimary(key);
      System.out.println("Is primary active? " + primary.isActive());

      if (primary.isActive() || failureMode == FailureMode.Retry) {
        placeIn = primary;
      } else if (failureMode == FailureMode.Cancel) {
        o.cancel();
      } else {
        Iterator<MemcachedNode> i = locator.getSequence(key);
        while (placeIn == null && i.hasNext()) {
          MemcachedNode n = i.next();
          if (n.isActive()) {
            placeIn = n;
          }
        }

        if (placeIn == null) {
          placeIn = primary;
          this.getLogger().warn("Could not redistribute to another node, "
            + "retrying primary node for %s.", key);
        }
      }

      assert o.isCancelled() || placeIn != null : "No node found for key " + key;
      if (placeIn != null) {
        addOperation(placeIn, o);
      } else {
        assert o.isCancelled() : "No node found for " + key + " (and not "
          + "immediately cancelled)";
      }
  }

  @Override
  protected void addOperation(MemcachedNode node, Operation o) {
    if (!node.isAuthenticated()) {
      retryOperation(o);
      return;
    }
    o.setHandlingNode(node);
    o.initialize();
    node.addOp(o);
    addedQueue.offer(node);
    metrics.markMeter(OVERALL_REQUEST_METRIC);

    Selector s = selector.wakeup();
    assert s == selector : "Wakeup returned the wrong selector.";
    getLogger().debug("Added %s to %s", o, node);
  }

  private Operation handleReadsWhenChannelEndOfStream(final Operation currentOp,
    final MemcachedNode node, final ByteBuffer rbuf) throws IOException {
    if (currentOp instanceof TapOperation) {
      currentOp.getCallback().complete();
      ((TapOperation) currentOp).streamClosed(OperationState.COMPLETE);

      getLogger().debug("Completed read op: %s and giving the next %d bytes",
        currentOp, rbuf.remaining());
      Operation op = node.removeCurrentReadOp();
      assert op == currentOp : "Expected to pop " + currentOp + " got " + op;
      return node.getCurrentReadOp();
    } else {
      throw new IOException("Disconnected unexpected, will reconnect.");
    }
  }

  private void handleReads(final MemcachedNode node) throws IOException {
    System.out.println("READ DO UDPMEMCACHEDCONNECTION");
    Operation currentOp = node.getCurrentReadOp();
    System.out.println("currentOp: " + currentOp);
    if (currentOp instanceof TapAckOperationImpl) {
      node.removeCurrentReadOp();
      return;
    }
    
    ByteBuffer rbuf = node.getRbuf();
    final DatagramChannel channel = ((UDPMemcachedNodeImpl)node).getDatagramChannel();

    int read = channel.read(rbuf) - 2;
    metrics.updateHistogram(OVERALL_AVG_BYTES_READ_METRIC, read);
    //read preceding short identifying the packet number
    rbuf.getShort();

    if (read < 0) {
      currentOp = handleReadsWhenChannelEndOfStream(currentOp, node, rbuf);
    }

    while (read > 0) {
      getLogger().debug("Read %d bytes", read);
      rbuf.flip();
      while (rbuf.remaining() > 0) {
        System.out.println("rbuf.remaining() > 0");
        if (currentOp == null) {
          throw new IllegalStateException("No read operation.");
        }

        long timeOnWire =
          System.nanoTime() - currentOp.getWriteCompleteTimestamp();
        metrics.updateHistogram(OVERALL_AVG_TIME_ON_WIRE_METRIC,
          (int)(timeOnWire / 1000));
        metrics.markMeter(OVERALL_RESPONSE_METRIC);
        synchronized(currentOp) {
          readBufferAndLogMetrics(currentOp, rbuf, node);
        }

        currentOp = node.getCurrentReadOp();
      }
      rbuf.clear();
      read = channel.read(rbuf) - 2;
      node.completedRead();
    }
  }

  private void readBufferAndLogMetrics(final Operation currentOp,
          final ByteBuffer rbuf, final MemcachedNode node) throws IOException {
    currentOp.readFromBuffer(rbuf);
    if (currentOp.getState() == OperationState.COMPLETE) {
      System.out.println("Completed read op");
      getLogger().debug("Completed read op: %s and giving the next %d "
        + "bytes", currentOp, rbuf.remaining());
      Operation op = node.removeCurrentReadOp();
      assert op == currentOp : "Expected to pop " + currentOp + " got "
        + op;

      if (op.hasErrored()) {
        metrics.markMeter(OVERALL_RESPONSE_FAIL_METRIC);
      } else {
        metrics.markMeter(OVERALL_RESPONSE_SUCC_METRIC);
      }
    } else if (currentOp.getState() == OperationState.RETRY) {
      System.out.println("Retrying op");
      handleRetryInformation(currentOp.getErrorMsg());
      getLogger().debug("Reschedule read op due to NOT_MY_VBUCKET error: "
        + "%s ", currentOp);
      ((VBucketAware) currentOp).addNotMyVbucketNode(
        currentOp.getHandlingNode());
      Operation op = node.removeCurrentReadOp();
      assert op == currentOp : "Expected to pop " + currentOp + " got "
        + op;

      retryOps.add(currentOp);
      metrics.markMeter(OVERALL_RESPONSE_RETRY_METRIC);
    }
  }

  private void handleWrites(final MemcachedNode node) throws IOException {
    node.fillWriteBuffer(false);
    boolean canWriteMore = node.getBytesRemainingToWrite() > 0;
    while (canWriteMore) {
      int wrote = node.writeSome();
      metrics.updateHistogram(OVERALL_AVG_BYTES_WRITE_METRIC, wrote);
      node.fillWriteBuffer(false);
      canWriteMore = wrote > 0 && node.getBytesRemainingToWrite() > 0;
    }
  }

  private void handleInputQueue() {
    if (!addedQueue.isEmpty()) {
      getLogger().debug("Handling queue");
      Collection<MemcachedNode> toAdd = new HashSet<MemcachedNode>();
      Collection<MemcachedNode> todo = new HashSet<MemcachedNode>();
      
      MemcachedNode qaNode;
      while ((qaNode = addedQueue.poll()) != null) {
        todo.add(qaNode);
      }

      for (MemcachedNode node : todo) {
        boolean readyForIO = false;
        if(node.isActive()){
          if (node.getCurrentWriteOp() != null) {
            readyForIO = true;
            getLogger().debug("Handling queued write %s", node);
          }
        }else {
          toAdd.add(node);
        }
        
        node.copyInputQueue();
        if (readyForIO) {
          try {
            if (node.getWbuf().hasRemaining()) {
              handleWrites(node);
            }
          } catch (IOException e) {
            System.out.println("Exception handling write");
          }
        }
        node.fixupOps();
      }
      addedQueue.addAll(toAdd);
    }
  }

  @Override
  public void handleIO() throws IOException {
    if (shutDown) {
      getLogger().debug("No IO while shut down.");
      return;
    }

    handleInputQueue();

    assert selectorsMakeSense() : "Selectors don't make sense.";
    int selected = selector.select();

    if (shutDown) {
      return;
    } else if (selected == 0 && addedQueue.isEmpty()) {
      handleWokenUpSelector();
    } else if (selector.selectedKeys().isEmpty()) {
      handleEmptySelects();
    } else {
      getLogger().debug("Selected %d, selected %d keys", selected,
        selector.selectedKeys().size());
      emptySelects = 0;

      Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
      while(iterator.hasNext()) {
        SelectionKey sk = iterator.next();
        handleIO(sk);
        iterator.remove();
      }
    }
  }

  private void handleEmptySelects() {
    getLogger().debug("No selectors ready, interrupted: "
      + Thread.interrupted());

    if (++emptySelects > DOUBLE_CHECK_EMPTY) {
      for (SelectionKey sk : selector.keys()) {
        getLogger().debug("%s has %s, interested in %s", sk, sk.readyOps(),
          sk.interestOps());
        if (sk.readyOps() != 0) {
          getLogger().debug("%s has a ready op, handling IO", sk);
          handleIO(sk);
        } else {
          lostConnection((MemcachedNode) sk.attachment());
        }
      }
      assert emptySelects < EXCESSIVE_EMPTY : "Too many empty selects";
    }
  }

  private void lostConnection(final MemcachedNode node) {
    queueReconnect(node);
    for (ConnectionObserver observer : connObservers) {
      observer.connectionLost(node.getSocketAddress());
    }
  }

  private boolean selectorsMakeSense() {
    for (MemcachedNode qa : locator.getAll()) {
      if (qa.getSk() != null && qa.getSk().isValid()) {
        if (qa.getChannel().isConnected()) {
          int sops = qa.getSk().interestOps();
          int expected = 0;
          if (qa.hasReadOp()) {
            expected |= SelectionKey.OP_READ;
          }
          if (qa.hasWriteOp()) {
            expected |= SelectionKey.OP_WRITE;
          }
          if (qa.getBytesRemainingToWrite() > 0) {
            expected |= SelectionKey.OP_WRITE;
          }
          assert sops == expected : "Invalid ops:  " + qa + ", expected "
            + expected + ", got " + sops;
        } else {
          int sops = qa.getSk().interestOps();
          assert sops == SelectionKey.OP_CONNECT
            : "Not connected, and not watching for connect: " + sops;
        }
      }
    }
    getLogger().debug("Checked the selectors.");
    return true;
  }

  boolean belongsToCluster(final MemcachedNode node) {
    for (MemcachedNode n : locator.getAll()) {
      if (n.getSocketAddress().equals(node.getSocketAddress())) {
        return true;
      }
    }
    return false;
  }

  private void handleIO(final SelectionKey sk) {
    MemcachedNode node = (MemcachedNode) sk.attachment();

    try {
      getLogger().debug("Handling IO for:  %s (r=%s, w=%s, c=%s, op=%s)", sk,
        sk.isReadable(), sk.isWritable(), sk.isConnectable(),
        sk.attachment());
      if (sk.isConnectable() && belongsToCluster(node)) {
        getLogger().debug("Connection state changed for %s", sk);
        final DatagramChannel channel = ((UDPMemcachedNodeImpl)node).getDatagramChannel();
        
        finishConnect(sk, node);
        
      } else {
        handleReadsAndWrites(sk, node);
      }
    } catch (ClosedChannelException e) {
      if (!shutDown) {
        getLogger().info("Closed channel and not shutting down. Queueing"
            + " reconnect on %s", node, e);
        lostConnection(node);
      }
    } catch (ConnectException e) {
      getLogger().info("Reconnecting due to failure to connect to %s", node, e);
      queueReconnect(node);
    } catch (OperationException e) {
      node.setupForAuth();
      getLogger().info("Reconnection due to exception handling a memcached "
        + "operation on %s. This may be due to an authentication failure.",
        node, e);
      lostConnection(node);
    } catch (Exception e) {
      node.setupForAuth();
      getLogger().info("Reconnecting due to exception on %s", node, e);
      lostConnection(node);
    }
    node.fixupOps();
  }

  static String dbgBuffer(ByteBuffer b, int size) {
    StringBuilder sb = new StringBuilder();
    byte[] bytes = new byte[size];
    b.get(bytes);
    for (int i = 0; i < size; i++) {
      char ch = (char) bytes[i];
      if (Character.isWhitespace(ch) || Character.isLetterOrDigit(ch)) {
        sb.append(ch);
      } else {
        sb.append("\\x");
        sb.append(Integer.toHexString(bytes[i] & 0xff));
      }
    }
    return sb.toString();
  }

  private void finishConnect(final SelectionKey sk, final MemcachedNode node)
    throws IOException {
    if (verifyAliveOnConnect) {
      final CountDownLatch latch = new CountDownLatch(1);
      final OperationFuture<Boolean> rv = new OperationFuture<Boolean>("noop",
        latch, 2500, listenerExecutorService);
      NoopOperation testOp = opFact.noop(new OperationCallback() {
        public void receivedStatus(OperationStatus status) {
          rv.set(status.isSuccess(), status);
        }

        @Override
        public void complete() {
          latch.countDown();
        }
      });

      testOp.setHandlingNode(node);
      testOp.initialize();
      checkState();
      insertOperation(node, testOp);
      node.copyInputQueue();

      boolean done = false;
      if (sk.isValid()) {
        long timeout = TimeUnit.MILLISECONDS.toNanos(
          connectionFactory.getOperationTimeout());

        long stop = System.nanoTime() + timeout;
        while (stop > System.nanoTime()) {
          handleWrites(node);
          handleReads(node);
          if(done = (latch.getCount() == 0)) {
            break;
          }
        }
      }

      if (!done || testOp.isCancelled() || testOp.hasErrored()
        || testOp.isTimedOut()) {
        throw new ConnectException("Could not send noop upon connect! "
          + "This may indicate a running, but not responding memcached "
          + "instance.");
      }
    }

    connected(node);
    addedQueue.offer(node);
    if (node.getWbuf().hasRemaining()) {
      handleWrites(node);
    }
  }
}