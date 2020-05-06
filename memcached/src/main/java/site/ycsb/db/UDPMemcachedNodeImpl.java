package site.ycsb.db;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.FailureMode;
import net.spy.memcached.MemcachedConnection;
import net.spy.memcached.MemcachedNode;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.protocol.binary.TapAckOperationImpl;

public class UDPMemcachedNodeImpl implements MemcachedNode {

    private static final int MAX_REPLY_SIZE = 65536;


    private SocketAddress socketAddress;
    private DatagramSocket datagramSocket;
    private String ip;
    private int port;
    protected final BlockingQueue<Operation> writeQ;
    private final BlockingQueue<Operation> readQ;
    private final BlockingQueue<Operation> inputQueue;
    private final long opQueueMaxBlockTime;
    private long defaultOpTimeout;
    private int toWrite = 0;
    private final Queue<Integer> opsSize;
    private final ByteBuffer rbuf;
    private final ByteBuffer wbuf;
    private volatile long lastReadTimestamp = System.nanoTime();
    private volatile SelectionKey sk = null;
    private ArrayList<Operation> reconnectBlocked;
    private AtomicInteger reconnectAttempt = new AtomicInteger(1);
    private final AtomicInteger continuousTimeout = new AtomicInteger(0);
    private CountDownLatch authLatch;
    private boolean shouldAuth = false;
    private final long authWaitTime;
    private DatagramChannel channel;
    private MemcachedConnection connection;
    private ByteBuffer datagramBuffer;
    private List<Integer> datagramSizes;
    private final ConnectionFactory connectionFactory;

    private short requestId;

    public UDPMemcachedNodeImpl(SocketAddress sa, DatagramChannel c, int bufSize, BlockingQueue<Operation> rq, 
                BlockingQueue<Operation> wq, BlockingQueue<Operation> iq, 
                long opQueueMaxBlockTime, long dt, long authWaitTime, ConnectionFactory cf) throws SocketException {

        this.socketAddress = sa;

        String address = sa.toString().substring(1);
        String tokens[] = address.split(":");
        this.ip = tokens[0];
        this.port = Integer.parseInt(tokens[1]);
        this.requestId = 0;
        inputQueue = iq;
        this.opQueueMaxBlockTime = opQueueMaxBlockTime;
        this.writeQ = wq;
        this.readQ = rq;
        this.defaultOpTimeout = dt;
        this.opsSize = new LinkedList<>();
        rbuf = ByteBuffer.allocateDirect(bufSize);
        wbuf = ByteBuffer.allocateDirect(bufSize);
        getWbuf().clear();
        this.authWaitTime = authWaitTime;
        setupForAuth();
        datagramBuffer = ByteBuffer.allocateDirect(MAX_REPLY_SIZE);
        datagramSizes = new LinkedList<>();
        setDatagramChannel(c);
        this.connectionFactory = cf;
    }

    /**
     * 
     * @return a linkedlist with the size and the the datagram data
     */
    public List<?> getNextDatagram(){
        List l = new LinkedList<>();
        int size = datagramSizes.remove(0);
        l.add(size);
        l.add(ByteBuffer.allocateDirect(size).put(datagramBuffer.array(), 0, size));

        return l;
    }

    private Operation getNextWritableOp() {
        Operation o = getCurrentWriteOp();
        while (o != null && o.getState() == OperationState.WRITE_QUEUED) {
          synchronized(o) {
            if (o.isCancelled()) {
              //getLogger().debug("Not writing cancelled op.");
              Operation cancelledOp = removeCurrentWriteOp();
              assert o == cancelledOp;
            } else if (o.isTimedOut(defaultOpTimeout)) {
              //getLogger().debug("Not writing timed out op.");
              Operation timedOutOp = removeCurrentWriteOp();
              assert o == timedOutOp;
            } else {
              o.writing();
              if (!(o instanceof TapAckOperationImpl)) {
                readQ.add(o);
              }
              return o;
            }
            o = getCurrentWriteOp();
          }
        }
        return o;
    }

    private boolean preparePending() {
        // Copy the input queue into the write queue.
        copyInputQueue();
    
        // Now check the ops
        Operation nextOp = getCurrentWriteOp();
        while (nextOp != null && nextOp.isCancelled()) {
          //getLogger().info("Removing cancelled operation: %s", nextOp);
          removeCurrentWriteOp();
          nextOp = getCurrentWriteOp();
        }
        return nextOp != null;
    }

    @Override
    public void copyInputQueue() {
        Collection<Operation> tmp = new ArrayList<Operation>();

        // don't drain more than we have space to place
        inputQueue.drainTo(tmp, writeQ.remainingCapacity());
        writeQ.addAll(tmp);
    }

    @Override
    public Collection<Operation> destroyInputQueue() {
        Collection<Operation> rv = new ArrayList<Operation>();
        inputQueue.drainTo(rv);
        return rv;
    }

    @Override
    public void setupResend() {
        // First, reset the current write op, or cancel it if we should
        // be authenticating
        Operation op = getCurrentWriteOp();
        if (shouldAuth && op != null) {
            op.cancel();
        } else if (op != null) {
            ByteBuffer buf = op.getBuffer();
            if (buf != null) {
                buf.reset();
            } else {
                //getLogger().info("No buffer for current write op, removing");
                removeCurrentWriteOp();
            }
        }
        // Now cancel all the pending read operations. Might be better to
        // to requeue them.
        while (hasReadOp()) {
            op = removeCurrentReadOp();
            if (op != getCurrentWriteOp()) {
                //getLogger().warn("Discarding partially completed op: %s", op);
                op.cancel();
            }
        }

        while (shouldAuth && hasWriteOp()) {
            op = removeCurrentWriteOp();
           //  getLogger().warn("Discarding partially completed op: %s", op);
            op.cancel();
        }

        getWbuf().clear();
        getRbuf().clear();
        toWrite = 0;
    }

    @Override
    public void fillWriteBuffer(boolean optimizeGets) {
        if (toWrite == 0 && readQ.remainingCapacity() > 0) {
            getWbuf().clear();
            Operation o=getNextWritableOp();

            while(o != null && toWrite < getWbuf().capacity()) {
                synchronized(o) {
                assert o.getState() == OperationState.WRITING;
        
                ByteBuffer obuf = o.getBuffer();
                assert obuf != null : "Didn't get a write buffer from " + o;
                int bytesToCopy = Math.min(getWbuf().remaining(), obuf.remaining());
                byte[] b = new byte[bytesToCopy];
                obuf.get(b);
                getWbuf().put(b);
                opsSize.add(bytesToCopy);
                //getLogger().debug("After copying stuff from %s: %s", o, getWbuf());
                if (!o.getBuffer().hasRemaining()) {
                    o.writeComplete();
                    transitionWriteItem();
        
                    preparePending();
        
                    o=getNextWritableOp();
                }
                toWrite += bytesToCopy;
                }
            }
            getWbuf().flip();
            assert toWrite <= getWbuf().capacity() : "toWrite exceeded capacity: "
                + this;
            assert toWrite == getWbuf().remaining() : "Expected " + toWrite
                + " remaining, got " + getWbuf().remaining();
        } else {
            System.out.println("Buffer is full, skipping");
        }
    }

    @Override
    public void transitionWriteItem() {
        Operation op = removeCurrentWriteOp();
        assert op != null : "There is no write item to transition";
        //getLogger().debug("Finished writing %s", op);
    }

    @Override
    public Operation getCurrentReadOp() {
        return readQ.peek();
    }

    @Override
    public Operation removeCurrentReadOp() {
        return readQ.remove();
    }

    @Override
    public Operation getCurrentWriteOp() {
       return writeQ.peek();
    }

    @Override
    public Operation removeCurrentWriteOp() {
        return writeQ.remove();
    }

    @Override
    public boolean hasReadOp() {
        return !readQ.isEmpty();
    }

    @Override
    public boolean hasWriteOp() {
        return !writeQ.isEmpty();
    }

    @Override
    public void addOp(Operation op){
        try{
            if (!authLatch.await(authWaitTime, TimeUnit.MILLISECONDS)) {
                FailureMode mode = connectionFactory.getFailureMode();
                if (mode == FailureMode.Redistribute || mode == FailureMode.Retry) {
                /*getLogger().debug("Redistributing Operation " + op + " because auth "
                    + "latch taken longer than " + authWaitTime + " milliseconds to "
                    + "complete on node " + getSocketAddress());*/
                connection.retryOperation(op);
                } else {
                op.cancel();
                /*getLogger().warn("Operation canceled because authentication "
                    + "or reconnection and authentication has "
                    + "taken more than " + authWaitTime + " milliseconds to "
                    + "complete on node " + this);
                getLogger().debug("Canceled operation %s", op.toString());*/
                }
                return;
            }

            try {
                if (!this.inputQueue.offer(op, opQueueMaxBlockTime, TimeUnit.MILLISECONDS)) {
                    throw new IllegalStateException(
                            "Timed out waiting to add " + op + "(max wait=" + opQueueMaxBlockTime + "ms)");
                }
            } catch (InterruptedException e) {
                System.out.println("addOp: Error offering to inputQUeue");
            }
        } catch (InterruptedException e) {
            // Restore the interrupted status
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting to add " + op);
        }
    }

    @Override
    public void insertOp(Operation op) {
        ArrayList<Operation> tmp = new ArrayList<Operation>(inputQueue.size() + 1);
        tmp.add(op);
        inputQueue.drainTo(tmp);
        inputQueue.addAll(tmp);
    }

    @Override
    public int getSelectionOps() {
        int rv = 0;
        
        if (getDatagramChannel().isConnected()) {
            if (hasReadOp()) {
              rv |= SelectionKey.OP_READ;
            }
            if (toWrite > 0 || hasWriteOp()) {
              rv |= SelectionKey.OP_WRITE;
            }
        } else {
            rv = SelectionKey.OP_CONNECT;
        }
        
        return rv;
    }

    @Override
    public ByteBuffer getRbuf() {
        return rbuf;
    }

    @Override
    public ByteBuffer getWbuf() {
        return wbuf;
    }

    @Override
    public SocketAddress getSocketAddress() {
        return socketAddress;
    }

    @Override
    public boolean isActive() {
        return getDatagramChannel() != null
        && getDatagramChannel().isConnected();
    }

    @Override
    public boolean isAuthenticated() {
        return (0 == authLatch.getCount());
    }

    @Override
    public long lastReadDelta() {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastReadTimestamp);
    }

    @Override
    public void completedRead() {
        lastReadTimestamp = System.nanoTime();
    }

    @Override
    public void reconnecting() {
        reconnectAttempt.incrementAndGet();
        continuousTimeout.set(0);
    }

    @Override
    public void connected() {
        reconnectAttempt.set(0);
        continuousTimeout.set(0);
    }

    @Override
    public int getReconnectCount() {
        return reconnectAttempt.get();
    }

    public void registerDatagramChannel(DatagramChannel ch, SelectionKey skey) {
        setDatagramChannel(ch);
        setSk(skey);
    }

    @Override
    public void registerChannel(SocketChannel ch, SelectionKey skey) {
        setChannel(ch);
        setSk(skey);
    }

    public void setDatagramChannel(DatagramChannel to){
        assert channel == null || !channel.isOpen()
      : "Attempting to overwrite channel";
        channel = to;
    }

    public DatagramChannel getDatagramChannel(){
        return channel;
    }

    @Override
    public void setChannel(SocketChannel to) {
        return;
    }

    @Override
    public SocketChannel getChannel() {
        return null;
    }

    @Override
    public void setSk(SelectionKey to) {
        sk = to;
    }

    @Override
    public SelectionKey getSk() {
        return sk;
    }

    @Override
    public int getBytesRemainingToWrite() {
        return toWrite;
    }

    @Override
    public int writeSome() throws IOException {
        int wrote = 0;

        while(wbuf.hasRemaining()){
            int bytesToCopy = opsSize.remove();
            byte[] b = new byte[bytesToCopy + 8];

            b[0] = String.valueOf(requestId / 256).getBytes()[0];
            b[1] = String.valueOf(requestId % 256).getBytes()[0];
            b[2] = '0';
            b[3] = '0';
            b[4] = '0';
            b[5] = '1';
            b[6] = '0';
            b[7] = '0';

            requestId++;
            getWbuf().get(b, 8, bytesToCopy);

            channel.write(ByteBuffer.wrap(b));

            wrote += bytesToCopy;
        }

        assert wrote >= 0 : "Wrote negative bytes?";
        toWrite -= wrote;
        assert toWrite >= 0 : "toWrite went negative after writing " + wrote
        + " bytes for " + this;
        //getLogger().debug("Wrote %d bytes", wrote);
        return wrote;
    }

    @Override
    public void fixupOps() {
        SelectionKey s = sk;
        if (s != null && s.isValid()) {
            int iops = getSelectionOps();
            //getLogger().debug("Setting interested opts to %d", iops);
            s.interestOps(iops);
        } else {
            System.out.println("Selection key is not valid.");
        }
    }

    @Override
    public void authComplete() {
        if (reconnectBlocked != null && reconnectBlocked.size() > 0) {
            inputQueue.addAll(reconnectBlocked);
        }
        authLatch.countDown();
    }

    @Override
    public void setupForAuth() {
        if (shouldAuth) {
            authLatch = new CountDownLatch(1);
            if (inputQueue.size() > 0) {
                reconnectBlocked = new ArrayList<Operation>(inputQueue.size() + 1);
                inputQueue.drainTo(reconnectBlocked);
            }
            assert (inputQueue.size() == 0);
            setupResend();
        } else {
            authLatch = new CountDownLatch(0);
        }
    }

    @Override
    public void setContinuousTimeout(boolean timedOut) {
        if (timedOut && isActive()) {
            continuousTimeout.incrementAndGet();
          } else {
            continuousTimeout.set(0);
        }
    }

    @Override
    public int getContinuousTimeout() {
        return continuousTimeout.get();
    }

    @Override
    public MemcachedConnection getConnection() {
        return connection;
    }

    @Override
    public void setConnection(MemcachedConnection connection) {
        this.connection = connection;
    }
}