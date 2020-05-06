package site.ycsb.db;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SocketChannel;
import java.util.List;

import net.spy.memcached.DefaultConnectionFactory;
import net.spy.memcached.MemcachedConnection;
import net.spy.memcached.MemcachedNode;

public class UDPDefaultConnFactory extends DefaultConnectionFactory {
    private int thisThreadN;
    public UDPDefaultConnFactory(int threadN) {
        super();
        this.thisThreadN = threadN;
    }

    @Override
    public MemcachedConnection createConnection(List<InetSocketAddress> addrs) throws IOException {
        return new UDPMemcachedConnection(getReadBufSize(), this, addrs, getInitialObservers(), getFailureMode(),
                getOperationFactory(), thisThreadN);
    }

    public MemcachedNode createMemcachedNode(SocketAddress sa, DatagramChannel c, int bufSize) {
        try {
            return new UDPMemcachedNodeImpl(sa, c, DEFAULT_READ_BUFFER_SIZE, createReadOperationQueue(), 
                        createWriteOperationQueue(), createOperationQueue(), 
                        getOpQueueMaxBlockTime(), getOperationTimeout(),
                        getAuthWaitTime(), this);
        } catch (SocketException e) {
            System.out.println("createMemcachedNode: Error while creating UDP node");
        }

        return null;
    }
}