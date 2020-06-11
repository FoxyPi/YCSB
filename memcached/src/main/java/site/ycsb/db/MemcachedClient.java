/**
 * Copyright (c) 2014-2015 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb.db;

import site.ycsb.ByteIterator;
import site.ycsb.Client;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.DefaultHashAlgorithm;
import net.spy.memcached.FailureMode;
// We also use `net.spy.memcached.MemcachedClient`; it is not imported
// explicitly and referred to with its full path to avoid conflicts with the
// class of the same name in this file.
import net.spy.memcached.internal.GetFuture;
import net.spy.memcached.internal.OperationFuture;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import org.apache.log4j.Logger;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Concrete Memcached client implementation.
 */
public class MemcachedClient extends DB {

  private final Logger logger = Logger.getLogger(getClass());

  protected static final ObjectMapper MAPPER = new ObjectMapper();

  private boolean checkOperationStatus;
  private long shutdownTimeoutMillis;
  private int objectExpirationTime;

  public static final String HOSTS_PROPERTY = "memcached.hosts";

  public static final int DEFAULT_PORT = 11211;

  private static final String TEMPORARY_FAILURE_MSG = "Temporary failure";
  private static final String CANCELLED_MSG = "cancelled";

  public static final String SHUTDOWN_TIMEOUT_MILLIS_PROPERTY =
      "memcached.shutdownTimeoutMillis";
  public static final String DEFAULT_SHUTDOWN_TIMEOUT_MILLIS = "30000";

  public static final String OBJECT_EXPIRATION_TIME_PROPERTY =
      "memcached.objectExpirationTime";
  public static final String DEFAULT_OBJECT_EXPIRATION_TIME =
      String.valueOf(Integer.MAX_VALUE);

  public static final String CHECK_OPERATION_STATUS_PROPERTY =
      "memcached.checkOperationStatus";
  public static final String CHECK_OPERATION_STATUS_DEFAULT = "true";

  public static final String READ_BUFFER_SIZE_PROPERTY =
      "memcached.readBufferSize";
  public static final String DEFAULT_READ_BUFFER_SIZE = "3000000";

  public static final String OP_TIMEOUT_PROPERTY = "memcached.opTimeoutMillis";
  public static final String DEFAULT_OP_TIMEOUT = "60000";

  public static final String FAILURE_MODE_PROPERTY = "memcached.failureMode";
  public static final FailureMode FAILURE_MODE_PROPERTY_DEFAULT =
      FailureMode.Redistribute;

  public static final String PROTOCOL_PROPERTY = "memcached.protocol";
  public static final ConnectionFactoryBuilder.Protocol DEFAULT_PROTOCOL =
      ConnectionFactoryBuilder.Protocol.TEXT;

  /**
   * The MemcachedClient implementation that will be used to communicate
   * with the memcached server.
   */
  private net.spy.memcached.MemcachedClient[] client;

  private Map<Integer, Byte> keyLambda; 

  /**
   * @returns Underlying Memcached protocol client, implemented by
   *     SpyMemcached.
   */
  protected net.spy.memcached.MemcachedClient memcachedClient(int index) {
    return client[index];
  }

  private static int threadCount;

  public static long _MurmurHash3_ (byte[] data, int lambda, int length)
  {
    final int c1 = 0xcc9e2d51;
    final int c2 = 0x1b873593;
    int h1 = 0;

    int roundedEnd = length & 0xfffffffc;  // round down to 4 byte block

    //lambda
    int k1 = lambda*'A';

    k1 *= c1;
    k1 = (k1 << 15) | (k1 >> 17);  //ROTL32(k1,15);
    k1 *= c2;

    h1 ^= k1;
    h1 = (h1 << 13) | (h1 >> 19);  //ROTL32(h1,13);
    h1 = h1*5+0xe6546b64;




    for (int i=0; i<length - 4; i+=4) {
      // little endian load order
      k1 = (data[i] & 0xff) | ((data[i+1] & 0xff) << 8) | ((data[i+2] & 0xff) << 16) | (data[i+3] << 24);
      
      k1 *= c1;
      k1 = (k1 << 15) | (k1 >> 17);  // ROTL32(k1,15);
      k1 *= c2;

      h1 ^= k1;
      //h1 = (h1 << 13) | (h1 >> 19);  // ROTL32(h1,13);
      h1 = h1*5+0xe6546b64;
    }

    // tail
    k1 = 0;

    switch(length & 0x03) {
      case 3:
        k1 = (data[roundedEnd + 2] & 0xff) << 16;
        // fallthrough
      case 2:
        k1 |= (data[roundedEnd + 1] & 0xff) << 8;
        // fallthrough
      case 1:
        k1 |= (data[roundedEnd] & 0xff);
        k1 *= c1;
        k1 = (k1 << 15) | (k1 >> 17);  // ROTL32(k1,15);
        k1 *= c2;
        h1 ^= k1;
    }

    // finalization
    h1 ^= length;

    // fmix(h1);
    h1 ^= h1 >> 16;
    h1 *= 0x85ebca6b;
    h1 ^= h1 >> 13;
    h1 *= 0xc2b2ae35;
    h1 ^= h1 >> 16;

    return h1;
  }

  public static int djb2_variant_index_hash(String str,int size, int lambda)
  {
    // changed to murmurhash due to colisions and visible patterns
    long hashy =  _MurmurHash3_(str.getBytes(), lambda, size);
    
    int res = (int) Math.abs(hashy % threadCount);
    return res;
  }


  @Override
  public void init() throws DBException {
    threadCount = Client.threadcount;
    client = new net.spy.memcached.MemcachedClient[threadCount];
    keyLambda = new HashMap<>();
    try {
      client[0] = createMemcachedClient(0);
      checkOperationStatus = Boolean.parseBoolean(
          getProperties().getProperty(CHECK_OPERATION_STATUS_PROPERTY,
                                      CHECK_OPERATION_STATUS_DEFAULT));
      objectExpirationTime = Integer.parseInt(
          getProperties().getProperty(OBJECT_EXPIRATION_TIME_PROPERTY,
                                      DEFAULT_OBJECT_EXPIRATION_TIME));
      shutdownTimeoutMillis = Integer.parseInt(
          getProperties().getProperty(SHUTDOWN_TIMEOUT_MILLIS_PROPERTY,
                                      DEFAULT_SHUTDOWN_TIMEOUT_MILLIS));
    } catch (Exception e) {
      throw new DBException(e);
    }
  }

  protected net.spy.memcached.MemcachedClient createMemcachedClient(int index)
      throws Exception {
      
    ConnectionFactoryBuilder udpConnectionFactoryBuilder = new UDPConnFactoryBuilder();
    
    udpConnectionFactoryBuilder.setReadBufferSize(Integer.parseInt(
        getProperties().getProperty(READ_BUFFER_SIZE_PROPERTY,
                                    DEFAULT_READ_BUFFER_SIZE)));

    udpConnectionFactoryBuilder.setOpTimeout(Integer.parseInt(
        getProperties().getProperty(OP_TIMEOUT_PROPERTY, DEFAULT_OP_TIMEOUT)));

    String protocolString = getProperties().getProperty(PROTOCOL_PROPERTY);
    udpConnectionFactoryBuilder.setProtocol(
        protocolString == null ? DEFAULT_PROTOCOL
                         : ConnectionFactoryBuilder.Protocol.valueOf(protocolString.toUpperCase()));

    String failureString = getProperties().getProperty(FAILURE_MODE_PROPERTY);
    udpConnectionFactoryBuilder.setFailureMode(
        failureString == null ? FAILURE_MODE_PROPERTY_DEFAULT
                              : FailureMode.valueOf(failureString));

    // Note: this only works with IPv4 addresses due to its assumption of
    // ":" being the separator of hostname/IP and port; this is not the case
    // when dealing with IPv6 addresses.
    //
    // TODO(mbrukman): fix this.
    List<InetSocketAddress> addresses = new ArrayList<InetSocketAddress>();
    String[] hosts = getProperties().getProperty(HOSTS_PROPERTY).split(",");
    for (String address : hosts) {
      int colon = address.indexOf(":");
      int port = DEFAULT_PORT;
      String host = address;
      if (colon != -1) {
        port = Integer.parseInt(address.substring(colon + 1)) + index;
        host = address.substring(0, colon);
      }
      addresses.add(new InetSocketAddress(host, port));
    }
    return new net.spy.memcached.MemcachedClient(
        udpConnectionFactoryBuilder.build(), addresses);
  }

  @Override
  public Status read(
      String table, String key, Set<String> fields,
      Map<String, ByteIterator> result) {
    key = createQualifiedKey(table, key);
    
    boolean retry = false;

    for(int i = 0; i < 2; i++){
      try {
        int keyHash = (int) DefaultHashAlgorithm.CRC_HASH.hash(key);
        
        if(!keyLambda.containsKey(keyHash))
          keyLambda.put(keyHash, (byte)0);
        
        byte lambda = retry ? 0 : (byte)(Math.random() * (keyLambda.get(keyHash) + 1));
        
        //int index =  (keyHash % threadCount) + (int)(Math.random() * (keyLambda.get(keyHash) + 1)); 
        int index = djb2_variant_index_hash(key, key.length(), lambda);
        //int index = djb2_variant_index_hash(key, key.length(), 0);
        
        net.spy.memcached.MemcachedClient memClient = client[index];
        
        if(memClient == null){
          memClient = createMemcachedClient(index);
          client[index] = memClient;
        }
        GetFuture<Object> future = memClient.asyncGet(key);
        Object document = future.get();

        lambda = Byte.parseByte(((String) document).substring(3, 5));
        keyLambda.put(keyHash, lambda);
        
        //System.out.println("key: " + key + "lambda: " + lambda + "index: " + index);

        if (document != null) {
          fromJson(((String) document).substring(10), fields, result);
        }

        return Status.OK;
      } catch (Exception e) {
        System.out.println("CHAVE AO POSTE");
        logger.error("Error encountered for key: " + key, e);
        retry = true;
      }
    }
    return Status.ERROR;
  }

  @Override
  public Status scan(
      String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result){
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status update(
      String table, String key, Map<String, ByteIterator> values) {
    key = createQualifiedKey(table, key);
    try {
      int keyHash = (int) DefaultHashAlgorithm.CRC_HASH.hash(key);
      if(!keyLambda.containsKey(keyHash))
        keyLambda.put(keyHash, (byte)0);
      
      
      //int index =  (keyHash % threadCount) + (int)(Math.random() * (keyLambda.get(keyHash) + 1)); 
      //int index = djb2_variant_index_hash(key, key.length(), (int)(Math.random() * (keyLambda.get(keyHash) + 1)));
      int index = djb2_variant_index_hash(key, key.length(), 0);

      net.spy.memcached.MemcachedClient memClient = client[index];
      
      if(memClient == null){
        memClient = createMemcachedClient(index);
        client[index] = memClient;
      }

      OperationFuture<Boolean> future =
          memClient.replace(key, objectExpirationTime, toJson(values));
      return getReturnCode(future);
    } catch (Exception e) {
      logger.error("Error updating value with key: " + key, e);
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(
      String table, String key, Map<String, ByteIterator> values) {
    key = createQualifiedKey(table, key);
    try {
      int keyHash = (int) DefaultHashAlgorithm.CRC_HASH.hash(key);
      if(!keyLambda.containsKey(keyHash))
        keyLambda.put(keyHash, (byte)0);

      int index = djb2_variant_index_hash(key, key.length(), (int)(Math.random() * (keyLambda.get(keyHash) + 1)));
      System.out.println("key: " + key + "index: " + index);
      
      net.spy.memcached.MemcachedClient memClient = client[index];
      if(memClient == null){
        memClient = createMemcachedClient(index);
        client[index] = memClient;
      }

      OperationFuture<Boolean> future =
          memClient.add(key, objectExpirationTime, toJson(values));
      return getReturnCode(future);
    } catch (Exception e) {
      logger.error("Error inserting value", e);
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    key = createQualifiedKey(table, key);
    try {
      int index = (int) (DefaultHashAlgorithm.CRC_HASH.hash(key) % threadCount);
      net.spy.memcached.MemcachedClient memClient = client[index];
      
      if(memClient == null){
        memClient = createMemcachedClient(index);
        client[index] = memClient;
      }

      OperationFuture<Boolean> future = memClient.delete(key);
      return getReturnCode(future);
    } catch (Exception e) {
      logger.error("Error deleting value", e);
      return Status.ERROR;
    }
  }

  protected Status getReturnCode(OperationFuture<Boolean> future) {
    if (!checkOperationStatus) {
      return Status.OK;
    }
    if (future.getStatus().isSuccess()) {
      return Status.OK;
    } else if (TEMPORARY_FAILURE_MSG.equals(future.getStatus().getMessage())) {
      return new Status("TEMPORARY_FAILURE", TEMPORARY_FAILURE_MSG);
    } else if (CANCELLED_MSG.equals(future.getStatus().getMessage())) {
      return new Status("CANCELLED_MSG", CANCELLED_MSG);
    }
    return new Status("ERROR", future.getStatus().getMessage());
  }

  @Override
  public void cleanup() throws DBException {
    for(int i = 0; i < threadCount; i++){
      net.spy.memcached.MemcachedClient memClient = client[i];

      if (memClient != null) {
        memClient.shutdown(shutdownTimeoutMillis, MILLISECONDS);
      }
    }
  }

  protected static String createQualifiedKey(String table, String key) {
    return MessageFormat.format("{0}-{1}", table, key);
  }

  protected static void fromJson(
      String value, Set<String> fields,
      Map<String, ByteIterator> result) throws IOException {
    JsonNode json = MAPPER.readTree(value);
    boolean checkFields = fields != null && !fields.isEmpty();
    for (Iterator<Map.Entry<String, JsonNode>> jsonFields = json.getFields();
         jsonFields.hasNext();
         /* increment in loop body */) {
      Map.Entry<String, JsonNode> jsonField = jsonFields.next();
      String name = jsonField.getKey();
      if (checkFields && !fields.contains(name)) {
        continue;
      }
      JsonNode jsonValue = jsonField.getValue();
      if (jsonValue != null && !jsonValue.isNull()) {
        result.put(name, new StringByteIterator(jsonValue.asText()));
      }
    }
  }

  protected static String toJson(Map<String, ByteIterator> values)
      throws IOException {
    ObjectNode node = MAPPER.createObjectNode();
    Map<String, String> stringMap = StringByteIterator.getStringMap(values);
    for (Map.Entry<String, String> pair : stringMap.entrySet()) {
      node.put(pair.getKey(), pair.getValue());
    }
    JsonFactory jsonFactory = new JsonFactory();
    Writer writer = new StringWriter();
    JsonGenerator jsonGenerator = jsonFactory.createJsonGenerator(writer);
    MAPPER.writeTree(jsonGenerator, node);
    return writer.toString();
  }
}
