package site.ycsb.db;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import net.spy.memcached.KeyUtil;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationErrorType;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.StatusCode;
import net.spy.memcached.protocol.BaseOperationImpl;
import net.spy.memcached.protocol.ascii.*;

abstract class TestOperationImpl extends BaseOperationImpl implements Operation {

    protected static final byte[] CRLF = { '\r', '\n' };
  private static final String CHARSET = "UTF-8";

  enum OperationReadType {
    /**
     * Read type indicating an operation currently wants to read lines.
     */
    LINE,
    /**
     * Read type indicating an operation currently wants to read raw data.
     */
    DATA
  }

  private final ByteArrayOutputStream byteBuffer = new ByteArrayOutputStream();
  private OperationReadType readType = OperationReadType.LINE;
  private boolean foundCr;
  private byte[] errorMsg;

  protected TestOperationImpl() {
  }

  protected TestOperationImpl(OperationCallback cb) {
    callback = cb;
  }

  /**
   * Match the status line provided against one of the given OperationStatus
   * objects. If none match, return a failure status with the given line.
   *
   * @param line the current line
   * @param statii several status objects
   * @return the appropriate status object
   */
  protected final OperationStatus matchStatus(String line,
      OperationStatus... statii) {
    OperationStatus rv = null;
    for (OperationStatus status : statii) {
      if (line.equals(status.getMessage())) {
        rv = status;
      }
    }
    if (rv == null) {
      rv = new OperationStatus(false, line, StatusCode.fromAsciiLine(line));
    }
    return rv;
  }

  /*
   * (non-Javadoc)
   *
   * @see net.spy.memcached.protocol.ascii.Operation#getReadType()
   */
  protected final OperationReadType getReadType() {
    return readType;
  }

  /**
   * Set the read type of this operation.
   */
  protected final void setReadType(OperationReadType to) {
    readType = to;
  }

  /**
   * Set some arguments for an operation into the given byte buffer.
   */
  protected final void setArguments(ByteBuffer bb, Object... args) {
    boolean wasFirst = true;
    for (Object o : args) {
      if (wasFirst) {
        wasFirst = false;
      } else {
        bb.put((byte) ' ');
      }
      bb.put(KeyUtil.getKeyBytes(String.valueOf(o)));
    }
    bb.put(CRLF);
  }

  OperationErrorType classifyError(String line) {
    OperationErrorType rv = null;
    if (line.startsWith("ERROR")) {
      rv = OperationErrorType.GENERAL;
    } else if (line.startsWith("CLIENT_ERROR")) {
      rv = OperationErrorType.CLIENT;
    } else if (line.startsWith("SERVER_ERROR")) {
      rv = OperationErrorType.SERVER;
    }
    return rv;
  }

  @Override
  public void readFromBuffer(ByteBuffer data) throws IOException {
    System.out.println("readFromBuffer");
    // Loop while there's data remaining to get it all drained.
    while (getState() != OperationState.COMPLETE && data.remaining() > 0) {
      if (readType == OperationReadType.DATA) {
        handleRead(data);
      } else {
        int offset = -1;
        for (int i = 0; data.remaining() > 0; i++) {
          byte b = data.get();
          if (b == '\r') {
            foundCr = true;
          } else if (b == '\n') {
            assert foundCr : "got a \\n without a \\r";
            offset = i;
            foundCr = false;
            break;
          } else {
            assert !foundCr : "got a \\r without a \\n";
            byteBuffer.write(b);
          }
        }
        if (offset >= 0) {
          String line = new String(byteBuffer.toByteArray(), CHARSET);
          byteBuffer.reset();
          OperationErrorType eType = classifyError(line);
          if (eType != null) {
            errorMsg = line.getBytes();
            handleError(eType, line);
          } else {
            handleLine(line);
          }
        }
      }
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * net.spy.memcached.protocol.ascii.Operation#handleLine(java.lang.String)
   */
  public abstract void handleLine(String line);

  @Override
  public byte[] getErrorMsg() {
    return errorMsg;
  }
    
}