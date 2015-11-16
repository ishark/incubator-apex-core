/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.bufferserver.server;

import java.nio.ByteBuffer;
import java.util.Queue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.bufferserver.internal.DataList;
import com.datatorrent.netlet.util.DTThrowable;
import com.datatorrent.netlet.util.VarInt;

public class PublisherReceiverThread implements Runnable
{
  private volatile boolean shutdown = false;
  private volatile boolean suspended = false;

  private static final int INT_ARRAY_SIZE = 4096 - 5;

  DataList datalist;
  Queue<byte[]> messageQueue;

  protected byte[] buffer;
  protected ByteBuffer byteBuffer;
  protected int writeOffset;

  //byte[] currentTuple;
  int tupleOffset;

  public PublisherReceiverThread(Queue<byte[]> queue, DataList dl, long windowId)
  {
    this.buffer = dl.getBuffer(windowId);
    this.writeOffset = dl.getPosition();
    messageQueue = queue;
    datalist = dl;
  }

  @Override
  public void run()
  {
    try {
      while (!shutdown) {
        while (suspended) {
          Thread.sleep(100);
        }
        byte[] tuple = messageQueue.poll();
        if (tuple != null) {
          // put the tuple in DL
          writeToDataList(tuple);
        } else {
          Thread.sleep(5);
        }
      }
      if (shutdown) {
        // Read till queue is empty
        while (!messageQueue.isEmpty()) {
          writeToDataList(messageQueue.poll());
        }
      }
    } catch (InterruptedException e) {
      logger.debug("Thread interrupted");
    } catch (Exception e) {
      DTThrowable.wrapIfChecked(e);
    }
  }

  private void writeToDataList(byte[] tuple)
  {
    byte[] intBuffer = new byte[INT_ARRAY_SIZE + 5];
    int intOffset = 0;
    int newOffset = VarInt.write(tuple.length, intBuffer, intOffset);

    writeBytesToDataList(intBuffer, intOffset, newOffset - intOffset);
    intOffset = newOffset;

    //logger.info("Writing to data list...");
    //currentTuple = tuple;
    tupleOffset = 0;
    // TODO Auto-generated method stub
    writeBytesToDataList(tuple, 0, tuple.length);

  }

  public void writeBytesToDataList(byte[] tuple, int offset, int length)
  {
    if (writeOffset + tuple.length <= this.buffer.length) {
      // Copy tuple bytes into datalist
      System.arraycopy(tuple, 0, this.buffer, writeOffset, length);
      writeOffset += length;
      datalist.flush(writeOffset);
    } else {
      // Data cannot be added completely in new buffer
      // Write partial data
      System.arraycopy(tuple, 0, this.buffer, writeOffset, this.buffer.length - writeOffset);
      this.tupleOffset = this.buffer.length - writeOffset;
      writeOffset = this.buffer.length;
      datalist.flush(writeOffset);

      switchToNewBufferOrSuspendRead(tuple, offset, length);
    }
  }

  private boolean switchToNewBufferOrSuspendRead(final byte[] tuple, int offset, int length)
  {
    if (switchToNewBuffer(tuple, offset, length)) {
      return true;
    }
    datalist.suspendRead(this);
    return false;
  }

  private boolean switchToNewBuffer(final byte[] tuple, int offset, int length)
  {
    if (datalist.isMemoryBlockAvailable()) {
      buffer = datalist.newBuffer();
      System.arraycopy(tuple, this.tupleOffset, this.buffer, 0, length - this.tupleOffset);
      writeOffset = length - this.tupleOffset;
      datalist.addBuffer(buffer);
      datalist.flush(writeOffset);
      return true;
    }
    return false;
  }

  public void shutdownThread()
  {
    shutdown = true;
  }

  public void suspendThread()
  {
    suspended = true;
  }

  public void resumeThread()
  {
    suspended = false;
  }

  private Logger logger = LoggerFactory.getLogger(PublisherReceiverThread.class);
}
