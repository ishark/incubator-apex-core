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
import com.datatorrent.bufferserver.util.VarInt;
import com.datatorrent.netlet.util.DTThrowable;

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
  byte[] currentTuple;
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
          logger.info("Thread suspended...");
          Thread.sleep(100);
        }

        byte[] tuple;
        if ((tuple = messageQueue.poll()) != null) {
          // put the tuple in DL
          writeToDataList(tuple);
        } else {        
//          logger.info("Queue is empty.. sleeping");
          Thread.sleep(5);
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
    if (writeOffset + tuple.length + 5 <= this.buffer.length) {
      writeOffset = VarInt.write(tuple.length, this.buffer, writeOffset);
      writeBytesToDataList(tuple, 0, tuple.length);
    } else {
      // Write partial data
      if (writeOffset + 5 <= this.buffer.length) {
        writeOffset = VarInt.write(tuple.length, this.buffer, writeOffset);

        if (writeOffset < this.buffer.length) {
          writeBytesToDataList(tuple, 0, this.buffer.length - writeOffset);
        }
      }
      if (switchToNewBufferOrSuspendRead(tuple, 0, tuple.length)) {
        currentTuple = null;
      } else {
        currentTuple = tuple;
      }
    }
  }

  public void writeBytesToDataList(byte[] tuple, int offset, int length)
  {
    System.arraycopy(tuple, offset, this.buffer, writeOffset, length);
    writeOffset += length;
    datalist.flush(writeOffset);
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
      logger.info("Switching to new buffer.. current write offset = {}, tuple length= {}, buffer length = {}", writeOffset, length, buffer.length);
      buffer = datalist.newBuffer();
      writeOffset = 0;
      writeOffset = VarInt.write(tuple.length, this.buffer, writeOffset);
      System.arraycopy(tuple, 0, this.buffer, writeOffset, length);
      writeOffset += length;
      datalist.addBuffer(buffer);
      datalist.flush(writeOffset);
      logger.info("Switched to new buffer.. {} {} {}", writeOffset, length, buffer.length);
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
