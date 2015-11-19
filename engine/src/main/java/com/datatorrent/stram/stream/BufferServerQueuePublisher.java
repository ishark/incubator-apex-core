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

package com.datatorrent.stram.stream;

import java.io.IOException;
import java.util.Queue;
//import java.util.concurrent.ArrayBlockingQueue;
//import java.util.concurrent.BlockingQueue;

import org.jctools.queues.SpscArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.bufferserver.packet.PublishRequestTuple;
import com.datatorrent.bufferserver.packet.Tuple;
//import com.datatorrent.netlet.util.VarInt;
import com.datatorrent.stram.engine.StreamContext;

import static java.lang.Thread.sleep;

public class BufferServerQueuePublisher extends BufferServerPublisher
{
  private static final int INT_ARRAY_SIZE = 4096;
  int queueCapacity;
  String sourceId;

  public final Queue<byte[]> messageQueue;
  QueueServer server;

  public BufferServerQueuePublisher(String sourceId, int queueCapacity, QueueServer server)
  {
    super(sourceId, queueCapacity);
    logger.debug("Instantiating BufferServerQueuePublisher");
    this.sourceId = sourceId;
    queueCapacity = 64000;
    this.queueCapacity = queueCapacity;
    logger.info("Queue capacity = {}", queueCapacity);

    this.messageQueue = new SpscArrayQueue<byte[]>(queueCapacity);
    // new ArrayBlockingQueue<byte[]>(queueCapacity);
    this.server = server;
  }

  @Override
  public boolean write(byte[] array)
  {
    try {
      // sleep(5);
//      byte[] intBuffer = new byte[INT_ARRAY_SIZE];
//      int intOffset = 0;
//      int newOffset = VarInt.write(array.length, intBuffer, intOffset);
//      byte[] prependLengthArray = new byte[newOffset + array.length];
//      System.arraycopy(intBuffer, 0, prependLengthArray, 0, newOffset);
//      System.arraycopy(array, 0, prependLengthArray, newOffset, array.length);
//      while (!messageQueue.offer(prependLengthArray)) {
      while (!messageQueue.offer(array)) {
        logger.info("Sleeping for 5 ms.. Queue is full queue size = {}", messageQueue.size());
        sleep(5);
      }
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      // e.printStackTrace();
      logger.debug("Thread interrupted in sleep");
    }
    return true;
  }

  /**
   *
   * @param context
   */
  @Override
  public void activate(StreamContext context)
  {
    logger.info("Activating publisher..");
    // super.activate(context);
    logger.debug("source = {}", sourceId);
    byte[] requestBytes = PublishRequestTuple.getSerializedRequest(null, sourceId, context.getFinishedWindowId());
    PublishRequestTuple request = (PublishRequestTuple)Tuple.getTuple(requestBytes, 0, requestBytes.length);
    logger.debug("request = {}", request);
    server.handlePublisherRequest(request, messageQueue);
  }

  @Override
  public void deactivate()
  {
    logger.info("Deactivating publisher..");
    try {
      server.disconnectPublisher(sourceId);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    // super.deactivate();

    // Remove connection from buffer server queue
  }

  private static final Logger logger = LoggerFactory.getLogger(BufferServerQueuePublisher.class);
}
