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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.bufferserver.packet.PublishRequestTuple;
import com.datatorrent.bufferserver.packet.Tuple;
import com.datatorrent.stram.engine.StreamContext;

public class BufferServerQueuePublisher extends BufferServerPublisher
{
  int queueCapacity;
  String sourceId;

  public final BlockingQueue<byte[]> messageQueue;
  QueueServer server;

  public BufferServerQueuePublisher(String sourceId, int queueCapacity, QueueServer server)
  {
    super(sourceId, queueCapacity);
    logger.debug("Instantiating BufferServerQueuePublisher");
    this.sourceId = sourceId;
    this.queueCapacity = queueCapacity;
    this.messageQueue = new ArrayBlockingQueue<byte[]>(queueCapacity);
    // new SpscArrayQueue<byte[]>(queueCapacity);
    this.server = server;
  }

  @Override
  public boolean write(byte[] array)
  {
    // logger.debug("Writing data..");
    try {
      messageQueue.put(array);
    } catch (InterruptedException e) {
      // DTThrowable.wrapIfChecked(e);
      logger.debug("Exiting publisher");
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
    PublishRequestTuple request = (PublishRequestTuple) Tuple.getTuple(requestBytes, 0, requestBytes.length);
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
