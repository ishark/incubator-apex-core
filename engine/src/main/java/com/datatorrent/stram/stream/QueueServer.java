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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.bufferserver.internal.DataList;
import com.datatorrent.bufferserver.packet.PublishRequestTuple;
import com.datatorrent.bufferserver.packet.ResetRequestTuple;
import com.datatorrent.bufferserver.server.PublisherReceiverThread;
import com.datatorrent.bufferserver.server.Server;
import com.datatorrent.netlet.AbstractLengthPrependerClient;

public class QueueServer extends Server
{
  private final ConcurrentHashMap<String, PublisherReceiverThread> publisherThreads = new ConcurrentHashMap<String, PublisherReceiverThread>();
  private final ConcurrentHashMap<String, Thread> receiverThreads = new ConcurrentHashMap<String, Thread>();

  public QueueServer(int port)
  {
    super(port);
  }

  public QueueServer(int port, int blocksize, int numberOfCacheBlocks)
  {
    super(port, blocksize, numberOfCacheBlocks);
  }

  public synchronized void  disconnectPublisher(String sourceId) throws IOException
  {
    PublisherReceiverThread thread = publisherThreads.get(sourceId);
    Thread t = receiverThreads.get(sourceId);
    if (thread != null) {
      thread.shutdownThread();
    }
    if (t != null) {
      t.interrupt();
    }
  }

  @Override
  protected void handleResetRequest(ResetRequestTuple request, final AbstractLengthPrependerClient ctx) throws IOException
  {
    super.handleResetRequest(request, ctx);
    disconnectPublisher(request.getIdentifier());
  }

  public DataList handlePublisherRequest(PublishRequestTuple request, BlockingQueue<byte[]> messageQueue)
  {
    logger.info("Handling publisher request... ");
    DataList dl = super.handlePublisherRequest(request, null);
    // Start new consumer thread and stop previous thread if exists
    String identifier = request.getIdentifier();
    PublisherReceiverThread thread = new PublisherReceiverThread(messageQueue, dl, getWindowId(request));
    if (identifier == null) {
      logger.debug("identifier is null");
    }
    PublisherReceiverThread previous = publisherThreads.put(identifier, thread);
    if (previous != null) {
      previous.shutdownThread();
    }

    Thread t = new Thread(thread);
    receiverThreads.put(identifier, t);
    t.start();
    dl.setAutoFlushExecutor(serverHelperExecutor);
    return dl;
  }

  // @Override
  // protected UnidentifiedClient getNewClient()
  // {
  // return new UnidentifiedClientForQueue();
  // }
  //
  // class UnidentifiedClientForQueue extends UnidentifiedClient
  // {
  // @Override
  // public void onMessage(byte[] buffer, int offset, int size)
  // {
  // Tuple request = Tuple.getTuple(buffer, offset, size);
  // if (!ignore) {
  // if (request.getType() == MessageType.PUBLISHER_REQUEST) {
  // unregistered(key);
  // logger.info("Received publisher request: {}", request);
  // // PublishRequestTuple publisherRequest = (PublishRequestTuple)
  // // request;
  //
  // // DataList dl = handlePublisherRequest(publisherRequest, this,
  // // getWindowId(request));
  // // dl.setAutoFlushExecutor(serverHelperExecutor);
  // } else if (request.getType() == MessageType.RESET_REQUEST) {
  // // Shutdown queue reading thread
  //
  // } else {
  // super.onMessage(buffer, offset, size);
  // }
  // }
  // }
  // }

  private static final Logger logger = LoggerFactory.getLogger(Server.class);
}
