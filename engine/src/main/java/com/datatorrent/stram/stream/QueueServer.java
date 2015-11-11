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


import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.bufferserver.internal.DataList;
import com.datatorrent.bufferserver.packet.MessageType;
import com.datatorrent.bufferserver.packet.PublishRequestTuple;
import com.datatorrent.bufferserver.packet.Tuple;
import com.datatorrent.bufferserver.server.PublisherReceiverThread;
import com.datatorrent.bufferserver.server.Server;
import com.datatorrent.bufferserver.server.Server.UnidentifiedClient;
import com.datatorrent.netlet.AbstractClient;
import com.datatorrent.netlet.AbstractLengthPrependerClient;

public class QueueServer extends Server
{
  private final ConcurrentHashMap<String, PublisherReceiverThread> publisherThreads = new ConcurrentHashMap<String, PublisherReceiverThread>();
  
  public QueueServer(int port)
  {
    super(port);
  }

  public QueueServer(int port, int blocksize, int numberOfCacheBlocks)
  {
    super(port, blocksize, numberOfCacheBlocks);
  }
  
  @Override
  public DataList handlePublisherRequest(PublishRequestTuple request, AbstractLengthPrependerClient connection)
  {
    DataList dl = super.handlePublisherRequest(request, connection);
    // Start new consumer thread and stop previous thread if exists
    String identifier = request.getIdentifier();
    BufferServerQueuePublisher publisher = (BufferServerQueuePublisher)connection;
    PublisherReceiverThread previous = publisherThreads.put(identifier, new PublisherReceiverThread(publisher.messageQueue, dl));
      if (previous != null) {
        previous.shutdownThread();
      }

    return dl;
  }
  
  class UnidentifiedClientForQueue extends UnidentifiedClient
  {
    
    @Override
    public void onMessage(byte[] buffer, int offset, int size)
    {
      Tuple request = Tuple.getTuple(buffer, offset, size);
      if (!ignore && request.getType() == MessageType.PUBLISHER_REQUEST) {
        unregistered(key);
        logger.info("Received publisher request: {}", request);
        PublishRequestTuple publisherRequest = (PublishRequestTuple) request;

        DataList dl = handlePublisherRequest(publisherRequest, this);
        dl.setAutoFlushExecutor(serverHelperExecutor);
      } else {
        super.onMessage(buffer, offset, size);
      }
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(Server.class);
}