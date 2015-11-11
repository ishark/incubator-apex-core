package com.datatorrent.bufferserver.server;

import java.util.concurrent.BlockingQueue;

import com.datatorrent.bufferserver.internal.DataList;

public class PublisherReceiverThread implements Runnable
{
  private volatile boolean shutdown;
  private volatile boolean suspended;
  
  DataList datalist;
  BlockingQueue<byte[]> messageQueue;
  
  public PublisherReceiverThread(BlockingQueue<byte[]> queue, DataList dl)
  {
    messageQueue = queue;
    datalist = dl;
  }
  
  @Override
  public void run() {
    try{
      while(!shutdown) {
         while(suspended) {
           Thread.sleep(100);
         }
         byte[] tuple = messageQueue.take();
         // put the tuple in DL
      }
    } catch(InterruptedException e) {
      
    } catch(Exception e) {
      
    }
  }
  
  public void shutdownThread()
  {
    shutdown = true;
  }
  
  public void suspendThread()
  {
    suspended = true;
  }
}
