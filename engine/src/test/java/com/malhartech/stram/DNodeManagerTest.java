/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram;

import java.net.InetSocketAddress;
import java.util.Arrays;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.malhartech.dag.DefaultSerDe;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.StreamingContainerContext;
import com.malhartech.stram.conf.TopologyBuilder;
import com.malhartech.stram.conf.TopologyBuilder.NodeConf;
import com.malhartech.stram.conf.TopologyBuilder.StreamConf;

public class DNodeManagerTest {

  @Test
  public void testAssignContainer() {

    TopologyBuilder b = new TopologyBuilder(new Configuration());

    NodeConf node1 = b.getOrAddNode("node1");
    NodeConf node2 = b.getOrAddNode("node2");
    NodeConf node3 = b.getOrAddNode("node3");

    StreamConf input1 = b.getOrAddStream("input1");
    input1.addProperty(TopologyBuilder.STREAM_CLASSNAME, NumberGeneratorInputAdapter.class.getName());
    node1.addInput(input1);
    
    node1.addOutput(b.getOrAddStream("n1n2"));
    node2.addInput(b.getOrAddStream("n1n2"));

    node2.addOutput(b.getOrAddStream("n2n3"));
    node3.addInput(b.getOrAddStream("n2n3"));
    b.getOrAddStream("n2n3").addProperty(TopologyBuilder.STREAM_INLINE, "true");
    
    Assert.assertEquals("number nodes", 3, b.getAllNodes().values().size());
    Assert.assertEquals("number root nodes", 1, b.getRootNodes().size());

    for (NodeConf nodeConf : b.getAllNodes().values()) {
        // required to construct context
        nodeConf.setClassName(TopologyBuilderTest.EchoNode.class.getName());
    }

    DNodeManager dnm = new DNodeManager(b);
    Assert.assertEquals("number required containers", 2, dnm.getNumRequiredContainers());
    
    String container1Id = "container1";
    String container2Id = "container2";
    
    // node1 needs to be deployed first, regardless in which order they were given
    StreamingContainerContext c1 = dnm.assignContainer(container1Id, InetSocketAddress.createUnresolved(container1Id+"Host", 9001));
    Assert.assertEquals("number nodes assigned to container", 2, c1.getNodes().size());
    Assert.assertTrue(node1.getId() + " assigned to " + container1Id, containsNodeContext(c1, node1));
    NodePConf input1PNode = getNodeContext(c1, input1.getId());
    Assert.assertNotNull(input1.getId() + " assigned to " + container1Id, input1PNode);

    Assert.assertEquals("stream connections for container1", 2, c1.getStreams().size());

    StreamPConf c1n1n2 = getStreamContext(c1, "n1n2");
    Assert.assertNotNull("stream connection for container1", c1n1n2);
    Assert.assertEquals("stream connects to upstream host", container1Id + "Host", c1n1n2.getBufferServerHost());
    Assert.assertEquals("stream connects to upstream port", 9001, c1n1n2.getBufferServerPort());

    StreamPConf input1Phys = getStreamContext(c1, input1.getId());
    Assert.assertNotNull("stream connection " + input1.getId(), input1Phys);
    Assert.assertEquals(input1.getId() + " sourceId", input1PNode.getDnodeId(), input1Phys.getSourceNodeId());
    Assert.assertEquals(input1.getId() + " targetId", c1n1n2.getSourceNodeId(), input1Phys.getTargetNodeId());
    Assert.assertNotNull(input1.getId() + " properties", input1Phys.getProperties());
    Assert.assertEquals(input1.getId() + " classname", NumberGeneratorInputAdapter.class.getName(), input1Phys.getProperties().get(TopologyBuilder.STREAM_CLASSNAME));
    Assert.assertTrue(input1Phys.isInline());
    
    StreamingContainerContext c2 = dnm.assignContainer(container2Id, InetSocketAddress.createUnresolved(container2Id+"Host", 9002));
    Assert.assertEquals("number nodes assigned to container", 2, c2.getNodes().size());
    Assert.assertTrue(node2.getId() + " assigned to " + container2Id, containsNodeContext(c2, node2));
    Assert.assertTrue(node3.getId() + " assigned to " + container2Id, containsNodeContext(c2, node3));
    
    Assert.assertEquals("one stream connection for container2", 2, c2.getStreams().size());
    StreamPConf c2n1n2 = getStreamContext(c2, "n1n2");
    Assert.assertNotNull("stream connection for container2", c2n1n2);
    Assert.assertEquals("stream connects to upstream host", container1Id + "Host", c2n1n2.getBufferServerHost());
    Assert.assertEquals("stream connects to upstream port", 9001, c2n1n2.getBufferServerPort());
    
  }

  
  @Test
  public void testStaticPartitioning() {
    TopologyBuilder b = new TopologyBuilder(new Configuration());
    
    NodeConf node1 = b.getOrAddNode("node1");
    NodeConf node2 = b.getOrAddNode("node2");

    NodeConf mergeNode = b.getOrAddNode("mergeNode");
    
    StreamConf n1n2 = b.getOrAddStream("n1n2");
    n1n2.addProperty(TopologyBuilder.STREAM_SERDE_CLASSNAME, TestStaticPartitioningSerDe.class.getName());
    
    node1.addOutput(n1n2);
    node2.addInput(n1n2);

    StreamConf mergeStream = b.getOrAddStream("mergeStream");
    node2.addOutput(mergeStream);
    mergeNode.addInput(mergeStream);
    
    for (NodeConf nodeConf : b.getAllNodes().values()) {
      nodeConf.setClassName(TopologyBuilderTest.EchoNode.class.getName());
    }
    DNodeManager dnm = new DNodeManager(b);
    Assert.assertEquals("number required containers", 5, dnm.getNumRequiredContainers());

    String container1Id = "container1";
    StreamingContainerContext c1 = dnm.assignContainer(container1Id, InetSocketAddress.createUnresolved(container1Id+"Host", 9001));
    Assert.assertEquals("number nodes assigned to container", 1, c1.getNodes().size());
    Assert.assertTrue(node2.getId() + " assigned to " + container1Id, containsNodeContext(c1, node1));

    for (int i=0; i<TestStaticPartitioningSerDe.partitions.length; i++) {
      String containerId = "container"+(i+1);
      StreamingContainerContext cc = dnm.assignContainer(containerId, InetSocketAddress.createUnresolved(containerId+"Host", 9001));
      Assert.assertEquals("number nodes assigned to container", 1, cc.getNodes().size());
      Assert.assertTrue(node2.getId() + " assigned to " + containerId, containsNodeContext(cc, node2));
  
      // n1n2 in, mergeStream out
      Assert.assertEquals("stream connections for " + containerId, 2, cc.getStreams().size());
      StreamPConf sc = getStreamContext(cc, "n1n2");
      Assert.assertNotNull("stream connection for " + containerId, sc);
      Assert.assertTrue("partition for " + containerId, Arrays.equals(TestStaticPartitioningSerDe.partitions[i], sc.getPartitionKeys().get(0)));
    }
    
    // mergeNode container 
    String mergeContainerId = "mergeNodeContainer";
    StreamingContainerContext cmerge = dnm.assignContainer(mergeContainerId, InetSocketAddress.createUnresolved(mergeContainerId+"Host", 9001));
    Assert.assertEquals("number nodes assigned to " + mergeContainerId, 1, cmerge.getNodes().size());
    Assert.assertTrue(mergeNode.getId() + " assigned to " + container1Id, containsNodeContext(cmerge, mergeNode));
    Assert.assertEquals("stream connections for " + mergeContainerId, 3, cmerge.getStreams().size());
    
  }  
  
  @Test
  public void testAdaptersWithStaticPartitioning() {
    TopologyBuilder b = new TopologyBuilder(new Configuration());

    NodeConf node1 = b.getOrAddNode("node1");
    
    StreamConf input1 = b.getOrAddStream("input1");
    input1.addProperty(TopologyBuilder.STREAM_CLASSNAME, NumberGeneratorInputAdapter.class.getName());
    input1.addProperty(TopologyBuilder.STREAM_SERDE_CLASSNAME, TestStaticPartitioningSerDe.class.getName());

    StreamConf output1 = b.getOrAddStream("output1");
    output1.addProperty(TopologyBuilder.STREAM_CLASSNAME, NumberGeneratorInputAdapter.class.getName());
    
    node1.addInput(input1);
    node1.addOutput(output1);
    
    for (NodeConf nodeConf : b.getAllNodes().values()) {
      nodeConf.setClassName(TopologyBuilderTest.EchoNode.class.getName());
    }

    DNodeManager dnm = new DNodeManager(b);
    int expectedContainerCount = TestStaticPartitioningSerDe.partitions.length;
    Assert.assertEquals("number required containers", expectedContainerCount, dnm.getNumRequiredContainers());
    
    for (int i=0; i<expectedContainerCount; i++) {
      String containerId = "container"+(i+1);
      StreamingContainerContext cc = dnm.assignContainer(containerId, InetSocketAddress.createUnresolved(containerId+"Host", 9001));

      NodePConf node1PNode = getNodeContext(cc, node1.getId());
      Assert.assertNotNull(node1.getId() + " assigned to " + containerId, node1PNode);
      
      if (i==0) {
        // the input and output adapter should be assigned to first container (deployed with first partition)
        Assert.assertEquals("number nodes assigned to " + containerId, 3, cc.getNodes().size());
        // output subscribes to all upstream partitions
        Assert.assertEquals("number streams " + containerId, 1+TestStaticPartitioningSerDe.partitions.length, cc.getStreams().size());
        
        NodePConf input1PNode = getNodeContext(cc, input1.getId());
        Assert.assertNotNull(input1.getId() + " assigned to " + containerId, input1PNode);
        
        NodePConf output1PNode = getNodeContext(cc, output1.getId());
        Assert.assertNotNull(output1.getId() + " assigned to " + containerId, output1PNode);
        
        int inlineStreamCount = 0;
        for (StreamPConf pstream : cc.getStreams()) {
          if (pstream.getTargetNodeId() == output1PNode.getDnodeId() && pstream.getSourceNodeId() == node1PNode.getDnodeId()) {
            Assert.assertTrue("stream from " + node1PNode + " to " + output1PNode + " inline", pstream.isInline());
            inlineStreamCount++;
          }
        }
        Assert.assertEquals("number inline streams", 1, inlineStreamCount);
        
      } else {
        Assert.assertEquals("number nodes assigned to " + containerId, 1, cc.getNodes().size());
        Assert.assertEquals("number streams " + containerId, 2, cc.getStreams().size());
      }
        
      StreamPConf scIn1 = getStreamContext(cc, "input1");
      Assert.assertNotNull("in stream connection for " + containerId, scIn1);
      Assert.assertTrue("partition for " + containerId, Arrays.equals(TestStaticPartitioningSerDe.partitions[i], scIn1.getPartitionKeys().get(0)));
      Assert.assertFalse(scIn1.isInline());
      
      StreamPConf scOut1 = getStreamContext(cc, "output1");
      Assert.assertNotNull("out stream connection for " + containerId, scOut1);
      Assert.assertFalse(scOut1.isInline());
    }
  }  

   
  public static class TestStaticPartitioningSerDe extends DefaultSerDe {

    public final static byte[][] partitions = new byte[][]{
        {'1'}, {'2'}, {'3'}
    };
    
    @Override
    public byte[][] getPartitions() {
      return partitions;
    }
  }
  
  
  private boolean containsNodeContext(StreamingContainerContext scc, NodeConf nodeConf) {
    return getNodeContext(scc, nodeConf.getId()) != null;
  }

  private static NodePConf getNodeContext(StreamingContainerContext scc, String logicalName) {
    for (NodePConf snc : scc.getNodes()) {
      if (logicalName.equals(snc.getLogicalId())) {
        return snc;
      }
    }
    return null;
  }
  
  private static StreamPConf getStreamContext(StreamingContainerContext scc, String streamId) {
    for (StreamPConf sc : scc.getStreams()) {
      if (streamId.equals(sc.getId())) {
        return sc;
      }
    }
    return null;
  }
  
}
