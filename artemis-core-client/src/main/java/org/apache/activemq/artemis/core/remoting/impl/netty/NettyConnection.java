/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.core.remoting.impl.netty;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.GenericFutureListener;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQInterruptedException;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.buffers.impl.ChannelBufferWrapper;
import org.apache.activemq.artemis.core.client.ActiveMQClientLogger;
import org.apache.activemq.artemis.core.security.ActiveMQPrincipal;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.spi.core.remoting.BaseConnectionLifeCycleListener;
import org.apache.activemq.artemis.spi.core.remoting.Connection;
import org.apache.activemq.artemis.spi.core.remoting.ReadyListener;
import org.apache.activemq.artemis.utils.IPV6Util;

public class NettyConnection implements Connection {

   // Attributes ----------------------------------------------------

   private static final int DEFAULT_BATCH_BYTES = 1460;
   protected final Channel channel;
   private final BaseConnectionLifeCycleListener listener;
   private final boolean directDeliver;
   private final Map<String, Object> configuration;
   /**
    * if {@link #isWritable(ReadyListener)} returns false, we add a callback
    * here for when the connection (or Netty Channel) becomes available again.
    */
   private final List<ReadyListener> readyListeners = new ArrayList<>();
   private final ThreadLocal<ArrayList<ReadyListener>> localListenersPool = ThreadLocal.withInitial(ArrayList::new);
   private final boolean batchingEnabled;
   private final int writeBufferHighWaterMark;
   private final int writeBatchSize;

   // Static --------------------------------------------------------
   private boolean closed;
   private RemotingConnection protocolConnection;
   // Constructors --------------------------------------------------
   private boolean ready = true;

   // Public --------------------------------------------------------

   public NettyConnection(final Map<String, Object> configuration,
                          final Channel channel,
                          final BaseConnectionLifeCycleListener listener,
                          boolean batchingEnabled,
                          boolean directDeliver) {
      this.configuration = configuration;

      this.channel = channel;

      this.listener = listener;

      this.directDeliver = directDeliver;

      this.batchingEnabled = batchingEnabled;

      this.writeBufferHighWaterMark = this.channel.config().getWriteBufferHighWaterMark();

      this.writeBatchSize = batchingEnabled ? Math.min(this.writeBufferHighWaterMark, DEFAULT_BATCH_BYTES) : 0;
   }

   private static void waitFor(ChannelPromise promise, long millis) {
      try {
         final boolean completed = promise.await(millis);
         if (!completed) {
            ActiveMQClientLogger.LOGGER.timeoutFlushingPacket();
         }
      } catch (InterruptedException e) {
         throw new ActiveMQInterruptedException(e);
      }
   }
   // Connection implementation ----------------------------

   public final int writeBatchSize() {
      return this.writeBatchSize;
   }

   public Channel getNettyChannel() {
      return channel;
   }

   @Override
   public void setAutoRead(boolean autoRead) {
      channel.config().setAutoRead(autoRead);
   }

   @Override
   public boolean isWritable(ReadyListener callback) {
      synchronized (readyListeners) {
         if (!ready) {
            readyListeners.add(callback);
         }

         return ready;
      }
   }

   @Override
   public void fireReady(final boolean ready) {
      final ArrayList<ReadyListener> readyToCall = localListenersPool.get();
      synchronized (readyListeners) {
         this.ready = ready;

         if (ready) {
            final int size = this.readyListeners.size();
            readyToCall.ensureCapacity(size);
            try {
               for (int i = 0; i < size; i++) {
                  final ReadyListener readyListener = readyListeners.get(i);
                  if (readyListener == null) {
                     break;
                  }
                  readyToCall.add(readyListener);
               }
            } finally {
               readyListeners.clear();
            }
         }
      }
      try {
         final int size = readyToCall.size();
         for (int i = 0; i < size; i++) {
            try {
               final ReadyListener readyListener = readyToCall.get(i);
               readyListener.readyForWriting();
            } catch (Throwable logOnly) {
               ActiveMQClientLogger.LOGGER.warn(logOnly.getMessage(), logOnly);
            }
         }
      } finally {
         readyToCall.clear();
      }
   }

   @Override
   public void forceClose() {
      if (channel != null) {
         try {
            channel.close();
         } catch (Throwable e) {
            ActiveMQClientLogger.LOGGER.warn(e.getMessage(), e);
         }
      }
   }

   /**
    * This is exposed so users would have the option to look at any data through interceptors
    *
    * @return
    */
   public Channel getChannel() {
      return channel;
   }

   @Override
   public RemotingConnection getProtocolConnection() {
      return protocolConnection;
   }

   @Override
   public void setProtocolConnection(RemotingConnection protocolConnection) {
      this.protocolConnection = protocolConnection;
   }

   @Override
   public void close() {
      if (closed) {
         return;
      }
      //to be sure that there won't be any pending writes
      if (this.batchingEnabled) {
         this.channel.flush();
      }
      EventLoop eventLoop = channel.eventLoop();
      boolean inEventLoop = eventLoop.inEventLoop();
      //if we are in an event loop we need to close the channel after the writes have finished
      if (!inEventLoop) {
         final SslHandler sslHandler = (SslHandler) channel.pipeline().get("ssl");
         closeSSLAndChannel(sslHandler, channel, false);
      } else {
         eventLoop.execute(() -> {
            final SslHandler sslHandler = (SslHandler) channel.pipeline().get("ssl");
            closeSSLAndChannel(sslHandler, channel, true);
         });
      }

      closed = true;

      listener.connectionDestroyed(getID());
   }

   @Override
   public ActiveMQBuffer createTransportBuffer(final int size) {
      return createTransportBuffer(size, false);
   }

   @Override
   public ActiveMQBuffer createTransportBuffer(final int size, boolean pooled) {
      return new ChannelBufferWrapper(PartialPooledByteBufAllocator.INSTANCE.directBuffer(size), true);
   }

   @Override
   public Object getID() {
      // TODO: Think of it
      return channel.hashCode();
   }

   // This is called periodically to flush the batch buffer
   @Override
   public void checkFlushBatchBuffer() {
      if (this.batchingEnabled) {
         this.channel.flush();
      }
   }

   @Override
   public void write(final ActiveMQBuffer buffer) {
      write(buffer, false, false);
   }

   @Override
   public void write(ActiveMQBuffer buffer, final boolean flush, final boolean batched) {
      write(buffer, flush, batched, null);
   }

   @Override
   public void write(ActiveMQBuffer buffer,
                     final boolean flush,
                     final boolean batched,
                     final ChannelFutureListener futureListener) {
      //no need to lock because the Netty's channel is thread-safe
      //and the order of write is ensured by the order of the write calls
      final boolean inEventLoop = channel.eventLoop().inEventLoop();
      final ChannelPromise promise;
      if ((!inEventLoop && flush) || (futureListener != null)) {
         promise = channel.newPromise();
      } else {
         promise = channel.voidPromise();
      }
      final ChannelFuture future;
      final ByteBuf bytes = buffer.byteBuf();
      final int readableBytes = bytes.readableBytes();
      final int writeBatchSize = this.writeBatchSize;
      if (this.batchingEnabled && batched && !flush && readableBytes < writeBatchSize) {
         future = writeBatch(bytes, readableBytes, promise);
      } else {
         future = channel.writeAndFlush(bytes, promise);
      }
      if (futureListener != null) {
         future.addListener(futureListener);
      }
      if (!inEventLoop && flush) {
         //it is for testing/debugging purposes?
         waitFor(promise, 10_000);
      }
   }

   private ChannelFuture writeBatch(final ByteBuf bytes, final int readableBytes, final ChannelPromise promise) {
      final int bytesBeforeUnwritable = (int) channel.bytesBeforeUnwritable();
      assert bytesBeforeUnwritable >= 0;
      final int writtenBytes = this.writeBufferHighWaterMark - bytesBeforeUnwritable;
      final int nextBatchSize = writtenBytes + readableBytes;
      if (nextBatchSize > writeBatchSize) {
         //flush before writing to create the chance to make the channel writable again
         channel.flush();
         //let netty use its write batching ability
         return channel.write(bytes, promise);
      } else if (nextBatchSize == writeBatchSize) {
         return channel.writeAndFlush(bytes, promise);
      } else {
         //let netty use its write batching ability
         return channel.write(bytes, promise);
      }
   }

   @Override
   public String getRemoteAddress() {
      SocketAddress address = channel.remoteAddress();
      if (address == null) {
         return null;
      }
      return address.toString();
   }

   @Override
   public String getLocalAddress() {
      SocketAddress address = channel.localAddress();
      if (address == null) {
         return null;
      }
      return "tcp://" + IPV6Util.encloseHost(address.toString());
   }

   public boolean isDirectDeliver() {
      return directDeliver;
   }

   //never allow this
   @Override
   public ActiveMQPrincipal getDefaultActiveMQPrincipal() {
      return null;
   }

   @Override
   public TransportConfiguration getConnectorConfig() {
      if (configuration != null) {
         return new TransportConfiguration(NettyConnectorFactory.class.getName(), this.configuration);
      } else {
         return null;
      }
   }

   @Override
   public boolean isUsingProtocolHandling() {
      return true;
   }

   // Public --------------------------------------------------------

   @Override
   public String toString() {
      return super.toString() + "[local= " + channel.localAddress() + ", remote=" + channel.remoteAddress() + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void closeSSLAndChannel(SslHandler sslHandler, final Channel channel, boolean inEventLoop) {
      if (sslHandler != null) {
         try {
            ChannelFuture sslCloseFuture = sslHandler.close();
            sslCloseFuture.addListener(new GenericFutureListener<ChannelFuture>() {
               @Override
               public void operationComplete(ChannelFuture future) throws Exception {
                  channel.close();
               }
            });
            if (!inEventLoop && !sslCloseFuture.awaitUninterruptibly(10000)) {
               ActiveMQClientLogger.LOGGER.timeoutClosingSSL();
            }
         } catch (Throwable t) {
            // ignore
         }
      } else {
         ChannelFuture closeFuture = channel.close();
         if (!inEventLoop && !closeFuture.awaitUninterruptibly(10000)) {
            ActiveMQClientLogger.LOGGER.timeoutClosingNettyChannel();
         }
      }
   }
   // Inner classes -------------------------------------------------

}
