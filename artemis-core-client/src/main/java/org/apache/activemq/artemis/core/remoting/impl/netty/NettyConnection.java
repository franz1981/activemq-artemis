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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOutboundBuffer;
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
   private static final int DEFAULT_WAIT_MILLIS = 10_000;
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
   private final int batchLimit;
   private final Thread monitorThread;
   private final LongAdder writtenNotEventLoop;
   private final AtomicLong writtenEventLoopView;
   private final AtomicLong eventLoopWaitTimeView;
   private long eventLoopWaitTime;
   private long eventLoopSamples;

   // Static --------------------------------------------------------
   private boolean closed;
   private RemotingConnection protocolConnection;
   // Constructors --------------------------------------------------
   private boolean ready = true;
   private long writtenEventLoop;

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

      this.batchingEnabled = true;

      this.writeBufferHighWaterMark = this.channel.config().getWriteBufferHighWaterMark();

      this.batchLimit = batchingEnabled ? Math.min(this.writeBufferHighWaterMark, DEFAULT_BATCH_BYTES) : 0;

      this.eventLoopWaitTimeView = new AtomicLong(0);
      this.eventLoopWaitTime = 0;
      this.eventLoopSamples = 0;
      this.writtenEventLoopView = new AtomicLong();
      this.writtenNotEventLoop = new LongAdder();
      this.writtenEventLoop = 0;
      this.monitorThread = new Thread(() -> {
         final String id = channel.id().toString();
         final long nanoWait = TimeUnit.MILLISECONDS.toNanos(2000L);

         //read acquire the samples
         long eventLoopWaitTime = this.eventLoopWaitTimeView.get();
         long eventLoopSamples = this.eventLoopSamples;

         long writtenEventLoop = this.writtenEventLoopView.get();
         long writtenNotEventLoop = this.writtenNotEventLoop.longValue();
         long totalWritten = writtenEventLoop + writtenNotEventLoop;

         while (!Thread.currentThread().isInterrupted()) {
            LockSupport.parkNanos(nanoWait);
            final ChannelOutboundBuffer channelOutboundBuffer = channel.unsafe().outboundBuffer();
            if (channelOutboundBuffer != null) {
               eventLoopWaitTime = this.eventLoopWaitTimeView.get() - eventLoopWaitTime;
               eventLoopSamples = this.eventLoopSamples - eventLoopSamples;
               final long avgLoopWaitTime = eventLoopSamples > 0 ? eventLoopWaitTime / eventLoopSamples : -1;
               final long batchSizeUtilisation = channelOutboundBuffer.totalPendingWriteBytes();
               final long currentWrittenEventLoop = this.writtenEventLoopView.get();
               final long currentWrittenNotEventLoop = this.writtenNotEventLoop.longValue();

               final long currentTotalWritten = currentWrittenEventLoop + currentWrittenNotEventLoop;

               final long totalThroughtput = ((currentTotalWritten - totalWritten) * 1000_000L) / nanoWait;

               final long eventLoopThroughput = ((currentWrittenEventLoop - writtenEventLoop) * 1000_000L) / nanoWait;
               final long notEventLoopThroughput = ((currentWrittenNotEventLoop - writtenNotEventLoop) * 1000_000L) / nanoWait;

               writtenEventLoop = currentWrittenEventLoop;
               writtenNotEventLoop = currentWrittenNotEventLoop;
               totalWritten = currentTotalWritten;
               System.out.println('[' + id + "]\t->\t" + batchSizeUtilisation + "/" + writeBufferHighWaterMark + " bytes\t TOTAL: " + totalThroughtput + " KB/sec\t " + (totalThroughtput > 0 ? ("EVENT_LOOP: [" + (avgLoopWaitTime > 0 ? (avgLoopWaitTime / 1000f) : "N/A") + " us] " + eventLoopThroughput + " KB/sec\tASYNC:" + notEventLoopThroughput + " KB/sec\t ") : ""));
            }
         }
         System.out.println('[' + id + "]\tMONITORING CLOSED");
      });
      this.monitorThread.start();

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

   /**
    * Returns an estimation of the current size of the write buffer in the channel.
    * To obtain a more precise value is necessary to use the unsafe API of the channel to
    * call the {@link io.netty.channel.ChannelOutboundBuffer#totalPendingWriteBytes()}.
    * Anyway, both these values are subject to concurrent modifications.
    */
   private static int batchBufferSize(Channel channel, int writeBufferHighWaterMark) {
      //Channel::bytesBeforeUnwritable is performing a volatile load
      //this is the reason why writeBufferHighWaterMark is passed as an argument
      final int bytesBeforeUnwritable = (int) channel.bytesBeforeUnwritable();
      assert bytesBeforeUnwritable >= 0;
      final int writtenBytes = writeBufferHighWaterMark - bytesBeforeUnwritable;
      return writtenBytes;
   }

   public final int batchBufferCapacity() {
      return this.batchLimit;
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
      monitorThread.interrupt();
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
   public final void checkFlushBatchBuffer() {
      if (this.batchingEnabled) {
         //perform the flush only if necessary
         final int batchBufferSize = batchBufferSize(this.channel, this.writeBufferHighWaterMark);
         if (batchBufferSize > 0) {
            this.channel.flush();
         }
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
      final EventLoop eventLoop = channel.eventLoop();
      final boolean inEventLoop = eventLoop.inEventLoop();
      if (!inEventLoop) {
         this.writtenNotEventLoop.add(buffer.readableBytes());
         writeNotInEventLoop(buffer, flush, batched, futureListener);
      } else {
         final long startRequest = System.nanoTime();
         // OLD COMMENT:
         // create a task which will be picked up by the eventloop and trigger the write.
         // This is mainly needed as this method is triggered by different threads for the same channel.
         // if we not do this we may produce out of order writes.
         // NOTE:
         // the submitted task does not effect in any way the current written size in the batch
         // until the loop will process it, leading to a longer life for the ActiveMQBuffer buffer!!!
         // To solve it, will be necessary to manually perform the count of the current batch instead of rely on the
         // Channel:Config::writeBufferHighWaterMark value.
         eventLoop.execute(() -> {
            this.eventLoopWaitTime += (System.nanoTime() - startRequest);
            this.eventLoopSamples++;
            //write release the event loop samples values
            this.eventLoopWaitTimeView.lazySet(eventLoopWaitTime);
            //the real write happens here
            this.writtenEventLoop += buffer.readableBytes();
            this.writtenEventLoopView.lazySet(this.writtenEventLoop);
            writeInEventLoop(buffer, flush, batched, futureListener);
         });
      }
   }

   private void writeNotInEventLoop(ActiveMQBuffer buffer,
                                    final boolean flush,
                                    final boolean batched,
                                    final ChannelFutureListener futureListener) {
      final ChannelPromise promise;
      if (flush || (futureListener != null)) {
         promise = channel.newPromise();
      } else {
         promise = channel.voidPromise();
      }
      final ChannelFuture future;
      final ByteBuf bytes = buffer.byteBuf();
      final int readableBytes = bytes.readableBytes();
      final int writeBatchSize = this.batchLimit;
      if (this.batchingEnabled && batched && !flush && readableBytes < writeBatchSize) {
         future = writeBatch(bytes, readableBytes, promise);
      } else {
         future = channel.writeAndFlush(bytes, promise);
      }
      if (futureListener != null) {
         future.addListener(futureListener);
      }
      if (flush) {
         //NOTE: this code path seems used only on RemotingConnection::disconnect
         waitFor(promise, DEFAULT_WAIT_MILLIS);
      }
   }

   private void writeInEventLoop(ActiveMQBuffer buffer,
                                 final boolean flush,
                                 final boolean batched,
                                 final ChannelFutureListener futureListener) {
      //no need to lock because the Netty's channel is thread-safe
      //and the order of write is ensured by the order of the write calls
      final ChannelPromise promise;
      if (futureListener != null) {
         promise = channel.newPromise();
      } else {
         promise = channel.voidPromise();
      }
      final ChannelFuture future;
      final ByteBuf bytes = buffer.byteBuf();
      final int readableBytes = bytes.readableBytes();
      final int writeBatchSize = this.batchLimit;
      if (this.batchingEnabled && batched && !flush && readableBytes < writeBatchSize) {
         future = writeBatch(bytes, readableBytes, promise);
      } else {
         future = channel.writeAndFlush(bytes, promise);
      }
      if (futureListener != null) {
         future.addListener(futureListener);
      }
   }

   private ChannelFuture writeBatch(final ByteBuf bytes, final int readableBytes, final ChannelPromise promise) {
      final int batchBufferSize = batchBufferSize(channel, this.writeBufferHighWaterMark);
      final int nextBatchSize = batchBufferSize + readableBytes;
      if (nextBatchSize > batchLimit) {
         //flush before writing to create the chance to make the channel writable again
         channel.flush();
         //let netty use its write batching ability
         return channel.write(bytes, promise);
      } else if (nextBatchSize == batchLimit) {
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
      checkFlushBatchBuffer();
      if (sslHandler != null) {
         try {
            ChannelFuture sslCloseFuture = sslHandler.close();
            sslCloseFuture.addListener(new GenericFutureListener<ChannelFuture>() {
               @Override
               public void operationComplete(ChannelFuture future) throws Exception {
                  channel.close();
               }
            });
            if (!inEventLoop && !sslCloseFuture.awaitUninterruptibly(DEFAULT_WAIT_MILLIS)) {
               ActiveMQClientLogger.LOGGER.timeoutClosingSSL();
            }
         } catch (Throwable t) {
            // ignore
         }
      } else {
         ChannelFuture closeFuture = channel.close();
         if (!inEventLoop && !closeFuture.awaitUninterruptibly(DEFAULT_WAIT_MILLIS)) {
            ActiveMQClientLogger.LOGGER.timeoutClosingNettyChannel();
         }
      }
   }
   // Inner classes -------------------------------------------------

}
