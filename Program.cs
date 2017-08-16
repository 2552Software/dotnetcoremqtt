using System;
using System.Text;
using DotNetty.Buffers;
using DotNetty.Transport.Channels;
using System.IO;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using DotNetty.Transport.Bootstrapping;
using DotNetty.Transport.Channels.Sockets;
using DotNetty.Common.Internal.Logging;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Console;
using DotNetty.Handlers.Tls;
using DotNetty.Handlers.Logging;
using DotNetty.Codecs.Mqtt;
using DotNetty.Codecs.Mqtt.Packets;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using DotNetty.Common.Concurrency;

namespace DotNet2ConsoleApp1
{
    public sealed class ReadListeningHandler : ChannelHandlerAdapter
    {
        readonly Queue<object> receivedQueue = new Queue<object>();
        TaskCompletionSource<object> readPromise;
        Exception registeredException;
        readonly TimeSpan defaultReadTimeout;

        public ReadListeningHandler()
            : this(TimeSpan.Zero)
        {
        }

        public ReadListeningHandler(TimeSpan defaultReadTimeout)
        {
            this.defaultReadTimeout = defaultReadTimeout;
        }

        public override void ChannelRead(IChannelHandlerContext context, object message)
        {
            TaskCompletionSource<object> promise = this.readPromise;
            if (this.readPromise != null)
            {
                this.readPromise = null;
                promise.TrySetResult(message);
            }
            else
            {
                this.receivedQueue.Enqueue(message);
            }
        }

        public override void ChannelInactive(IChannelHandlerContext context)
        {
            this.SetException(new InvalidOperationException("Channel is closed."));
            base.ChannelInactive(context);
        }

        public override void ExceptionCaught(IChannelHandlerContext context, Exception exception) => this.SetException(exception);

        void SetException(Exception exception)
        {
            this.registeredException = exception;
            this.readPromise?.TrySetException(exception);
        }

        public async Task<object> ReceiveAsync(TimeSpan timeout = default(TimeSpan))
        {
            Contract.Assert(this.readPromise == null);

            if (this.registeredException != null)
            {
                throw this.registeredException;
            }

            if (this.receivedQueue.Count > 0)
            {
                return this.receivedQueue.Dequeue();
            }

            var promise = new TaskCompletionSource<object>();
            this.readPromise = promise;

            timeout = timeout <= TimeSpan.Zero ? this.defaultReadTimeout : timeout;
            if (timeout > TimeSpan.Zero)
            {
                Task task = await Task.WhenAny(promise.Task, Task.Delay(timeout));
                if (task != promise.Task)
                {
                    throw new TimeoutException("ReceiveAsync timed out");
                }

                return promise.Task.Result;
            }

            return await promise.Task;
        }
    }


    public static class ChannelExtensions
    {
        public static Task WriteAndFlushManyAsync(this IChannel channel, params object[] messages)
        {
            var list = new List<Task>();
            foreach (object m in messages)
            {
                list.Add(channel.WriteAsync(m));
            }
            IEnumerable<Task> tasks = list.ToArray();
            channel.Flush();
            return Task.WhenAll(tasks);
        }
    }

    public class EchoServerHandler : ChannelHandlerAdapter
    {
        public override void ChannelRead(IChannelHandlerContext context, object message)
        {
            var buffer = message as IByteBuffer;
            if (buffer != null)
            {
                Console.WriteLine("Received from client: " + buffer.ToString(Encoding.UTF8));
            }
            context.WriteAsync(message);
        }

        public override void ChannelReadComplete(IChannelHandlerContext context) => context.Flush();

        public override void ExceptionCaught(IChannelHandlerContext context, Exception exception)
        {
            Console.WriteLine("Exception: " + exception);
            context.CloseAsync();
        }
    }
    public static class ServerSettings
    {
        public static bool IsSsl
        {
            get
            {
                string ssl = ExampleHelper.Configuration["ssl"];
                return !string.IsNullOrEmpty(ssl) && bool.Parse(ssl);
            }
        }

        public static int Port => int.Parse(ExampleHelper.Configuration["port"]);
    }
    public static class ExampleHelper
    {
        static ExampleHelper()
        {

            Configuration = new ConfigurationBuilder()
                .SetBasePath(ProcessDirectory)
                .AddJsonFile("appsettings.json")
                .Build();
        }

        public static string ProcessDirectory
        {
            get
            {
#if NETSTANDARD1_3
                return AppContext.BaseDirectory;
#else
                return AppDomain.CurrentDomain.BaseDirectory;
#endif
            }
        }

        public static IConfigurationRoot Configuration { get; }

        public static void SetConsoleLogger() => InternalLoggerFactory.DefaultFactory.AddProvider(new ConsoleLoggerProvider((s, level) => true, false));
    }
    class ExceptionCatchHandler : ChannelHandlerAdapter
    {
        readonly Action<Exception> exceptionCaughtAction;

        public ExceptionCatchHandler(Action<Exception> exceptionCaughtAction)
        {
            Contract.Requires(exceptionCaughtAction != null);
            this.exceptionCaughtAction = exceptionCaughtAction;
        }

        public override void ExceptionCaught(IChannelHandlerContext context, Exception exception) => this.exceptionCaughtAction(exception);
    }
    class Program
    {
        static int GetRandomPacketId() => Guid.NewGuid().GetHashCode() & ushort.MaxValue;
        const int Port = 8009;

        const string ClientId = "scenarioClient1";
        const string SubscribeTopicFilter1 = "test/+";
        const string SubscribeTopicFilter2 = "test2/#";
        const string PublishC2STopic = "loopback/qosZero";
        const string PublishC2SQos0Payload = "C->S, QoS 0 test.";
        const string PublishC2SQos1Topic = "loopback2/qos/One";
        const string PublishC2SQos1Payload = "C->S, QoS 1 test. Different data length.";
        const string PublishS2CQos1Topic = "test2/scenarioClient1/special/qos/One";
        const string PublishS2CQos1Payload = "S->C, QoS 1 test. Different data length #2.";

        async Task RunMqttServerScenarioAsync(IChannel channel, ReadListeningHandler readListener)
        {
            var connectPacket = await readListener.ReceiveAsync();
            // todo verify

            await channel.WriteAndFlushAsync(new ConnAckPacket
            {
                ReturnCode = ConnectReturnCode.Accepted,
                SessionPresent = true
            });

            var subscribePacket = (SubscribePacket)await readListener.ReceiveAsync();
            // todo verify

            await channel.WriteAndFlushAsync(SubAckPacket.InResponseTo(subscribePacket, QualityOfService.ExactlyOnce));

            var unsubscribePacket = (UnsubscribePacket)await readListener.ReceiveAsync();
            // todo verify

            await channel.WriteAndFlushAsync(UnsubAckPacket.InResponseTo(unsubscribePacket));

            var publishQos0Packet = (PublishPacket)await readListener.ReceiveAsync();
            // todo verify

            var publishQos1Packet = (PublishPacket)await readListener.ReceiveAsync();
            // todo verify

            int publishQos1PacketId = GetRandomPacketId();
            await channel.WriteAndFlushManyAsync(
                PubAckPacket.InResponseTo(publishQos1Packet),
                new PublishPacket(QualityOfService.AtLeastOnce, false, false)
                {
                    PacketId = publishQos1PacketId,
                    TopicName = PublishS2CQos1Topic,
                    Payload = Unpooled.WrappedBuffer(Encoding.UTF8.GetBytes(PublishS2CQos1Payload))
                });

            var pubAckPacket = (PubAckPacket)await readListener.ReceiveAsync();

            var disconnectPacket = (DisconnectPacket)await readListener.ReceiveAsync();
        }

        /// <summary>
        ///     Starts Echo server.
        /// </summary>
        /// <returns>function to trigger closure of the server.</returns>
        async Task<Func<Task>> StartServerAsync(bool tcpNoDelay, Action<IChannel> childHandlerSetupAction, TaskCompletionSource testPromise)
        {
            var bossGroup = new MultithreadEventLoopGroup(1);
            var workerGroup = new MultithreadEventLoopGroup();
            bool started = false;
            try
            {
                ServerBootstrap b = new ServerBootstrap()
                    .Group(bossGroup, workerGroup)
                    .Channel<TcpServerSocketChannel>()
                    .Handler(new ExceptionCatchHandler(ex => testPromise.TrySetException(ex)))
                    .ChildHandler(new ActionChannelInitializer<ISocketChannel>(childHandlerSetupAction))
                    .ChildOption(ChannelOption.TcpNodelay, tcpNoDelay);

                //this.Output.WriteLine("Configured ServerBootstrap: {0}", b);

                IChannel serverChannel = await b.BindAsync(Port);

                //this.Output.WriteLine("Bound server channel: {0}", serverChannel);

                started = true;

                return async () =>
                {
                    try
                    {
                        await serverChannel.CloseAsync();
                    }
                    finally
                    {
                        await bossGroup.ShutdownGracefullyAsync();
                        await workerGroup.ShutdownGracefullyAsync();
                    }
                };
            }
            finally
            {
                if (!started)
                {
                    await bossGroup.ShutdownGracefullyAsync();
                    await workerGroup.ShutdownGracefullyAsync();
                }
            }
        }

        async Task RunMqttClientScenarioAsync(IChannel channel, ReadListeningHandler readListener)
        {
            await channel.WriteAndFlushAsync(new ConnectPacket
            {
                ClientId = ClientId,
                Username = "testuser",
                Password = "notsafe",
                WillTopicName = "last/word",
                WillMessage = Unpooled.WrappedBuffer(Encoding.UTF8.GetBytes("oops"))
            });


            int subscribePacketId = GetRandomPacketId();
            int unsubscribePacketId = GetRandomPacketId();
            await channel.WriteAndFlushManyAsync(
                new SubscribePacket(subscribePacketId,
                    new SubscriptionRequest(SubscribeTopicFilter1, QualityOfService.ExactlyOnce),
                    new SubscriptionRequest(SubscribeTopicFilter2, QualityOfService.AtLeastOnce),
                    new SubscriptionRequest("for/unsubscribe", QualityOfService.AtMostOnce)),
                new UnsubscribePacket(unsubscribePacketId, "for/unsubscribe"));

            int publishQoS1PacketId = GetRandomPacketId();
            await channel.WriteAndFlushManyAsync(
                new PublishPacket(QualityOfService.AtMostOnce, false, false)
                {
                    TopicName = PublishC2STopic,
                    Payload = Unpooled.WrappedBuffer(Encoding.UTF8.GetBytes(PublishC2SQos0Payload))
                },
                new PublishPacket(QualityOfService.AtLeastOnce, false, false)
                {
                    PacketId = publishQoS1PacketId,
                    TopicName = PublishC2SQos1Topic,
                    Payload = Unpooled.WrappedBuffer(Encoding.UTF8.GetBytes(PublishC2SQos1Payload))
                });
            //new PublishPacket(QualityOfService.AtLeastOnce, false, false) { TopicName = "feedback/qos/One", Payload = Unpooled.WrappedBuffer(Encoding.UTF8.GetBytes("QoS 1 test. Different data length.")) });

            var pubAckPacket = await readListener.ReceiveAsync();
            PublishPacket publishPacket = (PublishPacket)(await readListener.ReceiveAsync());

            await channel.WriteAndFlushManyAsync(
                PubAckPacket.InResponseTo(publishPacket),
                DisconnectPacket.Instance);
        }
        static async Task RunServerAsync()
        {
            ExampleHelper.SetConsoleLogger();

            var bossGroup = new MultithreadEventLoopGroup(1);
            var workerGroup = new MultithreadEventLoopGroup();
            X509Certificate2 tlsCertificate = null;
            if (ServerSettings.IsSsl)
            {
                tlsCertificate = new X509Certificate2(Path.Combine(ExampleHelper.ProcessDirectory, "dotnetty.com.pfx"), "password");
            }
            try
            {
                var serverReadListener = new ReadListeningHandler();
                var bootstrap = new ServerBootstrap();
                bootstrap
                    .Group(bossGroup, workerGroup)
                    .Channel<TcpServerSocketChannel>()
                    .Option(ChannelOption.SoBacklog, 100)
                    .Handler(new LoggingHandler("SRV-LSTN"))
                    .ChildHandler(new ActionChannelInitializer<ISocketChannel>(channel =>
                    {
                        IChannelPipeline pipeline = channel.Pipeline;
                        if (tlsCertificate != null)
                        {
                            pipeline.AddLast("tls", TlsHandler.Server(tlsCertificate));
                        }
                        pipeline.AddLast(new LoggingHandler("SRV-CONN"));
                        pipeline.AddLast(MqttEncoder.Instance, new MqttDecoder(true, 256 * 1024), serverReadListener);
                    }));

                IChannel boundChannel = await bootstrap.BindAsync(ServerSettings.Port);

                Console.ReadLine();

                await boundChannel.CloseAsync();
            }
            finally
            {
                await Task.WhenAll(
                    bossGroup.ShutdownGracefullyAsync(TimeSpan.FromMilliseconds(100), TimeSpan.FromSeconds(1)),
                    workerGroup.ShutdownGracefullyAsync(TimeSpan.FromMilliseconds(100), TimeSpan.FromSeconds(1)));
            }
        }

        static void Main() => RunServerAsync().Wait();
    }



}


