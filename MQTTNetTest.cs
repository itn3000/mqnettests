namespace netmqtest
{
    using MQTTnet.Server;
    using MQTTnet;
    using MQTTnet.Adapter;
    using System;
    using System.Threading.Tasks;
    using System.Threading;
    using MQTTnet.Client;
    using MQTTnet.ManagedClient;
    using System.Net;
    using MessagePack;
    using System.Collections.Concurrent;

    class MQTTNetTestAdapter : MQTTnet.Adapter.IMqttServerAdapter
    {
        public event EventHandler<MqttServerAdapterClientAcceptedEventArgs> ClientAccepted;

        public Task StartAsync(IMqttServerOptions options)
        {
            throw new NotImplementedException();
        }

        public Task StopAsync()
        {
            throw new NotImplementedException();
        }
    }
    [MessagePackObject]
    public struct ReqMsg
    {
        public ReqMsg(string replyTopic, string body)
        {
            ReplyTopic = replyTopic;
            Body = body;
        }
        [MessagePack.Key(0)]
        public readonly string ReplyTopic;
        [MessagePack.Key(1)]
        public readonly string Body;
    }
    class MQTTNetTest
    {
        public static async Task ReqRepTest()
        {
            string requestTopic = "mytopic/A/request";
            var sw = new System.Diagnostics.Stopwatch();
            sw.Start();
            using (var cts = new CancellationTokenSource())
            using (var startedev = new ManualResetEventSlim())
            using (var connectedev = new ManualResetEventSlim())
            using (var recvev = new ManualResetEventSlim())
            {
                var fac = new MqttFactory();
                await Task.WhenAll(
                    Task.Run(async () =>
                    {
                        var svr = fac.CreateMqttServer();
                        svr.ApplicationMessageReceived += async (sender, ev) =>
                        {
                            if (ev.ApplicationMessage.Topic.Equals(requestTopic, StringComparison.Ordinal))
                            {
                                var reqmsg = MessagePackSerializer.Deserialize<ReqMsg>(ev.ApplicationMessage.Payload);
                                var msg = new MqttApplicationMessageBuilder().WithPayload("req").WithTopic(reqmsg.ReplyTopic).Build();
                                await svr.PublishAsync(msg);
                            }
                        };
                        svr.Started += (sender, ev) =>
                        {
                            startedev.Set();
                        };
                        var opt = new MqttServerOptionsBuilder()
                            .WithDefaultEndpoint()
                            .WithDefaultEndpointBoundIPAddress(IPAddress.Loopback)
                            .WithDefaultEndpointPort(10012)
                            .Build();
                        await svr.StartAsync(opt).ConfigureAwait(false);
                        cts.Token.WaitHandle.WaitOne();
                        await svr.StopAsync().ConfigureAwait(false);
                    })
                    ,
                    Task.Run(async () =>
                    {
                        var client = fac.CreateMqttClient();
                        string replyTopic = "mytopic/A/reply";
                        var queue = new ConcurrentQueue<TaskCompletionSource<byte[]>>();
                        client.ApplicationMessageReceived += (sender, ev) =>
                        {
                            if (queue.TryDequeue(out var tcs))
                            {
                                tcs.TrySetResult(ev.ApplicationMessage.Payload);
                            }
                        };
                        client.Connected += (sender, ev) =>
                        {
                            connectedev.Set();
                        };
                        var clientopt = new MqttClientOptionsBuilder()
                            .WithClientId("clid")
                            .WithTcpServer("localhost", 10012)
                            .Build()
                            ;
                        await client.ConnectAsync(clientopt).ConfigureAwait(false);
                        connectedev.Wait();
                        var topicFilter = new TopicFilterBuilder()
                            .WithTopic(replyTopic)
                            .WithAtLeastOnceQoS()
                            .Build()
                            ;
                        await client.SubscribeAsync(topicFilter).ConfigureAwait(false);
                        Console.WriteLine($"client task loop started:{sw.Elapsed}");
                        var beginTime = sw.Elapsed;
                        const int LoopNum = 100000;
                        for (int i = 0; i < LoopNum; i++)
                        {
                            var reqpayload = MessagePackSerializer.Serialize(new ReqMsg(replyTopic, "hoge"));
                            var msg = new MqttApplicationMessageBuilder()
                                .WithPayload(reqpayload)
                                .WithTopic(requestTopic)
                                .Build();
                            ;
                            var reqtcs = new TaskCompletionSource<byte[]>();
                            queue.Enqueue(reqtcs);
                            await client.PublishAsync(msg).ConfigureAwait(false);
                            await reqtcs.Task;
                        }
                        var endTime = sw.Elapsed;
                        Console.WriteLine($"client task loop done:{sw.Elapsed},rps={LoopNum/(endTime.Subtract(beginTime).TotalSeconds)}");
                    }).ContinueWith(t =>
                    {
                        Console.WriteLine($"all client task done:{sw.Elapsed}");
                        cts.Cancel();
                        if (t.IsCanceled)
                        {
                            throw new TaskCanceledException("server task cancelled", t.Exception);
                        }
                        else if (t.IsFaulted)
                        {
                            throw new AggregateException(t.Exception);
                        }
                    })
                ).ConfigureAwait(false);
            }
        }
        public static async Task TestMany(int loopNum, int taskNum)
        {
            var fac = new MqttFactory();
            using (var cts = new CancellationTokenSource())
            {
                var sw = new System.Diagnostics.Stopwatch();
                sw.Start();
                await Task.WhenAll(
                    Task.Run(async () =>
                    {
                        var svr = fac.CreateMqttServer();
                        svr.Started += (sender, ev) =>
                        {
                            Console.WriteLine($"{sw.Elapsed} svr started");
                        };
                        svr.ClientConnected += (sender, ev) =>
                        {
                            Console.WriteLine($"{sw.Elapsed} client connected(S): {ev.Client.ClientId}");
                        };
                        svr.ClientDisconnected += (sender, ev) =>
                        {
                            Console.WriteLine($"{sw.Elapsed} client disconnected(S): {ev.Client.ClientId}");
                        };
                        var opt = new MqttServerOptionsBuilder()
                            .WithDefaultEndpoint()
                            .WithDefaultEndpointBoundIPAddress(IPAddress.Loopback)
                            .WithDefaultEndpointPort(10012)
                            .Build();
                        await svr.StartAsync(opt).ConfigureAwait(false);
                        cts.Token.WaitHandle.WaitOne();
                        await svr.StopAsync().ConfigureAwait(false);
                    })
                    ,
                    Task.Run(async () =>
                    {
                        await Task.Delay(100).ConfigureAwait(false);
                        var client = fac.CreateMqttClient();
                        var clientopt = new MqttClientOptionsBuilder()
                            .WithClientId("clid")
                            .WithTcpServer("localhost", 10012)
                            .Build()
                            ;
                        // var opt = new ManagedMqttClientOptionsBuilder()
                        //     .WithClientOptions(clientopt)
                        //     .WithAutoReconnectDelay(TimeSpan.FromMilliseconds(100))
                        //     .Build();
                        using (var connectedEvent = new ManualResetEventSlim(false))
                        {
                            client.ApplicationMessageReceived += (sender, ev) =>
                            {
                                Console.WriteLine($"{sw.Elapsed} recv {ev.ApplicationMessage.Topic},{ev.ApplicationMessage.Payload[0]}");
                            };
                            client.Connected += (sender, ev) =>
                            {
                                Console.WriteLine($"{sw.Elapsed} client connected");
                                connectedEvent.Set();
                            };
                            client.Disconnected += (sender, ev) =>
                            {
                                Console.WriteLine($"{sw.Elapsed} client disconnected");
                            };
                            var topicFilter = new TopicFilterBuilder()
                                .WithTopic("mytopic/A").WithAtLeastOnceQoS().Build();
                            Console.WriteLine($"{sw.Elapsed} begin connection");
                            await client.ConnectAsync(clientopt).ConfigureAwait(false);
                            // await client.StartAsync(opt).ConfigureAwait(false);
                            connectedEvent.Wait();
                            if (!client.IsConnected)
                            {
                                Console.WriteLine($"not connected");
                            }
                            else
                            {
                                Console.WriteLine($"{sw.Elapsed} connection established");
                            }
                            await client.SubscribeAsync(new[] { topicFilter }).ConfigureAwait(false);
                            Console.WriteLine($"{sw.Elapsed} subscribing done");
                            var msgbuilder = new MqttApplicationMessageBuilder();
                            msgbuilder = msgbuilder.WithTopic("mytopic/A").WithAtLeastOnceQoS();
                            var dat = new byte[1];
                            for (int i = 0; i < 100; i++)
                            {
                                await Task.Yield();
                                dat[0] = (byte)(i % 255);
                                var msg = msgbuilder.WithPayload(dat).Build();
                                await client.PublishAsync(msg).ConfigureAwait(false);
                                Console.WriteLine($"{sw.Elapsed}: publish{i}");
                            }
                            await client.UnsubscribeAsync(new[] { topicFilter.Topic }).ConfigureAwait(false);
                            await client.DisconnectAsync().ConfigureAwait(false);
                            // await client.StopAsync().ConfigureAwait(false);
                            // cts.Cancel();
                        }

                        // var opt = new MqttClientOptionsBuilder()
                        //     .Build();
                        // await client.ConnectAsync(opt).ConfigureAwait(false);
                        // var topicFilter = new TopicFilterBuilder()
                        //     .WithTopic("topic")
                        //     .Build();
                        // var subresults = await client.SubscribeAsync(new TopicFilter[] {topicFilter}).ConfigureAwait(false);
                        // foreach(var subresult in subresults)
                        // {
                        // }
                    }).ContinueWith(t =>
                    {
                        cts.Cancel();
                    })
                );
                sw.Stop();
            }

        }
    }
}