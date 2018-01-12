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
    class MQTTNetTest
    {
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