namespace netmqtest
{
    using Amqp;
    using Amqp.Listener;
    using Amqp.Framing;
    using System.Threading;
    using System;
    using System.Threading.Tasks;
    class RequestProcessor : IRequestProcessor
    {
        public int Credit => 100;

        public void Process(RequestContext requestContext)
        {
            try
            {
                var x = (int)requestContext.Message.ApplicationProperties["offset"];
                // Console.WriteLine($"coming request:{x},{requestContext.State}");
                var msg = new Message($"reply{x}");
                msg.ApplicationProperties = new ApplicationProperties();
                msg.ApplicationProperties["offset"] = 0;
                // requestContext.ResponseLink.SendMessage(msg);
                requestContext.Complete(msg);
            }
            catch (Exception e)
            {
                Console.WriteLine($"svr proc err: {e}");

            }
        }
    }
    class AMQPNetTest
    {
        public static async Task ReqRepTest(int LoopNum)
        {
            var sw = new System.Diagnostics.Stopwatch();
            sw.Start();
            const string url = "amqp://guest:guest@127.0.0.1:5672";
            const string requestUrl = "request_processor";
            const string receiverName = "client-request-receiver";
            const string sendername = "client-request-sender";
            using (var cts = new CancellationTokenSource())
            using (var connected = new ManualResetEventSlim())
            using (var started = new ManualResetEventSlim())
            {
                await Task.WhenAll(
                    Task.Run(() =>
                    {
                        var uri = new Uri(url);
                        var host = new ContainerHost(new[] { uri }, null, uri.UserInfo);
                        host.Open();
                        try
                        {
                            host.RegisterRequestProcessor(requestUrl, new RequestProcessor());
                            Console.WriteLine($"wait begin");
                            cts.Token.WaitHandle.WaitOne();
                        }
                        finally
                        {
                            host.Close();
                        }
                        Console.WriteLine($"svr thread done");
                    })
                    ,
                    Task.Run(async () =>
                    {
                        await Task.Yield();
                        var con = new Connection(new Address(url));
                        try
                        {
                            var session = new Session(con);
                            string replyTo = "client-abc";
                            var recvAttach = new Attach()
                            {
                                Source = new Source() { Address = requestUrl },
                                Target = new Target() { Address = replyTo }
                            };
                            var recver = new ReceiverLink(session, receiverName, recvAttach, (link, attach) =>
                            {
                                Console.WriteLine($"{link},{attach}");
                            });
                            recver.Start(300);
                            var sender = new SenderLink(session, sendername, new Target() { Address = requestUrl }, (obj, ev) =>
                               {
                                   connected.Set();
                               });
                            connected.Wait();
                            var beginTime = sw.Elapsed;
                            Console.WriteLine($"begin sending({sw.Elapsed})");
                            for (int i = 0; i < LoopNum; i++)
                            {
                                var msg = new Message($"clientmessage{i}");
                                msg.Properties = new Properties()
                                {
                                    MessageId = "command-request",
                                    ReplyTo = replyTo
                                };
                                msg.ApplicationProperties = new ApplicationProperties();
                                msg.ApplicationProperties["offset"] = i;
                                sender.Send(msg);
                                var response = recver.Receive();
                                recver.Accept(response);
                            }
                            var endTime = sw.Elapsed;
                            Console.WriteLine($"elapsed(amqp):({sw.Elapsed}, rps={LoopNum/endTime.Subtract(beginTime).TotalSeconds}");
                        }
                        finally
                        {
                            con.Close();
                        }

                    })
                    .ContinueWith(t =>
                    {
                        Console.WriteLine($"cancelling:{t.Status}");
                        cts.Cancel();
                        if (t.IsCanceled)
                        {
                            Console.WriteLine($"a");
                            throw new AggregateException(t.Exception);
                        }
                        if (t.IsFaulted)
                        {
                            Console.WriteLine($"b");
                            throw new AggregateException(t.Exception);
                        }
                    })
                ).ConfigureAwait(false); // Task.WhenAll
            }
        }
        public static async Task TestMany()
        {
            const string url = "amqp://guest:guest@127.0.0.1:5672";
            const string requestUrl = "request_processor";
            const string receiverName = "client-request-receiver";
            const string sendername = "client-request-sender";
            using (var cts = new CancellationTokenSource())
            {
                await Task.WhenAll(
                    Task.Run(() =>
                    {
                        var uri = new Uri(url);
                        var host = new ContainerHost(new[] { uri }, null, uri.UserInfo);
                        Console.WriteLine($"opening host");
                        host.Open();
                        try
                        {
                            host.RegisterRequestProcessor(requestUrl, new RequestProcessor());
                            Console.WriteLine($"wait begin");
                            cts.Token.WaitHandle.WaitOne();
                        }
                        finally
                        {
                            host.Close();
                        }
                        Console.WriteLine($"svr thread done");
                    })
                    ,
                    Task.Run(async () =>
                    {
                        await Task.Delay(1000).ConfigureAwait(false);
                        var con = new Connection(new Address(url));
                        try
                        {

                            var session = new Session(con);
                            string replyTo = "client-abc";
                            var recvAttach = new Attach()
                            {
                                Source = new Source() { Address = requestUrl },
                                Target = new Target() { Address = replyTo }
                            };
                            var recver = new ReceiverLink(session, receiverName, recvAttach, (link, attach) =>
                            {
                                Console.WriteLine($"{link},{attach}");
                            });
                            recver.Start(300);
                            var sender = new SenderLink(session, sendername, requestUrl);
                            Console.WriteLine($"{sender.IsClosed}");
                            for (int i = 0; i < 100; i++)
                            {
                                var msg = new Message($"clientmessage{i}");
                                msg.Properties = new Properties()
                                {
                                    MessageId = "command-request",
                                    ReplyTo = replyTo
                                };
                                msg.ApplicationProperties = new ApplicationProperties();
                                msg.ApplicationProperties["offset"] = i;
                                sender.Send(msg);
                                var response = recver.Receive();
                                recver.Accept(response);
                                Console.WriteLine($"{response.Properties}, {response.Body}");
                            }
                        }
                        finally
                        {
                            con.Close();
                        }

                    })
                    .ContinueWith(t =>
                    {
                        Console.WriteLine($"cancelling:{t.Status}");
                        cts.Cancel();
                        if (t.IsCanceled)
                        {
                            Console.WriteLine($"a");
                            throw new AggregateException(t.Exception);
                        }
                        if (t.IsFaulted)
                        {
                            Console.WriteLine($"b");
                            throw new AggregateException(t.Exception);
                        }
                    })
                ).ConfigureAwait(false); // Task.WhenAll
            }
        }
    }
}