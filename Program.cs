using System;

namespace netmqtest
{
    using NetMQ;
    using NetMQ.Sockets;
    using System.Threading.Tasks;
    using System.Threading;
    using System.Linq;
    class Program
    {
        static RequestSocket CreateClientSocket(int port)
        {
            var ret = new RequestSocket($">tcp://127.0.0.1:{port}");
            return ret;
        }
        static async Task ClientTask(int port, CancellationToken ctoken, int idx, long maxLoop)
        {
            try
            {
                var sw = new System.Diagnostics.Stopwatch();
                sw.Start();
                using (var cl = CreateClientSocket(port))
                {
                    for (long i = 0; i < maxLoop && !ctoken.IsCancellationRequested; i++)
                    {
                        DoClientRequest(cl, idx, i);
                        await Task.Yield();
                    }
                }
                Console.WriteLine($"client elapsed({idx}):{sw.Elapsed}");
            }
            catch (Exception e)
            {
                Console.WriteLine($"client exc:{e}");

            }
        }
        static async Task ClientWithSingleSocket(RequestSocket cl, CancellationToken ctoken, int idx, long maxLoop)
        {
            try
            {
                var sw = new System.Diagnostics.Stopwatch();
                sw.Start();
                for (long i = 0; i < maxLoop && !ctoken.IsCancellationRequested; i++)
                {
                    lock (cl)
                    {
                        DoClientRequest(cl, idx, i);
                    }
                    await Task.Yield();
                }
                Console.WriteLine($"client elapsed({idx}):{sw.Elapsed}");
            }
            catch (Exception e)
            {
                Console.WriteLine($"client exc({idx}):{e}");

            }
        }
        static byte[] i64tobytes(long val)
        {
            return new byte[8]{
                (byte)(val & 0xff),
                (byte)((val >> 8) & 0xff),
                (byte)((val >> 16) & 0xff),
                (byte)((val >> 24) & 0xff),
                (byte)((val >> 32) & 0xff),
                (byte)((val >> 40) & 0xff),
                (byte)((val >> 48) & 0xff),
                (byte)((val >> 56) & 0xff),
            };
        }
        static long bytestoi64(byte[] bytes)
        {
            return (long)bytes[0]
                + ((long)bytes[1] << 8)
                + ((long)bytes[2] << 16)
                + ((long)bytes[3] << 24)
                + ((long)bytes[4] << 32)
                + ((long)bytes[5] << 40)
                + ((long)bytes[6] << 48)
                + ((long)bytes[7] << 56)
                ;
        }
        static void DoClientRequest(RequestSocket cl, int idx, long loopCount)
        {
            cl.SendFrame(i64tobytes(loopCount + 1));
            cl.ReceiveFrameBytes();
        }
        static Task<long> ServerTask(ResponseSocket srv, CancellationToken ctoken)
        {
            return Task.Run(() =>
            {
                long ret = 0;
                using (var poller = new NetMQPoller())
                using (ctoken.Register(() => poller.StopAsync()))
                {
                    srv.ReceiveReady += (sender, e) =>
                    {
                        var msg = e.Socket.ReceiveFrameBytes();
                        var recvvalue = bytestoi64(msg);
                        while (true)
                        {
                            var old = Interlocked.CompareExchange(ref ret, ret + recvvalue, ret);
                            if (old < ret)
                            {
                                break;
                            }
                        }
                        // Console.WriteLine($"recv {msg}");
                        e.Socket.SendFrame("");
                        // e.Socket.SendFrame("OK", true);
                        // e.Socket.SendFrame("mogemoge", false);
                    };
                    poller.Add(srv);
                    try
                    {
                        Console.WriteLine($"polling start");
                        poller.Run();
                        Console.WriteLine($"polling end");
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine($"srv ex={e}");
                    }
                }
                return ret;
            });
        }
        static void ReqRepPattern()
        {

        }
        static void PubSubPattern()
        {
            const long maxLoop = 100000;
            var sw = new System.Diagnostics.Stopwatch();
            sw.Start();
            var port = 10006;
            long count = 0;
            using (var pub = new PublisherSocket($"tcp://127.0.0.1:{port}"))
            using (var sub = new SubscriberSocket($"tcp://127.0.0.1:{port}"))
            using (var poller = new NetMQPoller())
            using (var csrc = new CancellationTokenSource())
            using (csrc.Token.Register(() => poller.StopAsync()))
            {
                sub.ReceiveReady += (sender, ev) =>
                {
                    bool isMore;
                    var topic = ev.Socket.ReceiveFrameString(out isMore);
                    if (isMore)
                    {
                        var b = ev.Socket.ReceiveFrameBytes();
                        Console.WriteLine($"{bytestoi64(b)}");
                    }
                    Interlocked.Increment(ref count);
                    if (count == maxLoop)
                    {
                        csrc.Cancel();
                    }
                };
                sub.Subscribe("mytopic");
                poller.Add(sub);
                poller.RunAsync();
                Task.Delay(100).Wait();
                for (long i = 0; i < maxLoop; i++)
                {
                    pub.SendMoreFrame("mytopic").SendFrame(i64tobytes(i));
                }
                Console.WriteLine($"begin wait");
                csrc.CancelAfter(10 * 1000);
                csrc.Token.WaitHandle.WaitOne();
            }
            sw.Stop();
            Console.WriteLine($"pubsub:{count}, {sw.Elapsed}, rps = {count * 1000 / sw.ElapsedMilliseconds}");
        }
        static void ReqRepTest()
        {
            var sw = new System.Diagnostics.Stopwatch();
            sw.Start();
            var port = 10005;
            long maxLoop = 100000;
            Console.WriteLine($"creating socket");
            const int clientNum = 10;
            using (var srv = new ResponseSocket($"@tcp://127.0.0.1:{port}"))
            using (var client = new RequestSocket($">tcp://127.0.0.1:{port}"))
            using (var csrc = new CancellationTokenSource())
            {
                Console.WriteLine($"begin loop");
                Task.WhenAll(ServerTask(srv, csrc.Token).ContinueWith((t) =>
                {
                    if (t.Status == TaskStatus.RanToCompletion)
                    {
                        Console.WriteLine($"total={t.Result}");
                    }
                }),
                Task.Run(async () =>
                {
                    await Task.WhenAll(Enumerable.Range(0, clientNum).Select(i => ClientWithSingleSocket(client, csrc.Token, i, maxLoop / clientNum))).ConfigureAwait(false);
                    csrc.Cancel();
                })
                ).Wait();
            }
            sw.Stop();
            Console.WriteLine($"Hello World!:{sw.Elapsed},rps={maxLoop * 1000 / sw.ElapsedMilliseconds}");
        }
        static void Main(string[] args)
        {
            // MQTTNetTest.TestMany(100, 100).Wait();
            // AMQPNetTest.TestMany().Wait();
            ReqRepTest();
        }
    }
}
