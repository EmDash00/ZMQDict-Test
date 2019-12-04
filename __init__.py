import asyncio
import pickle
import codecs
import socket
import zmq
import zmq.asyncio
from zmq.auth.asyncio import AsyncioAuthenticator


class Context:
    def __init__(self, global_whitelist=None, global_pubkeys_dir=None):
        self._auth = AsyncioAuthenticator()
        self._callbacks = {}
        self._dicts = []

        if (global_whitelist is not None):
            addrs = []
            for addr in global_whitelist:
                addrs.append(socket.gethostbyname(addr))

            self._auth.allow(*tuple(addrs))

        if (global_pubkeys_dir is not None):
            self._auth.configure_curve(domain='*', location=global_pubkeys_dir)

    def __enter__(self):
        print("-----CONTEXT START-----\n")
        self._zmq_ctx = zmq.asyncio.Context()
        self._auth.start()
        self.poller = zmq.asyncio.Poller()
        self._open = True

        return self

    async def begin(self):
        async def poll():
            while self._open:
                sockets = dict(await self.poller.poll())
                print("POLLIN on:", sockets)
                for socket in sockets:
                    await self._callbacks[socket]()

        # Start polling asynchronously.
        asyncio.ensure_future(poll())

    def __exit__(self, exc_type, exc_val, exc_tb):
        print("\n-----CONTEXT END-------")

        self._open = False

        for zdict in self._dicts:
            zdict.close()

        self._zmq_ctx.destroy()


class ZMQDictServer:
    def __init__(self,
                 context,
                 ip='localhost',
                 port=9535,
                 whitelist=['localhost'],
                 pubkeys_dir=None):

        self._dict = {}

        self._listener = context._zmq_ctx.socket(zmq.REP)
        self._sychronizer = context._zmq_ctx.socket(zmq.PUB)

        self._listener.linger = 0
        self._sychronizer.linger = 0

        addrs = []

        for addr in whitelist:
            addrs.append(socket.gethostbyname(addr))

        self._listener.bind("tcp://{}:{}".format(socket.gethostbyname(ip),
                                                 port))

        self._sync_port = self._sychronizer.bind_to_random_port(
            "tcp://{}".format(socket.gethostbyname(ip)))

        context._auth.allow(*tuple(addrs))

        # Associate the listener with this dict's callback
        context._callbacks[self._listener] = self._listen

        # Add our dict to the list of dicts the context manages
        context._dicts.append(self)

        # Register our listener to the poller.
        context.poller.register(self._listener, zmq.POLLIN)

    def lookup(self, key, default=None):

        if (isinstance(default, Exception)):
            if key not in self._dict:
                raise default

        return self._dict.get(key, default)

    async def publish(self, key, value):
        self._dict[key] = value
        await self._forward(self._format_request(key, value))

    def close(self):
        # Let the clients know the server has closed.
        self._sychronizer.send_string("CLOSED")

    # Listen for incoming requests
    async def _listen(self):
        request = await self._listener.recv_string()

        # INIT populates the dict on startup and tells the client where to look
        # for change forwarding
        if (request == "INIT"):
            self._listener.send_string("{},{}".format(
                self._sync_port,
                codecs.encode(pickle.dumps(self._dict), "base64").decode()))

            print("SIGINIT recieved from client")
        else:
            # All other requests are regular requests
            await self.publish(*self._parse_request(request))
            self._listener.send_string("PUSHED")

    async def _forward(self, request):
        await self._sychronizer.send_string(request)
        print("FORWARD: {}".format(request))

    def _format_request(self, key, value=''):
        if (value != ''):
            return "{},{}".format(
                key,
                codecs.encode(pickle.dumps(value), "base64").decode())

        return "{},{}".format(key, value)

    def _parse_request(self, request):
        split_request = request.split(',')

        return (split_request[0],
                pickle.loads(
                    codecs.decode(split_request[1].encode(), 'base64')))


class ZMQDictClient:
    def __init__(self,
                 context,
                 ip='localhost',
                 port=9535,
                 whitelist=None,
                 pubkeys_dir=None,
                 strict=False):

        self._dict = None

        self._requester = context._zmq_ctx.socket(zmq.REQ)
        self._sychronizer = context._zmq_ctx.socket(zmq.SUB)

        self._requester.linger = 0
        self._sychronizer.linger = 0

        self._sychronizer.setsockopt_string(zmq.SUBSCRIBE, "")
        self._sychronizer.subscribe('')

        self.strict = strict
        self.ip = ip
        self.port = port

        context._callbacks[self._sychronizer] = self._synchronize
        context._dicts.append(self)
        context.poller.register(self._sychronizer, zmq.POLLIN)

    async def open(self):
        # Signal INIT to the server and grab the whole dict. This also has the
        # effect of making sure the client doesn't continue until the server is
        # started.

        self._requester.connect("tcp://{}:{}".format(
            socket.gethostbyname(self.ip), self.port))

        self._requester.send_string("INIT")
        format_str = await self._requester.recv_string()
        print("SIGINIT RESPONSE:", format_str)

        split_request = format_str.split(',')

        self._dict = pickle.loads(
            codecs.decode(split_request[1].encode(), 'base64'))

        self._sychronizer.connect("tcp://{}:{}".format(
            socket.gethostbyname(self.ip), split_request[0]))

    def close(self):
        pass

    def lookup(self, key, default=None):

        if (isinstance(default, Exception)):
            if key not in self._dict:
                raise default

        return self._dict.get(key, default)

    async def publish(self, key, value):
        await self._request(self._format_request(key, value))

    async def _request(self, request):
        await self._requester.send_string(request)
        string = await self._requester.recv_string()

    async def _synchronize(self):
        request = self._parse_request(await self._sychronizer.recv_string())
        print("RECIEVED FORWARD:", request)

        # This is different since publishing on the client technically just
        # bounces the change to the server who bounces it back to everyone. This
        # may seem convoluted since it has to make a full trip to get back but
        # this way everyone stays in sync. If the request can't be forwarded by
        # the server, it means you're disconnected and no longer in sync.

        self._dict[request[0]] = request[1]

    def _format_request(self, key, value=''):
        if (value != ''):
            return "{},{}".format(
                key,
                codecs.encode(pickle.dumps(value), "base64").decode())

        return "{},{}".format(key, value)

    def _parse_request(self, request):
        split_request = request.split(',')

        return (split_request[0],
                pickle.loads(
                    codecs.decode(split_request[1].encode(), 'base64')))

    def close(self):
        if (self.strict):
            raise Exception("Server Closed.")


async def main():
    with Context() as ctx:
        await ctx.begin()
        server = ZMQDictServer(ctx)
        client = ZMQDictClient(ctx)
        await client.open()

        await client.publish('data', "data")
        print(server.lookup("data"))
        await asyncio.sleep(.000001)
        print(client.lookup('data'))
        await client.publish("data2", 'more data')
        await asyncio.sleep(.000001)
        print(client.lookup('data2'))


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
