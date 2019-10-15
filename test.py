import asyncio
import zmq
import zmq.asyncio
from zmq.asyncio import Context


class Client(zmq.asyncio.Socket):
    def __init__(self, name, obj):
        self.__class__ = type(obj.__class__.__name__,
                              (self.__class__, obj.__class__),
                              {'name': name})


        self.__dict__ = obj.__dict__

class Test:
    def __init__(self, socket, thing):
        self.socket = socket
        self.thing = thing

    def sayhi(self):
        print(self.thing)

async def main():
    ctx = Context()

    try:
        socket = ctx.socket(zmq.SUB)
        socket2 = ctx.socket(zmq.SUB)

        socket2.connect("tcp://127.0.0.1:5555")
        socket.connect("tcp://127.0.0.1:5555")

        socket.subscribe(b'')
        socket2.subscribe(b'')

        client1 = Client("hi4", socket)
        test = Test(client1, "Something to say.")
        mapping = {client1.name: test}


        print(socket)
        print(client1)
        print(mapping)
        poller = zmq.asyncio.Poller()
        poller.register(test.socket, zmq.POLLIN)
        poller.register(socket2, zmq.POLLIN)
        sockets = await poller.poll()

        print(sockets)
        print(await test.socket.recv_string())
        print(await socket2.recv_string())
        mapping[test.socket.name].sayhi()

    finally:
        ctx.destroy()
        print("Context terminated")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
