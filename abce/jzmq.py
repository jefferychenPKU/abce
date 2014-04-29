import sys
if not sys.platform.startswith('java'):
    from zmq import * #pylint: disable=W0614,W0401
else:
    from org.zeromq.ZMQ import * #pylint: disable=F0401
    from org.zeromq import ZContext #pylint: disable=F0401
    from org.python.core.util.StringUtil import fromBytes #pylint: disable=F0401
    import cPickle
    pickle = cPickle

    class Context: #pylint: disable=R0903
        def __init__(self):
            self.context = ZContext()

        def socket(self, socket_type):
            """ Create a Socket associated with this Context.

            Parameters:
                socket_type : int
                The socket type, which can be any of the 0MQ socket types: REQ, REP, PUB, SUB, PAIR, DEALER, ROUTER, PULL, PUSH, etc.
            """
            return Socket(self.context.createSocket(socket_type))

        def __getattr__(self, name):
            return getattr(self.context, name)

    class Socket:
        def __init__(self, java_socket):
            self.socket = java_socket

        def setsockopt(self, action, option):
            if action == SUBSCRIBE:
                self.socket.subscribe(option)
            elif action == IDENTITY:
                self.socket.setIdentity(option)
            else:
                raise AttributeError(" %i not yet implemented in jzmq" % action)

        def send_multipart(self, msg_parts):
            """
            send a sequence of buffers as a multipart message
            The zmq.SNDMORE flag is added to all msg parts before the last.

            Parameters:
                msg_parts : iterable
                A sequence of objects to send as a multipart message. Each element can be any sendable object (Frame, bytes, buffer-providers)
                flags : int, optional
                    SNDMORE is handled automatically for frames before the last.
                copy : bool, optional
                    Should the frame(s) be sent in a copying or non-copying manner.
                track : bool, optional
                    Should the frame(s) be tracked for notification that ZMQ has finished with it (ignored if copy=True).

            Returns:
                None : if copy or not track
                MessageTracker : if track and not copy a MessageTracker object, whose pending property will be True until the last send is completed.
            """
            try:
                for part in msg_parts[:-1]:
                    self.socket.sendMore(part)
                return self.socket.send(msg_parts[-1])
            except TypeError:
                print msg_parts
                raise

        def recv(self, flags=0):
            return fromBytes(self.socket.recv(flags))

        def recv_multipart(self, flags=0):
            """ receive a multipart message as a list of bytes or Frame objects

            Parameters:
                flags : int, optional
                    Any supported flag: NOBLOCK. If NOBLOCK is set, this method will raise a ZMQError with EAGAIN if a message is not ready. If NOBLOCK is not set, then this method will block until a message arrives.
                copy : bool, optional
                    Should the message frame(s) be received in a copying or non-copying manner? If False a Frame object is returned for each part, if True a copy of the bytes is made for each frame.
                track : bool, optional
                    Should the message frame(s) be tracked for notification that ZMQ has finished with it? (ignored if copy=True)
            Returns:
                msg_parts : list
                    A list of frames in the multipart message; either Frames or bytes, depending on copy.
            """
            messages = []
            while True:
                messages.append(self.recv(flags))
                if not(self.socket.hasReceiveMore()):
                    break
            return messages

        def send_pyobj(self, obj, flags=0, protocol=-1):
            """
            send a Python object as a message using pickle to serialize

            Parameters:
                obj : Python object
                    The Python object to send.

                flags : int
                    Any valid send flag.

                protocol : int
                    The pickle protocol number to use. Default of -1 will select the highest supported number. Use 0 for multiple platform support.
            """
            msg = pickle.dumps(obj, protocol)
            return self.socket.send(msg, flags)

        def recv_pyobj(self, flags=0):
            """receive a Python object as a message using pickle to serialize

            Parameters
            ----------
            flags : int
                Any valid recv flag.

            Returns
            -------
            obj : Python object
                The Python object that arrives as a message.
            """
            s = self.recv(flags)
            return pickle.loads(s)

        def __getattr__(self, name):
            return getattr(self.socket, name)

    IDENTITY = 5
    SUBSCRIBE = 6
