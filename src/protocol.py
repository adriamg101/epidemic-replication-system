from client_socket_manager import ClientSocketManager
import struct


class EmptySocketError(Exception):
    pass


T_SIZE = 2      # 2 bytes
T_SIZE_S = "H"  # unsigned char (0-255)
SIZE = 2        # 2 bytes
SIZE_S = "H"    # unsigned short (0-65535)


class Frame:
    def __init__(self, type_: int,
                 *,
                 origin: int | None = None,
                 timestamp: int | None = None,
                 version: int | None = None,
                 key: int | None = None,
                 value: int | None = None):
        self.type = type_
        self.origin = origin
        self.timestamp = timestamp
        self.version = version
        self.key = key
        self.value = value

    def convertToBytes(self) -> bytes:
        data = bytearray()

        data += struct.pack(("!" + T_SIZE_S), self.type)

        if self.origin is not None:
            data += struct.pack(("!" + SIZE_S), self.origin)

        if self.timestamp is not None:
            data += struct.pack(("!" + SIZE_S), self.timestamp)

        if self.version is not None:
            data += struct.pack(("!" + SIZE_S), self.version)

        if self.key is not None:
            data += struct.pack(("!" + SIZE_S), self.key)

        if self.value is not None:
            data += struct.pack(("!" + SIZE_S), self.value)

        return bytes(data)


class Protocol:
    # Misc.
    HELLO = 0x00    # <origin>
    ACK = 0x01      #
    ERR = 0x02      #
    # C <-> C
    REQUEST = 0x03  # <origin><timestamp>
    REPLY = 0x04    # <origin><timestamp>
    RELEASE = 0x05  # <origin><timestamp>
    # C <-> L1
    # L1 <-> L2
    # Pr <-> Bkup.
    SYNC = 0x06     #
    # Server <-> Client
    READ = 0x07     # <key>
    RESP = 0x08     # <value>
    UNKNOWN = 0x09  #
    WRITE = 0x0A    # <key><value>
    # All
    DATA = 0x0B     # <version><key><value>
    DONE = 0x0C     #

    @ staticmethod
    def _recv_origin(type_: int, skt: ClientSocketManager):
        raw = skt.recive_message(SIZE)
        str_parser = "!" + (SIZE_S)
        (origin,) = struct.unpack(str_parser, raw)

        return Frame(type_, origin=origin)

    @ staticmethod
    def _recv_type(type_: int, skt: ClientSocketManager):
        return Frame(type_)

    @ staticmethod
    def _recv_origin_timestamp(type_: int, skt: ClientSocketManager):
        raw = skt.recive_message(SIZE * 2)
        str_parser = "!" + (SIZE_S * 2)
        (origin, timestamp,) = struct.unpack(str_parser, raw)

        return Frame(type_, origin=origin, timestamp=timestamp)

    @ staticmethod
    def _recv_key(type_: int, skt: ClientSocketManager):
        raw = skt.recive_message(SIZE)
        str_parser = "!" + (SIZE_S)
        (key,) = struct.unpack(str_parser, raw)

        return Frame(type_, key=key)

    @ staticmethod
    def _recv_value(type_: int, skt: ClientSocketManager):
        raw = skt.recive_message(SIZE)
        str_parser = "!" + (SIZE_S)
        (value,) = struct.unpack(str_parser, raw)

        return Frame(type_, value=value)

    @ staticmethod
    def _recv_key_value(type_: int, skt: ClientSocketManager):
        raw = skt.recive_message(SIZE * 2)
        str_parser = "!" + (SIZE_S * 2)
        (key, value,) = struct.unpack(str_parser, raw)

        return Frame(type_, key=key, value=value)

    @ staticmethod
    def _recv_version_key_value(type_: int, skt: ClientSocketManager):
        raw = skt.recive_message(SIZE * 3)
        str_parser = "!" + (SIZE_S * 3)
        (version, key, value,) = struct.unpack(str_parser, raw)

        return Frame(type_, version=version, key=key, value=value)

        # Fa la funcio de un switch
    recv_handlers = {
        HELLO: _recv_origin,
        ACK: _recv_type,
        ERR: _recv_type,
        REQUEST: _recv_origin_timestamp,
        REPLY: _recv_origin_timestamp,
        RELEASE: _recv_origin_timestamp,
        SYNC: _recv_type,
        READ: _recv_key,
        RESP: _recv_value,
        UNKNOWN: _recv_type,
        WRITE: _recv_key_value,
        DATA: _recv_version_key_value,
        DONE: _recv_type
    }

    def recv_message(self, skt: ClientSocketManager) -> Frame:
        raw = skt.recive_message(T_SIZE)

        if len(raw) == 0:
            raise EmptySocketError

        (type_,) = struct.unpack(("!" + T_SIZE_S), raw)

        # Usem el "switch"
        return self.recv_handlers[type_](type_, skt)

    def recv_unknown(self, skt: ClientSocketManager, blocking: bool = False) -> Frame:
        skt.socket.setblocking(blocking)
        try:
            frame = self.recv_message(skt)
        except BlockingIOError:
            raise EmptySocketError
        finally:
            skt.socket.setblocking(True)
        return frame

    def recv_frame_with_ack(self, skt: ClientSocketManager, obj: int) -> Frame:
        while True:
            # Escoltem frame
            frame = self.recv_message(skt)

            # Mirem si es l'especificat o no
            if frame.type == obj:
                break
            else:
                skt.send_message(self.err())

        # Enviem ACK
        skt.send_message(self.ack())

        return frame

    def recv_unknown_with_ack(self, skt: ClientSocketManager, blocking: bool = False) -> Frame:
        # Rebem el unknown (pot saltar EmptySocketError)
        frame = self.recv_unknown(skt, blocking)

        # Enviem ACK
        skt.send_message(self.ack())

        return frame

    # Funciones de crear mensajes
    def hello(self, origin: int) -> bytes:
        return Frame(self.HELLO, origin=origin).convertToBytes()

    def ack(self) -> bytes:
        return Frame(self.ACK).convertToBytes()

    def err(self) -> bytes:
        return Frame(self.ERR).convertToBytes()

    def request(self, origin: int, timestamp: int) -> bytes:
        return Frame(self.REQUEST, origin=origin, timestamp=timestamp).convertToBytes()

    def reply(self, origin: int, timestamp: int) -> bytes:
        return Frame(self.REPLY, origin=origin, timestamp=timestamp).convertToBytes()

    def release(self, origin: int, timestamp: int) -> bytes:
        return Frame(self.RELEASE, origin=origin, timestamp=timestamp).convertToBytes()

    def sync(self) -> bytes:
        return Frame(self.SYNC).convertToBytes()

    def read(self, key: int) -> bytes:
        return Frame(self.READ, key=key).convertToBytes()

    def resp(self, value: int) -> bytes:
        return Frame(self.RESP, value=value).convertToBytes()

    def unknown(self) -> bytes:
        return Frame(self.UNKNOWN).convertToBytes()

    def write(self, key: int, value: int) -> bytes:
        return Frame(self.WRITE, key=key, value=value).convertToBytes()

    def data(self, version: int, key: int, value: int) -> bytes:
        return Frame(self.DATA, version=version, key=key, value=value).convertToBytes()

    def done(self) -> bytes:
        return Frame(self.DONE).convertToBytes()

    def send_frame_wait_ack(self, frame: bytes, skt: ClientSocketManager):
        while True:
            # Enviem frame
            skt.send_message(frame)

            # Esperem a rebre ACK
            resp = self.recv_message(skt)

            if resp.type == self.ACK:
                # Si es ACK sortim del bucle
                break
            elif resp.type == self.ERR:
                # Si rebem ERR reenviem la trama
                continue
            else:
                # Hi ha hagut error retornem ERR
                self.send_frame_wait_ack(self.err(), skt)

    TIMEOUT_LAST_FRAME = 10

    def send_final_frame(self, frame: bytes, skt: ClientSocketManager):
        # Enviem frame
        skt.send_message(frame)

        # No fem res, esperem resposta durant un temps per si arriba ACK
        # pero no fem res mes
        skt.setTimeout(self.TIMEOUT_LAST_FRAME)
        try:
            self.recv_message(skt)
        except TimeoutError:
            # No hacer nada i pasar
            pass
        finally:
            skt.clearTimeout()
