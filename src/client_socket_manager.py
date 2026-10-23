import socket


class ClientSocketManager():
    def __init__(self, skt: socket.socket = None):
        if skt is None:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        else:
            self.socket = skt

    # Enviem missatge per skt
    def send_message(self, message: bytes):
        self.socket.sendall(message)

    # Recivim missatge pel skt
    def recive_message(self, n: int) -> bytes:
        data = b""
        while len(data) < n:
            chunk = self.socket.recv(n - len(data))
            if not chunk:
                # self.disconnect()
                return b""
            data += chunk
        return data

    # Mirem el type sense treure info del skt
    def peak_type(self) -> int:
        return int.from_bytes(self.socket.recv(2, socket.MSG_PEEK))

    # Setejem timeout del skt
    def setTimeout(self, timeout: int):
        self.socket.settimeout(timeout)

    # Clear del timeout
    def clearTimeout(self):
        self.socket.settimeout(None)

    # Tanquem el skt
    def disconnect(self):
        try:
            self.socket.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.socket.close()
        self.socket = None

    # Ens conectem al skt servidor
    def connect(self, host: str, port: int):
        try:
            self.socket.connect((host, port))
        except ConnectionRefusedError:
            raise ConnectionError
