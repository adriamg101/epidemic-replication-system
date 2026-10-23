from client_socket_manager import ClientSocketManager
import socket


class EmptySocket(Exception):
    pass


class ServerSocketManager():
    def __init__(self, host: str = "127.0.0.1", * port: int, backlog: int = 12):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.host = host
        self.port = port[0]
        self.backlog = backlog

    # Bind i listen i control d'errors
    def start_server(self) -> bool:
        try:
            self.server_socket.setsockopt(
                socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(self.backlog)
        except OSError as e:
            print(f"Failed to bind server socket: {e}")

    # Acceptem clients
    def accept_client(self) -> ClientSocketManager:
        client_socket, _ = self.server_socket.accept()
        client_manager = ClientSocketManager(client_socket)
        return client_manager

    def accept_client_non_block(self):
        self.server_socket.setblocking(False)
        try:
            client_socket, _ = self.server_socket.accept()
        except BlockingIOError:
            self.server_socket.setblocking(True)
            raise EmptySocket
        finally:
            self.server_socket.setblocking(True)

        client_manager = ClientSocketManager(client_socket)
        return client_manager

    def close(self):
        self.server_socket.close()
