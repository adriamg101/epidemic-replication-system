from client_socket_manager import ClientSocketManager
from protocol import Protocol
import argparse
import json
import re
import random

prot = Protocol()


class Command:
    BEGIN = "BEGIN"
    READ = "READ"
    WRITE = "WRITE"
    CLOSE = "CLOSE"

    def __init__(self,
                 type_: int,
                 layer: int | None = None,
                 key: int | None = None,
                 value: int | None = None):
        self.type = type_
        self.layer = layer
        self.key = key
        self.value = value


class ProcessInfo():
    def __init__(self, id: int, host: str, port: int):
        self.id = id
        self.host = host
        self.port = port


class Client:
    LAYERS = ["core", "l1", "l2"]

    def __init__(self, config_path: str, file_path: str):
        self.config_path = config_path
        self.file_path = file_path
        self.config = None
        self.commands: list[Command] = []

        self._load_config()
        self._load_commands()

    def _load_config(self):
        with open(self.config_path, "r") as f:
            config = json.load(f)

        id = 0
        self.nodes = {}
        for i in range(len(self.LAYERS)):
            nodes = config[self.LAYERS[i]]
            self.nodes[i] = []
            for node in nodes:
                self.nodes[i].append(ProcessInfo(
                    id, node["host"], node["port"]))
                id += 1

    def _load_commands(self):
        with open(self.file_path, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                parts = line.split(", ")
                for part in parts:
                    self.commands.append(self._parse_command(part))

    def _parse_command(self, cmd: str) -> Command:
        if cmd == "b":
            return Command(Command.BEGIN, layer=0)

        m = re.fullmatch(r"b([0-2])", cmd)
        if m:
            return Command(Command.BEGIN, layer=int(m.group(1)))

        m = re.fullmatch(r"r\((\d+)\)", cmd)
        if m:
            return Command(Command.READ, key=int(m.group(1)))

        m = re.fullmatch(r"w\((\d+),(\d+)\)", cmd)
        if m:
            return Command(
                Command.WRITE,
                key=int(m.group(1)),
                value=int(m.group(2))
            )

        if cmd == "c":
            return Command(Command.CLOSE)

        raise ValueError(cmd)

    def handleBegin(self, command: Command):
        # Nomes ens conectema un servidor random de la layer
        node = random.choice(self.nodes[command.layer])
        self.skt = ClientSocketManager()
        self.skt.connect(node.host, node.port)

    def handleRead(self, command: Command):
        # Enviem read i mostrem el que es retorni
        print(f"Reading from key {command.key}.")
        prot.send_frame_wait_ack(prot.read(command.key), self.skt)

        # Esperem resposta
        frame = prot.recv_unknown_with_ack(self.skt, True)
        if frame.type == prot.RESP:
            # Hi ha la data
            print(f"Key {command.key}: {frame.value}.")
        else:
            # Hem rebut unknown
            print(f"Key {command.key}: not set yet.")

    def handleWrite(self, command: Command):
        # Enviem write i esperem ack
        print(f"Writing value {command.value} to key {command.key}")
        prot.send_frame_wait_ack(prot.write(
            command.key, command.value), self.skt)

    def handleClose(self):
        # Ens desconectem del servidor
        prot.send_final_frame(prot.done(), self.skt)
        self.skt.disconnect()

    def run(self):
        for command in self.commands:
            # Quatre tipus de comandes
            # BEGIN = "BEGIN"
            # READ = "READ"
            # WRITE = "WRITE"
            # CLOSE = "CLOSE"

            try:
                if command.type == command.BEGIN:
                    # Ens conectem al servidor que toca
                    self.handleBegin(command)
                elif command.type == command.READ:
                    self.handleRead(command)
                elif command.type == command.WRITE:
                    self.handleWrite(command)
                elif command.type == command.CLOSE:
                    self.handleClose()
            except ConnectionResetError:
                continue


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-c", "--config",
        default="nodes.json",
        type=str,
        help="Config file (default: nodes.json)"
    )

    parser.add_argument(
        "-f", "--file",
        required=True,
        type=str,
        help="Command file"
    )

    return parser.parse_args()


def main():
    args = parse_args()
    Client(args.config, args.file).run()


if __name__ == "__main__":
    main()
