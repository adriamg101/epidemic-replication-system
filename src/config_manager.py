from client_socket_manager import ClientSocketManager
from pathlib import Path
import argparse
import json

JSON_KEYS = ["core", "l1", "l2"]


class ProcessInfo():
    def __init__(self, id: int, name: str, layer: str, host: str, port: int):
        self.id = id
        self.name = name
        self.layer = layer
        self.host = host
        self.port = port

    def addSkt(self, skt):
        self.skt = skt


class ConfigManager():
    DEFAULT_CONFIG_FILE = str(
        Path(__file__).resolve().parent.parent / "config" / "nodes.json"
    )

    nodes = {}

    def getJsonKeys(self) -> [str]:
        return JSON_KEYS

    def getLayer(self) -> [ProcessInfo]:
        if self.us.layer == JSON_KEYS[0]:
            return self.nodes[self.us.layer]
        else:
            # Mirem si som primary
            if self.us is self.nodes[self.us.layer][0]:
                self.us.is_primary = True
            else:
                self.us.is_primary = False
            return self.nodes[self.us.layer]

    def getLayerUpPrimary(self) -> ProcessInfo | None:
        if self.us.layer == JSON_KEYS[0]:
            return self.nodes[JSON_KEYS[1]][0]
        if self.us.layer == JSON_KEYS[1]:
            return self.nodes[JSON_KEYS[2]][0]
        if self.us.layer == JSON_KEYS[2]:
            return None

    def weArePrimary(self) -> bool:
        return self.us is self.nodes[self.us.layer][0]

    # Sempre es cridara desde l1 o l2
    def getDownNodes(self) -> [ProcessInfo]:
        if self.us.layer == JSON_KEYS[1]:
            return self.nodes[JSON_KEYS[0]]  # Cores
        elif self.us.layer == JSON_KEYS[2]:
            return [self.nodes[JSON_KEYS[1]][0]]  # Primary L1

    def read_config_file(self, config_file: str):
        try:
            with open(config_file, "r") as f:
                nodes = json.load(f)
        except json.JSONDecodeError as e:
            print(f"Syntax error in config file (JSON format): {e}")
            exit()

        # Mirem que existeixin totes les capes
        for key in JSON_KEYS:
            if key not in nodes:
                print(f"Missing key '{key}' in config file.")
                exit()

        # Mirem si hi han capes buides
        for layer in JSON_KEYS:
            layer_content = nodes[layer]
            if not layer_content or len(layer_content) == 0:
                print(f"Layer {layer} without nodes")
                exit()

        # Setejem els ids per el protocol
        id_count = 0
        for layer in JSON_KEYS:
            self.nodes[layer] = []
            for node in nodes[layer]:
                # Guardem les dades del process
                pi = ProcessInfo(
                    id_count, node["name"], layer, node["host"], node["port"])
                if pi.name == self.name:
                    self.us = pi
                self.nodes[layer].append(pi)
                id_count += 1

    def __init__(self):
        parser = argparse.ArgumentParser(
            description="Epidemic replication system"
        )

        parser.add_argument(
            "-n", "--name",
            type=str,
            required=True,
            help="Identifiier of the server."
        )

        parser.add_argument(
            "-c", "--config",
            type=str,
            help="Set the JSON config file. (default: nodes.json)"
        )

        parser.add_argument(
            "-v", "--verbose",
            action="store_true",
            help="Print information about the server."
        )

        args = parser.parse_args()

        self.name = args.name

        config_file = self.DEFAULT_CONFIG_FILE
        if args.config:
            config_file = args.config

        self.verbose = args.verbose

        self.read_config_file(config_file)
