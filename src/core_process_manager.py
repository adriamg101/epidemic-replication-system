from read_write_lock import ReadWriteLock
from ricart_agrawala_clock import RicartAgrawalaMutex
from client_socket_manager import ClientSocketManager
from server_socket_manager import EmptySocket
from config_manager import ConfigManager, ProcessInfo
from threading import Thread, Lock
from protocol import Protocol, EmptySocketError, Frame
from db_manager import DataBaseManager
from utils import Utils
import time
import select

prot = Protocol()

class CoreProcess():
    # Diccionari id:skt dels core
    up: ProcessInfo
    nodes = {}
    ITEMS_TO_UPDATE = 10

    def disconnectSockets(self):
        for node in self.nodes:
            if node.skt is not None:
                self.config.comment(f"Disconnecting from node {node.host}:{node.port}")
                node.skt.disconnect()
                node.skt = None
        if self.us.skt is not None:
            self.us.skt.disconnect()

    def _connectToCore(self, id: int, host: str, port: int, lock: Lock):
        csm = ClientSocketManager()

        while True:
            try:
                csm.connect(host, port)
                prot.send_frame_wait_ack(prot.hello(self.us.id), csm)
                with lock:
                    self.nodes[id] = csm
                self.utils.comment(f"Connected to core [{host}:{port}].")
                return
            except ConnectionError:
                time.sleep(0.5)

    def _connectToLayerUp(self, host: str, port: int):
        csm = ClientSocketManager()

        while True:
            try:
                csm.connect(self.up.host, self.up.port)
                prot.send_frame_wait_ack(prot.hello(self.us.id), csm)
                self.up.skt = csm
                self.utils.comment(f"Connected to layer up [{host}:{port}].")
                return
            except ConnectionError:
                time.sleep(0.5)

    def connectToNodes(self, config: ConfigManager):
        nodes = config.getLayer()
        nodes.remove(config.us)
        num_conn = len(nodes)

        self.us = config.us

        lock = Lock()
        ths = []
        for node in nodes:
            if self.us.id < node.id:
                # Ens conectem amb thread
                th = Thread(
                    target=self._connectToCore,
                    args=(node.id, node.host, node.port, lock,)
                )
                th.start()
                ths.append(th)
                num_conn -= 1

        # Si ens hem de conectar amb layer up del primary
        self.up = config.getLayerUpPrimary()
        self.utils.comment(f"Connecting to layer up.")
        th_up = Thread(
            target=self._connectToLayerUp,
            args=(self.up.host, self.up.port,),
        )
        th_up.start()
        ths.append(th_up)

        config.us.skt.start_server()
        while num_conn > 0:
            # Esperem a client
            skt = config.us.skt.accept_client()

            # Escoltem el HELLO
            frame = prot.recv_unknown_with_ack(skt, True)
            if frame.type != prot.HELLO:
                # Desconectem
                skt.disconnect()
            else:
                # Busquem el node i l'afegim al diccionari
                for node in nodes:
                    if node.id == frame.origin:
                        with lock:
                            self.nodes[node.id] = skt
                        num_conn -= 1
                        self.utils.comment(f"Added connection from [{node.host}:{node.port}]")
                        break

        # Esperem que acabin els threads
        for th in ths:
            th.join()

    def __init__(self, config: ConfigManager, rw_lock: ReadWriteLock, db: DataBaseManager, utils: Utils):
        self.config = config
        self.utils = utils

        # Conectem els core
        self.connectToNodes(config)

        # Setejem Ricart-Agrawala
        self.ra_clock = RicartAgrawalaMutex(config.us.id, self.nodes)

        self.rw_lock = rw_lock
        self.db = db

    def handleServerSocket(self, lock: Lock):
        # En aquest cas el que fem es acceptar al client
        while True:
            try:
                client = self.us.skt.accept_client_non_block()
                with lock:
                    self.select_skts.append(client.socket)
                self.utils.comment("Client connected.")
            except EmptySocket:
                break

        with lock:
            self.select_skts.append(self.us.skt.server_socket)

    def processClientRead(self, frame: Frame) -> int:
        with self.rw_lock.r_locked():
            value = self.db.read(frame.key)
        self.utils.comment(f"Client read key {frame.key} → value {value}.")
        return value

    def syncWithLayer(self):
        # Llista de elements no actualitzats
        with self.rw_lock.w_locked():
            items = self.db.getNonUpwardItems()
            self.db.clearNonUpwardItems()

        self.utils.comment("Updating upper layer...")

        for item in items:
            # Enviem la data per el skt
            prot.send_frame_wait_ack(
                prot.data(item.version, item.key, item.value), self.up.skt)

    def processClientWrite(self, frame: Frame):
        # Primer entrem en la CS
        self.ra_clock.requestCS()

        # Primer de tot escribim en db
        with self.rw_lock.w_locked():
            version = self.db.write(frame.key, frame.value)
        self.utils.comment(f"User write [Key: {frame.key}, Value: {frame.value}]. (new version: {version})")

        # Avisem a tots de que s'ha actualitzat (send_message: el select loop
        # del peer gestiona la recepció; send_frame_wait_ack competiria amb
        # el select loop pel mateix socket i causaria deadlock)
        self.utils.comment(f"Broadcasting update to L0. (key: {frame.key})")
        for skt in self.nodes.values():
            skt.send_message(prot.data(version, frame.key, frame.value))

        # Mirem si hem de sincronitzar
        self.utils.comment(f"Items updated: {len(self.db.getNonUpwardItems())}.")
        if (len(self.db.getNonUpwardItems()) >= self.ITEMS_TO_UPDATE):
            self.utils.comment(f"Limit reach, starting syncronization uppwards protocol...")

            # Avisem a tots els core que farem sync
            for skt in self.nodes.values():
                skt.send_message(prot.sync())

            # Sincronitzem cap a L1 (self.up.skt no esta en select_skts: OK)
            self.syncWithLayer()

        # Alliberem CS
        self.ra_clock.releaseCS()

    def processData(self, frame: Frame):
        # Hi hagiga el que hi hagiga actualitzem la dada si es que no tenim una versio mes nova
        with self.rw_lock.w_locked():
            self.db.update(frame.version, frame.key, frame.value)
        self.utils.comment(f"Recived data from node [Key: {frame.key}, Value: {frame.value}]. (Version: {frame.version})")

    def processSync(self):
        self.db.clearNonUpwardItems()
        self.utils.comment("Recived sync, clear non upward items list.")

    def handleClientSocket(self, lock: Lock, sock):
        skt = ClientSocketManager(sock)
        append = True

        # Mirem el type del missatge
        while True:
            try:
                # Mirem la trama que es
                frame = prot.recv_unknown_with_ack(skt)

                # Podem rebre:
                # Cores: REQUEST, REPLY, RELEASE, DATA i SYNC
                # Clients: READ i WRITE
                if (frame.type == prot.REQUEST or
                        frame.type == prot.REPLY or
                        frame.type == prot.RELEASE):
                    # Ricart-Agrawala stuff
                    self.ra_clock.handleFrame(frame, skt)
                elif frame.type == prot.READ:
                    # Read
                    try:
                        value = self.processClientRead(frame)
                        prot.send_frame_wait_ack(prot.resp(value), skt)
                    except KeyError:
                        prot.send_frame_wait_ack(prot.unknown(), skt)
                elif frame.type == prot.WRITE:
                    # Write
                    self.processClientWrite(frame)
                elif frame.type == prot.DATA:
                    # DATA
                    self.processData(frame)
                elif frame.type == prot.SYNC:
                    # SYNC
                    self.processSync()
                elif frame.type == prot.DONE:
                    append = False
                    break
                # No hi ha else per en un futur afegir mes handlers

            except (EmptySocketError, BlockingIOError):
                break

        # Si es un socket dels core tornem a afegir el skt
        if append:
            with lock:
                self.select_skts.append(sock)
        else:
            # Es un client que s'ha desconectat
            skt.disconnect()

    def cmd_read(self, key: int) -> int:
        with self.rw_lock.r_locked():
            return self.db.read(key)

    def cmd_write(self, key: int, value: int):
        frame = Frame(prot.WRITE, key=key, value=value)
        self.processClientWrite(frame)

    def run(self):
        self.utils.comment("Starting main process...")
        lock = Lock()  # Per llegir/escriure en select_skts
        self.select_skts = []

        # Afegim els skts dels core, incloos el de servidor
        self.select_skts.append(self.us.skt.server_socket)
        for node in self.nodes.values():
            self.select_skts.append(node.socket)

        # Inicialitzem els sockets a mirar en el select
        while True:
            with lock:
                check_read = self.select_skts.copy()

            # Fem el select
            readable, _, _ = select.select(check_read, [], [], 0.2)

            # Comprobem cada socket i fem threads
            for skt in readable:
                with lock:
                    self.select_skts.remove(skt)
                if skt is self.us.skt.server_socket:
                    # Server socket
                    Thread(
                        target=self.handleServerSocket,
                        args=(lock,)
                    ).start()
                else:
                    # Core o client
                    Thread(
                        target=self.handleClientSocket,
                        args=(lock, skt)
                    ).start()
