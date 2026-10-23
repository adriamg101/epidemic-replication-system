from read_write_lock import ReadWriteLock
from client_socket_manager import ClientSocketManager
from server_socket_manager import EmptySocket
from config_manager import ConfigManager, JSON_KEYS
from threading import Thread, Lock
from protocol import Protocol, EmptySocketError, Frame
from db_manager import DataBaseManager
from utils import Utils
import queue
import time
import select

prot = Protocol()


class NonCoreProcessManager():
    SEC_TO_UPDATE = 10

    def disconnectSockets(self):
        for raw_skt in self.nodes:
            try:
                ClientSocketManager(raw_skt).disconnect()
            except Exception:
                pass

        for raw_skt in self.down_nodes:
            try:
                ClientSocketManager(raw_skt).disconnect()
            except Exception:
                pass

        if self.up is not None and self.up.skt is not None:
            self.up.skt.disconnect()

        if self.us.skt is not None:
            self.us.skt.close()

    def _connectToLayer(self, host: str, port: int, lock: Lock):
        csm = ClientSocketManager()
        while True:
            try:
                csm.connect(host, port)
                prot.send_frame_wait_ack(prot.hello(self.us.id), csm)
                with lock:
                    self.nodes.append(csm.socket)
                self.utils.comment(f"Connected to same-layer node [{host}:{port}].")
                return
            except ConnectionError:
                time.sleep(0.5)

    def _connectToLayerUp(self, host: str, port: int):
        csm = ClientSocketManager()
        while True:
            try:
                csm.connect(host, port)
                prot.send_frame_wait_ack(prot.hello(self.us.id), csm)
                self.up.addSkt(csm)
                self.utils.comment(f"Connected to upper layer [{host}:{port}].")
                return
            except ConnectionError:
                time.sleep(0.5)

    def connectToNodes(self, config: ConfigManager):
        self.us = config.us
        self.nodes = []       # sockets de la mateixa capa (backup per primary, primary per backup)
        self.down_nodes = []  # sockets de la capa inferior (nomes primary)

        lock = Lock()
        ths = []

        # Connexio cap amunt: nomes el primary fa sync cap amunt
        self.up = config.getLayerUpPrimary()
        if self.up is not None and config.weArePrimary():
            th_up = Thread(
                target=self._connectToLayerUp,
                args=(self.up.host, self.up.port)
            )
            th_up.start()
            ths.append(th_up)

        # Iniciem el servidor
        config.us.skt.start_server()

        if config.weArePrimary():
            # Primary: ens conectem al backup
            same_layer = config.getLayer()
            same_layer.remove(config.us)
            for node in same_layer:
                th = Thread(
                    target=self._connectToLayer,
                    args=(node.host, node.port, lock)
                )
                th.start()
                ths.append(th)

            # Esperem que els nodes de la capa inferior es conectin a nosaltres
            down_nodes_info = config.getDownNodes()
            num_conn = len(down_nodes_info)
            while num_conn > 0:
                skt = config.us.skt.accept_client()
                frame = prot.recv_unknown_with_ack(skt, True)
                if frame.type == prot.HELLO:
                    self.down_nodes.append(skt.socket)
                    self.utils.comment(f"Down-layer node connected.")
                    num_conn -= 1
                else:
                    skt.disconnect()
        else:
            # Backup: esperem que el primary es connecti a nosaltres
            while True:
                skt = config.us.skt.accept_client()
                frame = prot.recv_unknown_with_ack(skt, True)
                if frame.type == prot.HELLO:
                    self.nodes.append(skt.socket)
                    self.utils.comment(f"Primary connected to us.")
                    break
                else:
                    skt.disconnect()

        for th in ths:
            th.join()

    def __init__(self,
                 config: ConfigManager,
                 rw_lock: ReadWriteLock,
                 db: DataBaseManager,
                 utils: Utils):
        self.config = config
        self.rw_lock = rw_lock
        self.db = db
        self.utils = utils
        self._backup_queue = queue.Queue()

        self.connectToNodes(config)
        self.is_primary = config.weArePrimary()

    def handleServerSocket(self, lock: Lock):
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

    def processClientWrite(self, frame: Frame):
        if self.is_primary and self.down_nodes:
            target = ClientSocketManager(self.down_nodes[0])
            prot.send_frame_wait_ack(prot.write(frame.key, frame.value), target)
            self.utils.comment(f"Forwarded WRITE [Key:{frame.key}] to down layer.")
        elif not self.is_primary and self.nodes:
            target = ClientSocketManager(self.nodes[0])
            prot.send_frame_wait_ack(prot.write(frame.key, frame.value), target)
            self.utils.comment(f"Forwarded WRITE [Key:{frame.key}] to primary.")

    def _backupSender(self):
        while True:
            version, key, value = self._backup_queue.get()
            data_bytes = prot.data(version, key, value)
            self.utils.comment(
                f"[REPLICATION] Sending key={key} value={value} v={version} to backup(s)...")
            for raw_skt in self.nodes:
                csm = ClientSocketManager(raw_skt)
                try:
                    prot.send_frame_wait_ack(data_bytes, csm)
                    self.utils.comment(
                        f"[REPLICATION] key={key} v={version} delivered to backup.")
                except Exception as e:
                    self.utils.comment(f"[REPLICATION] Error delivering key={key}: {e}")
            self._backup_queue.task_done()

    def processData(self, frame: Frame, from_lower: bool = False):
        with self.rw_lock.w_locked():
            self.db.update(frame.version, frame.key, frame.value)

        if from_lower:
            self.utils.comment(
                f"[PRIMARY] Received DATA from lower layer "
                f"[Key:{frame.key} Value:{frame.value} Version:{frame.version}]. "
                f"Queuing replication to backup(s).")
            self._backup_queue.put((frame.version, frame.key, frame.value))
        else:
            self.utils.comment(
                f"[REPLICA] Received DATA from primary "
                f"[Key:{frame.key} Value:{frame.value} Version:{frame.version}]. "
                f"DB and frontend updated.")

    def syncWithLayer(self):
        with self.rw_lock.w_locked():
            items = self.db.getNonUpwardItems()
            self.db.clearNonUpwardItems()

        if not items:
            return

        self.utils.comment(f"Syncing {len(items)} item(s) to upper layer...")
        for item in items:
            prot.send_frame_wait_ack(
                prot.data(item.version, item.key, item.value), self.up.skt)

    def handleClientSocket(self, lock: Lock, raw_sock):
        csm = ClientSocketManager(raw_sock)
        append = True
        # Si el socket ve de la capa inferior som el primary receptor
        from_lower = raw_sock in self.down_nodes

        while True:
            try:
                frame = prot.recv_unknown_with_ack(csm)

                if frame.type == prot.READ:
                    try:
                        value = self.processClientRead(frame)
                        prot.send_frame_wait_ack(prot.resp(value), csm)
                    except KeyError:
                        prot.send_frame_wait_ack(prot.unknown(), csm)

                elif frame.type == prot.WRITE:
                    self.processClientWrite(frame)

                elif frame.type == prot.DATA:
                    self.processData(frame, from_lower=from_lower)

                elif frame.type == prot.DONE:
                    append = False
                    break

            except (EmptySocketError, BlockingIOError):
                break

        if append:
            with lock:
                self.select_skts.append(raw_sock)
        else:
            csm.disconnect()

    def cmd_read(self, key: int) -> int:
        with self.rw_lock.r_locked():
            return self.db.read(key)

    def cmd_write(self, key: int, value: int):
        frame = Frame(prot.WRITE, key=key, value=value)
        self.processClientWrite(frame)

    def updateUpHandler(self):
        while True:
            time.sleep(self.SEC_TO_UPDATE)
            try:
                self.syncWithLayer()
            except Exception as e:
                self.utils.comment(f"syncWithLayer error: {e}")

    def run(self):
        self.utils.comment("Starting main process...")
        lock = Lock()

        if self.down_nodes:
            # Primary: té connexions de la capa inferior.
            # Els sockets de backup NO van a select_skts: _backupSender els usa en exclusiva.
            self.select_skts = (
                [self.us.skt.server_socket]
                + list(self.down_nodes)
            )
            Thread(target=self._backupSender, daemon=True).start()
        else:
            # Backup: monitora el socket del primary per rebre DATA.
            self.select_skts = (
                [self.us.skt.server_socket]
                + list(self.nodes)
                + list(self.down_nodes)
            )

        # Nomes el primary de L1 fa sync periodic cap a L2
        if self.down_nodes and self.us.layer == JSON_KEYS[1]:
            Thread(target=self.updateUpHandler, daemon=True).start()

        while True:
            with lock:
                check_read = self.select_skts.copy()

            readable, _, _ = select.select(check_read, [], [], 0.2)

            for skt in readable:
                with lock:
                    self.select_skts.remove(skt)

                if skt is self.us.skt.server_socket:
                    Thread(
                        target=self.handleServerSocket,
                        args=(lock,)
                    ).start()
                else:
                    Thread(
                        target=self.handleClientSocket,
                        args=(lock, skt)
                    ).start()
