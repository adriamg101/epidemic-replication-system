import threading
from protocol import Protocol, Frame

prot = Protocol()


class RicartAgrawalaMutex:
    def __init__(self, process_id: int, id_skt: dict):
        self.process_id = process_id
        self.id_skt = id_skt
        self.num_peers = len(id_skt.items())

        self.clock = 0

        self.requesting_cs = False
        self.request_ts = None

        self.replies_received = set()
        self.deferred_replies = set()

        self.lock = threading.Lock()
        self.cs_enter_event = threading.Event()

    def _tick(self):
        self.clock += 1
        return self.clock

    def _updateClock(self, received_ts: int):
        with self.lock:
            self.clock = max(self.clock, received_ts) + 1

    def requestCS(self):
        with self.lock:
            self.requesting_cs = True
            self.request_ts = self._tick()
            self.replies_received.clear()
            self.cs_enter_event.clear()

            for pid, skt in self.id_skt.items():
                if pid != self.process_id:
                    skt.send_message(prot.request(
                        self.process_id, self.request_ts))

        self.cs_enter_event.wait()

    def _canEnterCS(self):
        return len(self.replies_received) == self.num_peers

    def releaseCS(self):
        with self.lock:
            self.requesting_cs = False

            for pid in self.deferred_replies:
                skt = self.id_skt.get(pid)
                if skt:
                    ts = self._tick()
                    skt.send_message(prot.reply(self.process_id, ts))

            self.deferred_replies.clear()

    def handleFrame(self, frame: Frame, skt):
        self._updateClock(frame.timestamp)

        if frame.type == prot.REQUEST:
            sender = frame.origin
            ts_j = frame.timestamp

            send_reply = False

            with self.lock:
                if not self.requesting_cs:
                    send_reply = True
                else:
                    if (ts_j < self.request_ts or
                            (ts_j == self.request_ts and
                             sender < self.process_id)):
                        send_reply = True

                if send_reply:
                    skt_peer = self.id_skt.get(sender)
                    if skt_peer:
                        ts = self._tick()
                        skt_peer.send_message(prot.reply(
                            self.process_id, ts))
                else:
                    self.deferred_replies.add(sender)

        elif frame.type == prot.REPLY:
            with self.lock:
                self.replies_received.add(frame.origin)
                if self._canEnterCS():
                    self.cs_enter_event.set()
