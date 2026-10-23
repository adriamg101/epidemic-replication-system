from protocol import Protocol, Frame

prot = Protocol()


class MutexClock:
    def __init__(self, process_id: int, peer_ids: [int], skt):
        self.process_id = process_id
        self.peer_ids = peer_ids
        self.skt = skt

        self.clock = 0

    def _tick(self):
        self.clock += 1
        return self.clock

    def _updateClock(self, received_ts: int):
        self.clock = max(self.clock, received_ts) + 1

    def requestCS(self):
        pass

    def canEnterCS(self):
        pass

    def releaseCS(self):
        pass

    def handleFrame(self, frame: Frame):
        pass
