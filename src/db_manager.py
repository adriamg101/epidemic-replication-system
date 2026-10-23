from datetime import datetime
from web_communication_manager import WebCommunicationManager


class DataItem():
    def __init__(self, key: int, value: int, version: int):
        self.key = key
        self.value = value
        self.version = version


class DataBaseManager():
    DATE_FORMAT = "%d/%m/%Y %H:%M:%S.%f"

    def __init__(self, file_name: str, web: WebCommunicationManager):
        self.db = {}
        self.upward_data = []
        self.logs = open(file_name, "w", buffering=1)
        self.web = web

    def read(self, key: int) -> int:
        return self.db[key].value

    def write(self, key: int, value: int) -> int:
        if key not in self.db:
            self.db[key] = DataItem(key, value, 0)
        else:
            self.db[key].value = value
            self.db[key].version += 1

        # Ho comuniquem a la web
        self.web.send_data(key, self.db[key].version, value)
        self.upward_data.append(DataItem(key, value, self.db[key].version))

        ts = datetime.now().strftime(self.DATE_FORMAT)
        self.logs.write(
            f"[{ts}] version={self.db[key].version} key={key} value={value}\n")

        return self.db[key].version

    def update(self, version: int, key: int, value: int) -> int:
        if key not in self.db:
            self.db[key] = DataItem(key, value, version)
        else:
            if self.db[key].version > version:
                return

            self.db[key].value = value
            self.db[key].version = version

        # Ho comuniquem a la web
        self.web.send_data(key, version, value)
        self.upward_data.append(DataItem(key, value, version))

        ts = datetime.now().strftime(self.DATE_FORMAT)
        self.logs.write(f"[{ts}] version={version} key={key} value={value}\n")

        return self.db[key].version

    def getNonUpwardItems(self) -> [DataItem]:
        return self.upward_data.copy()

    def clearNonUpwardItems(self):
        self.upward_data.clear()
