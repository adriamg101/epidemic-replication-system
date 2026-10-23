from config_manager import ConfigManager
from core_process_manager import CoreProcess
from non_core_process_manager import NonCoreProcessManager
from server_socket_manager import ServerSocketManager
from read_write_lock import ReadWriteLock
from db_manager import DataBaseManager
from pathlib import Path
from web_communication_manager import WebCommunicationManager
from utils import Utils
import signal

# Per desactivar els sockets en desconexio
process = None
web = None


def main():
    global process, web

    # Configuracio
    config = ConfigManager()
    utils = Utils(config.verbose, config.name)

    utils.comment("Starting...")

    # Obrim server socket
    config.us.addSkt(ServerSocketManager(config.us.host, config.us.port))
    utils.comment(f"Opened server socket. [{config.us.host}:{config.us.port}]")

    web_port = config.us.port + 10000
    config_path = str(Path(__file__).resolve().parent.parent / "config" / "nodes.json")

    web = WebCommunicationManager(config.us.name, web_port, utils, config_path)
    web.start()
    utils.comment(f"Started web socket. [{web_port}]")

    # Creem directori de logs si no existeix
    BASE_DIR = Path(__file__).resolve().parent.parent
    logs_dir = BASE_DIR / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    db = DataBaseManager(str(logs_dir / (config.us.name + "_log.txt")), web)
    rw_lock = ReadWriteLock()

    # Mire quin tipus de servidor som
    if config.us.layer == config.getJsonKeys()[0]:
        process = CoreProcess(config, rw_lock, db, utils)
    else:
        process = NonCoreProcessManager(config, rw_lock, db, utils)

    web.register_handlers(
        on_read=process.cmd_read,
        on_write=process.cmd_write,
    )

    # Executem el process
    process.run()


def signalHandler(signum, frame):
    global process, web
    if web is not None:
        web.stop()
    if process is not None:
        process.disconnectSockets()
    exit()


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signalHandler)
    main()
