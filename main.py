
import multiprocessing as mp

from server import APIServer
from broker import ZMQBroker

def start_server():
    with APIServer(host='0.0.0.0', port=8000, broker_address='tcp://localhost:9090') as server:
        server.start_service()

def start_broker():
    with ZMQBroker() as broker:
        broker.run()

if __name__ == '__main__':
    server_ = mp.Process(target=start_server)
    broker_ = mp.Process(target=start_broker)

    server_.start()
    broker_.start()

    try:
        server_.join()
        broker_.join()
    except KeyboardInterrupt:
        server_.join()
        broker_.join()