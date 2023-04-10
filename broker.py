import zmq 

import time 
from log import logger

class ZMQBroker:
    def __init__(self):
        pass 

    def run(self):
        router_socket = self.ctx.socket(zmq.ROUTER)
        router_socket.bind("tcp://*:9090")

        keep_loop = True 
        logger.info("Starting broker")
        while keep_loop:
            try:
                incoming_events = router_socket.poll(timeout=100)
                if incoming_events == zmq.POLLIN:
                    client_id, _, message = router_socket.recv_multipart()
                    logger.info(f"Received message {message}")
                    time.sleep(5)
                    router_socket.send_multipart([client_id, b'', b"Hello from broker"])
            except KeyboardInterrupt:
                keep_loop = False
            except Exception as e:
                logger.error(f"Error: {e}")

        logger.info("Shutting down broker")
        router_socket.close(linger=0)

    def __enter__(self):
        self.ctx = zmq.Context()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.ctx.term()
        logger.info("Context terminated")

