import zmq

context = zmq.Context()

xsub = context.socket(zmq.XSUB)
xsub.bind("tcp://*:5557")

xpub = context.socket(zmq.XPUB)
xpub.bind("tcp://*:5558")

print("[PUBSUB PROXY] Iniciado XSUB=5557 XPUB=5558", flush=True)
zmq.proxy(xsub, xpub)