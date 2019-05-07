from elements import Graph
from manager import PodManager, minimum_total_costManager
import threading
import logging
import json
import logging.config


with open("logging.json") as f:
    config = json.load(f)

logging.config.dictConfig(config)
logger = logging.getLogger(__name__)

G = Graph("1")
G.start_nodes()
G.start()
Podmana = minimum_total_costManager(G)
Podmana.start()
for t in threading.enumerate():
    print(t)
print(G)
