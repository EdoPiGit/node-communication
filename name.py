from collections import Counter
import socket
import time


import ray

ray.init()

print('''This cluster consists of
    {} nodes in total
    {} CPU resources in total
'''.format(len(ray.nodes()), ray.cluster_resources()['CPU']))

@ray.remote
def f():
    time.sleep(0.001)
   
    # Return name host.
    return socket.gethostname()
    

object_ids = [f.remote() for _ in range(10000)]
hostname = ray.get(object_ids)

print('Tasks executed')
for hostname, num_tasks in Counter(hostname).items():
    print('    {} tasks on {}'.format(num_tasks, hostname))