## coding -utf8
import threading
import time
import random
lock_list = [threading.RLock() for i in range(2)]
class test1:
    def run(self, p1, t=None):
        name = threading.current_thread().name
        if not t:
            t = random.randint(2, 10)
        #print(name, threading.get_ident())
        # print(threading.current_thread().name+"sleep for", t)
        len_lock = len(lock_list)
        count = random.randint(0, len_lock-1)
        while True:
            result = lock_list[count].acquire(timeout=2)
            if result:
                print(name, "success lock:", count)
                break
            else:
                print(name, "fail lock:", count)
                count += 1
                count %= len_lock
        time.sleep(t)
        lock_list[count].release()
        print(threading.current_thread().name+"__end")

class subthread(threading.Thread):
    def run(self):
        print(self.name)
        super().run()

t1_obj = test1()

thread_box = []
for i in range(4):
    th = threading.Thread(target=t1_obj.run, args=(5, 5))
    thread_box.append(th)
flag = True
# for th in thread_box:
#     if flag:
#         th.setDaemon(True)
#         flag = True
#     else:
#         flag = True

for th in thread_box:
    th.start()
time.sleep(3)
print(threading.current_thread().name, threading.get_ident())
print(threading.current_thread().name, 'endddddddd')