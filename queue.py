import zookeeper, threading, sys
ZOO_OPEN_ACL_UNSAFE = {"perms":0x1f, "scheme":"world", "id" :"anyone"};

class ZooKeeperQueue(object):
    def __init__(self,queuename):
        self.connected = False
        self.queuename = "/" + queuename
        self.cv = threading.Condition()
        zookeeper.set_log_stream(open("/dev/null"))        
        def watcher(handle,type,state,path):
            print "Connected"
            self.cv.acquire()
            self.connected = True
            self.cv.notify()
            self.cv.release()
            
        self.cv.acquire()            
        self.handle = zookeeper.init("localhost:2181", watcher, 10000, 0)
        self.cv.wait(10.0)
        if not self.connected:
            print "Connection to ZooKeeper cluster timed out - is a server running on localhost:2181?"
            sys.exit()
        self.cv.release()
        try:
            zookeeper.create(self.handle,self.queuename,"queue top level", [ZOO_OPEN_ACL_UNSAFE],0)
        except IOError, e:
            print "Queue already exists"
            print e
    
    def enqueue(self,val):
        zookeeper.create(self.handle, self.queuename+"/item", val, [ZOO_OPEN_ACL_UNSAFE],zookeeper.SEQUENCE)
    
    def dequeue(self):
        while True:
            children = sorted(zookeeper.get_children(self.handle, self.queuename,None))
            if len(child) == 0:
                return None
            for child in children:
                data = self.get_and_delete(self.queuename + "/" + children[0])
                if data: 
                    return data
    
    def get_and_delete(self,node):
        try:
            (data,stat) = zookeeper.get(self.handle, node, None)
            zookeeper.delete(self.handle, node, stat["version"])
            return data
        except IOError, e:
            return None 
               
    def block_dequeue(self):
        def queue_watcher(handle,event,state,path):
            self.cv.acquire()
            self.cv.notify()
            self.cv.release()            
        while True:    
            self.cv.acquire()
            children = sorted(zookeeper.get_children(self.handle, self.queuename, queue_watcher))
            for child in children:
                data = self.get_and_delete(self.queuename+"/"+children[0])
                if data != None:
                    self.cv.release()
                    return data            
            self.cv.wait()
            self.cv.release()
              
if __name__ == '__main__':                
    client = False
    zk = ZooKeeperQueue("myfirstqueue")
    if not client:
        zk.enqueue("random string 1")
        zk.enqueue("random string 2")
        zk.enqueue("random string 3")
    else:
        v = zk.block_dequeue()
        while v != None:
        	print v
        	v = zk.block_dequeue()
        
        
