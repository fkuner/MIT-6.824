课程链接：http://nil.csail.mit.edu/6.824/2020/

GitHub链接：https://github.com/fkuner/6.824

Raft：https://raft.github.io/

## Lecture
### Lecture1 MapReduce
### Lecture2 RPC and Threads
1. I/O Concurrency
2. Parallelism
3. Convenience

race、coordination

web crawler execrise
### Lecture3 GFS

Master Data:
1. filename -> array of chunk handles
2. handle ->list of chunk servers
   ->version
   ->primary
   ->lease expiration
3. log, checkpoint

READ：
1. name, offset-master
2. M sends the chunk handle, list of servers which is cached by the client
3. client- CS and CS return data to the client

WRITE:

NO PRIMARY -> find up-to-date replicas -> pick primary, secondary -> increment v -> tells p, s -> master writes VA to disk

### Lecture4 Primary-Backup Replication

Failures:
1. fail-stop faults √
2. bugs ×

State transfer (memory)
Replicated State Machine (operation)

Non-def events:

1. Inputs-packet-data + interrupt
2. Weird instructions
3. Multicore

Log entry: instruction #, type, data

### Lecture5 Go Threads and Raft

condition variables:
```
mu.Lock()
// do something that might affect the condition
cond.Braodcast()
mu.Unlock()

---

mu.Lock()
while condition == false {
	cond.Wait()
}
// now condition is true, and we have the lock
mu.Unlock()
```
死锁：
```
s0.CallRequestVote, acquire the lock
s0.CallRequestVote, send RPC to s1
s1.CallRequestVote, acquire the lock
s1.CallRequestVote, send RPC to s0
s0.Handler, s1.Handler trying to acquire lock
```

### Lecture6 Fault Tolerance Raft (1)

Leader -> 高效

ELECTION TIMER -> START ELECTION

term++, requestVote

### Lecture7 Fault Tolerance Raft (2)
linearizable system ? ? ?
### Lecture8 ZooKeeper

WHY ZK?
1. API general-purpose coord service, not library as raft
2. N Servers can get N performance?

ZK GUARANTEES
1. Linearizable writes: all requests that update the state of ZooKeeper are serializable and respect precedence;
2. FIFO client order: all requests from a given client are executed in the order that they were sent by the client.

底层基于ZAB

### Lecture9 More Replication CRAQ
Chain Replication
### Lecture10 Cloud Replicated DB Aurora
EC2、EBS、RDS

Quorum
## Papers
### MapReduce: Simplified Data Processing on Large Clusters
![](./images/mapreduce.png)
### The Google File System
### The Design of a Practical System for Fault-Tolerant Virtual Machines

通过主从复制，在不同的物理服务器上运行一个备份虚拟机，通过和主机执行相同的操作来保持同步。备份虚拟机被建模为确定性状态机，主从都执行相同的动作，对于不确定性操作，还必须记录事件发生的确切指令。primary VM 接收的所有输入都会通过logging channel传输给backup VM，backup VM根据日志信息进行replay，因此可以和primary VM有相同的执行，从而达到备份的目的。
### In Search of an Understandable Consensus Algorithm (Extended Version)
Properties:
- **Election Safety**: at most one leader can be elected in a given term.
- **Leader Append-Only**: a leader never overwrites or deletes entries in its log; it only appends new entries.
- **Log Matching**: if two logs contain an entry with the same index and term, then the logs are identical in all entries up through the given index.
- **Leader Completeness**: if a log entry is committed in a given term, then that entry will be present in the logs of the leaders for all higher-numbered terms.
- **State Machine Safety**: if a server has applied a log entry at a given index to its state machine, no other server will ever apply a different log entry for the same index.

### ZooKeeper: Wait-free coordination for Internet-scale systems
the abstraction of a set of data nodes (znodes), organized according to a hierarchical name space.

znode types:
1. Regular: Clients manipulate regular znodes by creating and deleting them explicitly;
2. Clients create such znodes, and they either delete them explicitly, or let the system remove them automatically when the session that creates them terminates (deliberately or due to a failure).

Client API:
```
create(path, data, flags)
delete(path, version)
exists(path, watch)
getData(path, watch)
setData(path, data, version)
getChildren(path, watch)
sync(path)
```

**ZooKeeper guarantees**
1. Linearizable writes: all requests that update the state of ZooKeeper are serializable and respect prece-
   dence
2. FIFO client order: all requests from a given client are executed in the order that they were sent by the
   client.

**ZooKeeper Implementation**

1. Request Processor
2. Atomic Broadcast
3. Replicated Database
4. Client-Server Interactions

### Object Storage on CRAQ
基于对象存储的系统提供两个原语：
1. write(objID, V): The write (update) operation stores the value V associated with object identifier objID
2. V := read(objID): The read (query) operation retrieves the value V associated with object id objID.

普通的Chain Replication（CR）维护一个链表，头结点处理写请求，尾结点处理读请求，头结点收到写请求后，会传播到下一个结点，以此类推，直到到达尾节点，此时该操作为committed，所有只有committed的值才能被读到，因此保证了强一致性

CR存在一个问题，只能在尾结点读，吞吐性不高

CRAQ相比于CR拓展了：
1. 每一个结点包含一个自增的版本号和额外的属性，指示版本号是clean还是dirty，所有版本号属性初始为clean
2. 当一个结点收到一个对象的新版本号（写操作）时，插入最新的版本号到列表中，如果该结点不是尾结点，标记这个版本号为dirty，然后向后传播；否则，标记为clean，这时这个写操作为committed，然后尾结点发送ACK给所有之前的结点
3. 当一个结点收到一个对象的ACK，该结点标记这个对象的版本号为clean，并且删除所有之前的版本号
4. 当一个结点收到一个对象的读请求时，如果对象的最新版本号是clean, 返回值；否则，该结点向尾结点请求last committed版本号，并且返回这个版本号对应的值

在**Read-Mostly Workloads**和**Write-Heavy Workloads**场景都能提供比CR更高的吞吐

CRAQ提供了3种一致性：
1. Strong Consistency
2. Eventual Consistency
3. Eventual Consistency with Maximum-Bounded Inconsistency

CRAQ采取跟CR一样的容错

除此之外，为了提升效率，Node再给后续的节点发送消息时可以用多播协议，来并发处理，缩短了线性处理的时间

### Amazon Aurora: Design Considerations for High Throughput Cloud-Native Relational Databases
为了减少网络压力，提升数据库性能，Aurora将redo log下推到分布式存储层，采用Quorum协议，提供可扩展的高可用存储
## Lab
Lab1、Lab2、Lab3代码在GitHub上，Lab4肝不动了，有机会再写吧！