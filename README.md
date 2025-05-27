## IST-KVS SO 2 PROJ
This project represents the second phase of the Operating Systems course (2024–25) at IST. Its main objective is to enhance the IST-KVS (a key-value store) by allowing remote client processes to interact with it using named pipes (FIFOs). Clients can subscribe to specific keys and receive asynchronous notifications whenever the corresponding values are updated or deleted. To support this, the server has been extended to manage multiple client sessions concurrently, using a multi-threaded architecture and implementing producer-consumer synchronization with mutexes and semaphores. The project also introduces the use of UNIX signals (specifically SIGUSR1) to manage client disconnections: upon receiving this signal, the server forcibly removes all active subscriptions and disconnects the clients gracefully. The implementation is entirely in C, using the POSIX API, and incorporates multithreading (pthreads), FIFO-based IPC, and custom signal handling mechanisms.

## Instructions on how to run both client and server sides

1.- After cloning the repository make sure you (in root) run makefile to proper compile with gcc: 
   ```bash 
make
```

2.- To run the Server, enter in src/server and do ./kvs <jobs_dir> <max_threads> <max_backups> <fifo_register_name> or use the following command:
   ```bash 
./kvs jobs/ 10 10 my_server
```
Here <jobs_dir> is the directory that will contain material that the server will read. 
<max_backups> is the maximum paralel backups allowed. 

<fifo_register_name> is the fifo name that all clients will be connecting to. 

3.- To run any Client, enter in src/client and do  ./client <client_unique_id> <register_pipe_path> or use the following command:
   ```bash 
./client uniqueID my_server
```


## What can i do as a Client?

When connected to IST-KVS, clients can send the following commands via stdin (Check syntax in src/tests):


SUBSCRIBE <key>: Subscribe to updates on a key. The client will be notified whenever the key's value changes or is deleted.

UNSUBSCRIBE <key>: Unsubscribe from updates on a key.

DISCONNECT: Ends the session with the server, removing all subscriptions associated with the client.

DELAY <seconds>: Delays the next command by the specified number of seconds. Useful for testing command timing.


