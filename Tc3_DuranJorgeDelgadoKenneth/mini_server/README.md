##Mini Server

Instructions to set up a mini server:
- Install all the required dependencies

Running the project:
- Run make 

Run the server
- Run the server using the command: `./bin/server_unsafe 8080` 
- Args:
  - The first argument is the port. You can select another port number if the port is already in use. The server will start listening on the specified port.

Run the client 
- Run the client using the command: `./bin/load_client 127.0.0.1 8080 8 150` 
- Args:
- The first argument is the server IP address. You can select another IP address if the server is running on a different machine.
- The second argument is the server port number where the server is listening.
- The third argument is the number of threads.
- The fourth argument is the number of requests per thread. The client will send a total of (number of threads * number of requests per thread) requests to the server.