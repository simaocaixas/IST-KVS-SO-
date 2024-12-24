#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "api.h"
#include "src/common/constants.h"
#include "src/common/protocol.h"
  
char req_pipe_path[MAX_CONNECT_STRING], resp_pipe_path[MAX_CONNECT_STRING], notif_pipe_path[MAX_CONNECT_STRING]; 
int server_fd, req_fd, resp_fd, notif_fd;

int write_to_fd(int fd, const char *str) {
    size_t len = strlen(str);  // pode ser que o str não tenha o '\0' no final n sei 
    ssize_t written = 0;
    size_t total_written = 0;

    while (total_written < len) {
        written = write(fd, str + total_written, len - total_written);
        if (written < 0) {
            return 1;
        }
        total_written += (size_t)written;
    }

    return 0;
}

int kvs_connect(char const* req_pipe_path, char const* resp_pipe_path, char const* server_pipe_path, char const* notif_pipe_path) {
  

  if(mkfifo(req_pipe_path, 0640)) {
    fprintf(stderr, "Failed to create request FIFO\n");
    return 1;
  }

  if(mkfifo(resp_pipe_path, 0640)) {
    fprintf(stderr, "Failed to create response FIFO\n");
    return 1;
  }

  if(mkfifo(notif_pipe_path, 0640)) {
    fprintf(stderr, "Failed to create notification FIFO\n");
    return 1;
  }
  
  char buffer_request[MAX_CONNECT_STRING];

  server_fd = open(server_pipe_path, O_WRONLY);
  if (server_fd < 0) {
    fprintf(stderr, "Failed to open server FIFO\n");
    return 1;
  }

  // Enviar mensagem de connect para o servidor
  snprintf(buffer_request, MAX_CONNECT_STRING, "1|%s|%s|%s", req_pipe_path, resp_pipe_path, notif_pipe_path);

  if(write_to_fd(server_fd, buffer_request) != 0) {
    fprintf(stderr, "Failed to write in FIFO\n");
    close(server_fd);
    return 1;
  }
  close(server_fd); // Fechar o FIFO do servidor

  // Abrir o FIFO de respostas (leitura)
  resp_fd = open(resp_pipe_path, O_RDONLY);
  notif_fd = open(notif_pipe_path, O_RDONLY);
  req_fd = open(req_pipe_path, O_WRONLY);


  if (resp_fd < 0) {
    fprintf(stderr, "Failed to open response FIFO\n");
    close(server_fd);
    return 1;
  }

  char buffer_response[MAX_CONNECT_STRING];

  if(read(resp_fd, buffer_response, 3) == -1) { // a read operation is for sure atomic because 3 bytes < block size (4096 bytes)
    fprintf(stderr, "Failed to open response FIFO\n");
    close(resp_fd);
    return 1;
  }  

  close(resp_fd); // Fechar o FIFO de respostas
  
  if (buffer_response[0] == '1') {
    if (buffer_response[2] == '0') {
      fprintf(stdout, "Server returned 0 for operation: connect"); // Server returned <response-code> for operation: <connect|disconnect|subscribe|unsubscribe>
    } else {
      fprintf(stdout, "Server returned 1 for operation: connect"); // Server returned <response-code> for operation: <connect|disconnect|subscribe|unsubscribe>
      fprintf(stderr, "Could not connect to server!");
      return 1;
    }
  } else {
    fprintf(stderr, "Opcode not recognized for operation: connect");
    return 1;
  }
  
  return 0;
}
 
int kvs_disconnect(void) {
// close pipes and unlink pipe files
  
  req_fd = open(req_pipe_path, O_WRONLY);
  write_to_fd(req_fd, "2"); // send disconnect message to request pipe
  close(req_fd);
  char buffer_response[MAX_CONNECT_STRING];

  resp_fd = open(resp_pipe_path, 0640);
  
  if(read(resp_fd, buffer_response, 3) == -1) { // a read operation is for sure atomic because 3 bytes < block size (4096 bytes)
    fprintf(stderr, "Failed to open response FIFO\n");
    close(resp_fd);
  }  

  close(resp_fd); // Fechar o FIFO de respostas
  
  if (buffer_response[0] == '2') {
    if (buffer_response[2] == '0') {
      fprintf(stdout, "Server returned 0 for operation: disconnect"); // Server returned <response-code> for operation: <connect|disconnect|subscribe|unsubscribe>
    } else {
      fprintf(stdout, "Server returned 1 for operation: disconnect"); // Server returned <response-code> for operation: <connect|disconnect|subscribe|unsubscribe>
      fprintf(stderr, "Could not disconnect to server!");
      return 1;
    }
  } else {
    fprintf(stderr, "Opcode not recognized for operation: disconnect");
    return 1;
  }

  // Unlink the FIFOs
  unlink(req_pipe_path); unlink(resp_pipe_path); unlink(notif_pipe_path);

  return 0;

  /*
    APAGAR SUBSCRICOES DO CLIENTE NAS CHAVES
    APAGAR SUBSCRICOES DO CLIENTE NAS CHAVES 
    APAGAR SUBSCRICOES DO CLIENTE NAS CHAVES
  */
}

int kvs_subscribe(const char* key) {
  // send subscribe message to request pipe and wait for response in response pipe
  char buffer_request[MAX_CONNECT_STRING];
  snprintf(buffer_request, MAX_CONNECT_STRING, "3|%s", key);

  req_fd = open(req_fd, O_WRONLY);
  write_to_fd(req_fd, buffer_request); // send subscribe message to request pipe
  close(req_fd);

  char buffer_response[MAX_CONNECT_STRING];
  resp_fd = open(resp_fd, O_RDONLY);
  if(read(resp_fd, buffer_response, 3) == -1) { // a read operation is for sure atomic because 3 bytes < block size (4096 bytes)
    fprintf(stderr, "Failed to open response FIFO\n");
    close(resp_fd);
    return 1;
  }  

  close(resp_fd); // Fechar o FIFO de respostas
  
  if (buffer_response[0] == '3') {
    if (buffer_response[2] == '1') {
      fprintf(stdout, "Server returned 1 for operation: subscribe"); // Server returned <response-code> for operation: <connect|disconnect|subscribe|unsubscribe>
      
    } else {
      fprintf(stdout, "Server returned 0 for operation: subscribe"); 
      fprintf(stderr, "Could not subscribe to key! (Key not Found)");
      return 1;
    }
  } else {
    fprintf(stderr, "Opcode not recognized for operation: subscribe");
    return 1;
  }

  return 0;
}

int kvs_unsubscribe(const char* key) {
  // send unsubscribe message to request pipe and wait for response in response pipe
  req_fd = open(req_pipe_path, O_WRONLY);

  char buffer_request[MAX_CONNECT_STRING];
  snprintf(buffer_request, MAX_CONNECT_STRING, "4|%s", key);
  if(write_to_fd(req_fd, buffer_request) != 0) {              // send unsubscribe message to request pipe
    fprintf(stderr, "Failed to write to response FIFO\n");
    return 1;
  } 
  
  close(req_fd);

  char buffer_response[MAX_CONNECT_STRING];

  resp_fd = open(resp_pipe_path, O_RDONLY);

  if(read(resp_fd, buffer_response, 3) == -1) { // a read operation is for sure atomic because 3 bytes < block size (4096 bytes)
    fprintf(stderr, "Failed to open response FIFO\n");
    close(req_fd);
    return 1;
  }  
  
  close(resp_fd); // Fechar o FIFO de respostas
  
  if (buffer_response[0] == '4') {
    if (buffer_response[2] == '0') {
      fprintf(stdout, "Server returned 0 for operation: subscribe"); // Server returned <response-code> for operation: <connect|disconnect|subscribe|unsubscribe>
    } else {
      fprintf(stdout, "Server returned 1 for operation: subscribe"); // Server returned <response-code> for operation: <connect|disconnect|subscribe|unsubscribe>
      fprintf(stderr, "Could not subscribe to key! (Key not Found)");
      return 1;
    }
  } else {
    fprintf(stderr, "Opcode not recognized for operation: subscribe");
    return 1;
  }

  return 0;
}

/*
“Server returned <response-code> for operation: <connect|disconnect|subscribe|unsubscribe>
*/
