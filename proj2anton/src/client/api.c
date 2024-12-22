#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "api.h"
#include "src/common/constants.h"
#include "src/common/protocol.h"

int kvs_connect(char const* req_pipe_path, char const* resp_pipe_path, char const* server_pipe_path,
                char const* notif_pipe_path) {
  
  if(mkfifo(req_pipe_path, 0640) != 0 || mkfifo(resp_pipe_path, 0640) != 0 || mkfifo(notif_pipe_path, 0640) != 0) {
    fprintf(stderr, "Failed to create FIFO\n");
    return 1;
  }

  // Abrir o FIFO de pedidos (escrita) para comunicação com o servidor
    int req_fd = open(req_pipe_path, O_WRONLY);
    if (req_fd < 0) {
        fprintf(stderr, "Failed to open request FIFO\n");
        return 1;
    }

    // Abrir o FIFO de respostas (leitura)
    int resp_fd = open(resp_pipe_path, O_RDONLY);
    if (resp_fd < 0) {
        fprintf(stderr, "Failed to open response FIFO\n");
        close(req_fd); // Fechar o FIFO de pedidos caso haja erro
        return 1;
    }

    // Abrir o FIFO de notificações (leitura)
    int notif_fd = open(notif_pipe_path, O_RDONLY);
    if (notif_fd < 0) {
        fprintf(stderr, "Failed to open notification FIFO\n");
        close(req_fd);
        close(resp_fd);
        return 1;
    }

    // Aqui, você pode se conectar ao pipe do servidor (escrita)
    int server_fd = open(server_pipe_path, O_WRONLY);
    if (server_fd < 0) {
        fprintf(stderr, "Failed to open server FIFO\n");
        close(req_fd);
        close(resp_fd);
        close(notif_fd);
        return 1;
    }

  return 0;
}
 
int kvs_disconnect(void) {
  // close pipes and unlink pipe files
  return 0;
}

int kvs_subscribe(const char* key) {
  // send subscribe message to request pipe and wait for response in response pipe
  return 0;
}

int kvs_unsubscribe(const char* key) {
    // send unsubscribe message to request pipe and wait for response in response pipe
  return 0;
}


