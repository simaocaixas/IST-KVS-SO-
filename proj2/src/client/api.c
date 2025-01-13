#include "api.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "src/common/constants.h"
#include "src/common/protocol.h"

static const char* _req_pipe_path;
static const char* _resp_pipe_path;
static const char* _notif_pipe_path;
int _server_fd, _req_fd, _resp_fd, _notif_fd;

// Retorna descritor do fifo de notificações
int* get_notify_fd() { return &_notif_fd; }

int write_to_fd(int fd, const char *str) {
    size_t len = strlen(str) + 1;  
    ssize_t written = 0;
    size_t total_written = 0;

    // Continua a escrever até que toda a string seja enviada
    while (total_written < len) {
        written = write(fd, str + total_written, len - total_written);
        
        // Se houve erro na escrita (written < 0), retorna erro
        if (written < 0) {
            return 1;
        }
        total_written += (size_t)written;
    }

    // Retorna 0 para sucesso
    return 0;
}

/**
 * Estabelece uma conexão com o servidor KVS através de FIFOs (named pipes).
 * 
 * Esta função cria os FIFOs necessários para a comunicação cliente-servidor,
 * estabelece a conexão com o servidor e configura os canais de comunicação.
 * 
 * @param req_pipe_path    Caminho para o FIFO de pedidos
 * @param resp_pipe_path   Caminho para o FIFO de respostas
 * @param server_pipe_path Caminho para o FIFO do servidor
 * @param notif_pipe_path  Caminho para o FIFO de notificações
 * 
 * @return int Retorna 0 em caso de sucesso, 1 em caso de erro
 * 
 * @errors
 *    - Falha ao remover FIFOs existentes
 *    - Falha ao criar novos FIFOs
 *    - Falha ao abrir o FIFO do servidor
 *    - Falha ao escrever no FIFO
 *    - Falha ao abrir FIFOs de resposta ou notificação
 *    - Erro na resposta do servidor
 */
int kvs_connect(char const* req_pipe_path, char const* resp_pipe_path, char const* server_pipe_path,
                char const* notif_pipe_path) {

  if (unlink(req_pipe_path) != 0 && errno != ENOENT) {
    // Falha ao remover o FIFO de pedidos. Retorna erro.
    fprintf(stderr, "Failed to unlink request FIFO!\n");
    return 1;
  }

  if (unlink(resp_pipe_path) != 0 && errno != ENOENT) {
    // Falha ao remover o FIFO de respostas. Retorna erro.
    fprintf(stderr, "Failed to unlink response FIFO!\n");
    return 1;
  }

  if (unlink(notif_pipe_path) != 0 && errno != ENOENT) {
    // Falha ao remover o FIFO de notificações. Retorna erro.
    fprintf(stderr, "Failed to unlink notification FIFO!\n");
    return 1;
  }

  // Criação do FIFO para pedidos.
  if (mkfifo(req_pipe_path, 0640) == -1) {
    fprintf(stderr, "Failed to create request FIFO!\n");
    return 1;
  }

  // Criação do FIFO para respostas.
  if (mkfifo(resp_pipe_path, 0640) == -1) {
    fprintf(stderr, "Failed to create response FIFO\n");
    return 1;
  }

  // Criação do FIFO para notificações.
  if (mkfifo(notif_pipe_path, 0640) == -1) {
    fprintf(stderr, "Failed to create notification FIFO\n");
    return 1;
  }

  char buffer_request[MAX_CONNECT_STRING];

  // Abre o FIFO do servidor para escrita. Caso não abra, retorna erro e destrói os respetivos fifos
  _server_fd = open(server_pipe_path, O_WRONLY);
  if (_server_fd < 0) {
    fprintf(stderr, "Failed to open server FIFO\n");
    if (unlink(req_pipe_path) != 0) {
      perror("Failed to unlink req_pipe_path");
    }
    if (unlink(resp_pipe_path) != 0) {
      perror("Failed to unlink resp_pipe_path");
    }
    if (unlink(notif_pipe_path) != 0) {
      perror("Failed to unlink notif_pipe_path");
    }
    return 1;
  }
  
  const char* req_name = req_pipe_path + 5;  
  const char* resp_name = resp_pipe_path + 5;
  const char* notif_name = notif_pipe_path + 5;
  
  // Enviar mensagem de conexão para o servidor.
  snprintf(buffer_request, MAX_CONNECT_STRING, "%d|%s|%s|%s", OP_CODE_CONNECT, req_name, resp_name, notif_name);

  // Caso não consiga escrever, retorna erro e destrói os respetivos fifos
  if (write_to_fd(_server_fd, buffer_request) != 0) {
    fprintf(stderr, "Failed to write in FIFO\n");
    if(close(_server_fd) != 0){
      perror("Failed to close server FIFO!");
    }
    if (unlink(req_pipe_path) != 0) {
      perror("Failed to unlink req_pipe_path");
    }
    if (unlink(resp_pipe_path) != 0) {
      perror("Failed to unlink resp_pipe_path");
    }
    if (unlink(notif_pipe_path) != 0) {
      perror("Failed to unlink notif_pipe_path");
    }
    return 1;
  }

  // Abre o FIFO de respostas em modo de leitura
  _resp_fd = open(resp_pipe_path, O_RDONLY);
  if (_resp_fd < 0) {
    // Caso não consiga abrir o FIFO de respostas, limpa os recursos e retorna erro
    fprintf(stderr, "Failed to open response FIFO\n");
    if(close(_server_fd) != 0){
      perror("Failed to close server FIFO!");
    }
    if (unlink(req_pipe_path) != 0) {
      perror("Failed to unlink req_pipe_path");
    }
    if (unlink(resp_pipe_path) != 0) {
      perror("Failed to unlink resp_pipe_path");
    }
    if (unlink(notif_pipe_path) != 0) {
      perror("Failed to unlink notif_pipe_path");
    }
    return 1;
  }

  // Abre o FIFO de notificações em modo de leitura
  _notif_fd = open(notif_pipe_path, O_RDONLY);
  if (_notif_fd < 0) {
    // Caso não consiga abrir o FIFO de notificações, limpa os recursos e retorna erro
    fprintf(stderr, "Failed to open notification FIFO\n");
    if(close(_server_fd) != 0){
      perror("Failed to close server FIFO!");
    }
    if (close(_resp_fd) != 0) {
      perror("Failed to close response FIFO!");
    }
    if (unlink(req_pipe_path) != 0) {
      perror("Failed to unlink req_pipe_path");
    }
    if (unlink(resp_pipe_path) != 0) {
      perror("Failed to unlink resp_pipe_path");
    }
    if (unlink(notif_pipe_path) != 0) {
      perror("Failed to unlink notif_pipe_path");
    }
    return 1;
  }

  // Abre o FIFO de pedidos em modo de escrita
  _req_fd = open(req_pipe_path, O_WRONLY);
  if (_notif_fd < 0) {
    fprintf(stderr, "Failed to open request FIFO\n");
    if(close(_server_fd) != 0){
      perror("Failed to close server FIFO!");
    }
    if (close(_resp_fd) != 0) {
      perror("Failed to close response FIFO!");
    }
    if (close(_notif_fd) != 0) {
      perror("Failed to close notification FIFO!");
    }
    if (unlink(req_pipe_path) != 0) {
      perror("Failed to unlink req_pipe_path");
    }
    if (unlink(resp_pipe_path) != 0) {
      perror("Failed to unlink resp_pipe_path");
    }
    if (unlink(notif_pipe_path) != 0) {
      perror("Failed to unlink notif_pipe_path");
    }
    return 1;
  }
  
  printf("Waiting for server response\n");
  char buffer_response[MAX_CONNECT_STRING], charOPCODE = '0' + OP_CODE_CONNECT;

  // Lê a resposta do servidor (a leitura é atómica, pois lê menos de 4096 bytes)
  if (read(_resp_fd, buffer_response, 3) == -1) {
    fprintf(stderr, "Failed to open response FIFO\n");
    
    if (close(_resp_fd) != 0) {
    perror("Failed to close response FIFO!");
    }
    if (close(_req_fd) != 0) {
      perror("Failed to close request FIFO!");
    }
    if (close(_notif_fd) != 0) {
      perror("Failed to close notification FIFO!");
    }
    return 1;
  }

  // Verifica se o opcode da resposta é o esperado
  if (buffer_response[0] == charOPCODE) {
    if (buffer_response[2] == '0') {
      // Resposta do servidor indica sucesso
      fprintf(stdout, "Server returned 0 for operation: %s\n", CONNECT);  
      fflush(stdout);
    } else {
      // Resposta do servidor indica erro na operação
      fprintf(stdout, "Server returned 1 for operation: %s\n", CONNECT);  
      fprintf(stderr, "Could not connect to server!\n");
      fflush(stdout);
      return 1;
    }
  } else {
    // O opcode recebido não corresponde ao esperado
    fprintf(stderr, "Opcode not recognized for operation: %s\n", CONNECT);
    fflush(stdout);
    return 1;
  }

  // Regista os caminhos dos FIFOs utilizados
  _req_pipe_path = req_pipe_path;
  _resp_pipe_path = resp_pipe_path;
  _notif_pipe_path = notif_pipe_path;

  return 0;
}

/**
 * Desconecta o cliente do servidor KVS.
 * 
 * Esta função envia uma mensagem de desconexão ao servidor, aguarda confirmação,
 * fecha todos os descritores de arquivo (FIFOs) e limpa os recursos utilizados.
 * 
 * @return int Retorna 0 em caso de sucesso, 1 em caso de erro
 * 
 * @errors
 *    - Falha ao escrever mensagem de desconexão
 *    - Falha ao ler resposta do servidor
 *    - Erro na resposta do servidor
 * 
 */
int kvs_disconnect(void) {
  // Envia uma mensagem de desconexão para o FIFO de pedidos
  write(_req_fd, "2", 3);
  char buffer_response[MAX_CONNECT_STRING], charOPCODE = '0' + OP_CODE_DISCONNECT;

  // Lê a resposta do servidor (a leitura é atómica, pois lê menos de 4096 bytes)
  if (read(_resp_fd, buffer_response, 3) == -1) {
    fprintf(stderr, "Failed to read from response FIFO\n");
    fflush(stderr);
    if (close(_resp_fd) != 0) {
      perror("Failed to close response FIFO!");
    }
    if (close(_req_fd) != 0) {
      perror("Failed to close request FIFO!");
    }
    if (close(_notif_fd) != 0) {
      perror("Failed to close notification FIFO!");
    }
    if(close(_server_fd) != 0){
      perror("Failed to close server FIFO!");
    }
    return 1;
  }

  // Verifica se o opcode da resposta é o esperado
  if (buffer_response[0] == charOPCODE) {
    if (buffer_response[2] == '0') {
      // Resposta do servidor indica sucesso
      fprintf(stdout, "Server returned 0 for operation: %s\n", DISCONNECT);
      fflush(stdout);
    } else {
      // Resposta do servidor indica erro na operação
      fprintf(stdout, "Server returned 1 for operation: %s\n", DISCONNECT);
      fprintf(stderr, "Could not disconnect from server!\n");
      fflush(stdout);
      fflush(stderr);
      return 1;
    }
  } else {
    // O opcode recebido não corresponde ao esperado
    fprintf(stderr, "Opcode not recognized for operation: %s\n", DISCONNECT);
    fflush(stdout);
    return 1;
  }

  // Fecha os descritores abertos
  if (close(_req_fd) != 0) {
    perror("Failed to close request FIFO!");
  }

  if (close(_resp_fd) != 0) {
    perror("Failed to close response FIFO!");
  }

  if (close(_notif_fd) != 0) {
    perror("Failed to close notification FIFO!");
  }


  // Remove os FIFOs utilizados
  if (unlink(_req_pipe_path) != 0) {
    perror("Failed to unlink req_pipe_path");
  }
  if (unlink(_resp_pipe_path) != 0) {
    perror("Failed to unlink resp_pipe_path");
  }
  if (unlink(_notif_pipe_path) != 0) {
    perror("Failed to unlink notif_pipe_path");
  }

  return 0;
}

/**
 * Subscreve a notificações de alterações em uma chave específica.
 * 
 * Esta função registra o cliente para receber notificações sempre que o valor
 * associado à chave especificada for modificado no servidor.
 * 
 * @param key A chave que se deseja monitorar
 * 
 * @return int Retorna 0 em caso de sucesso, 1 em caso de erro
 * 
 * @errors
 *    - Chave não encontrada no servidor
 *    - Falha ao enviar pedido de subscrição
 *    - Falha ao ler resposta do servidor
 *    - Erro na resposta do servidor
 * 
 */
int kvs_subscribe(const char* key) {
  // Envia a mensagem de subscrição para o FIFO de pedidos e espera pela resposta no FIFO de resposta
  char buffer_request[MAX_STRING_SIZE], charOPCODE = '0' + OP_CODE_SUBSCRIBE;
  snprintf(buffer_request, MAX_STRING_SIZE, "%d|%s", OP_CODE_SUBSCRIBE, key);

  // Escreve a mensagem no FIFO de pedidos
  if (write(_req_fd, buffer_request, MAX_STRING_SIZE) == -1) {
    fprintf(stderr, "Failed to write to request FIFO\n");

    if (close(_req_fd) != 0) {
      perror("Failed to close request FIFO");
    }
    if (close(_resp_fd) != 0) {
      perror("Failed to close response FIFO");
    }
    if (close(_notif_fd) != 0) {
      perror("Failed to close notification FIFO");
    }

    return 1;
  }
  
  char buffer_response[MAX_STRING_SIZE];
  
  // Lê a resposta do servidor (a operação de leitura é atómica, pois lê menos de 4096 bytes)
  if (read(_resp_fd, buffer_response, 3) == -1) {
    fprintf(stderr, "Failed to read from response FIFO\n");

    if (close(_resp_fd) != 0) {
        perror("Failed to close response FIFO!");
    }
    if (close(_req_fd) != 0) {
        perror("Failed to close request FIFO!");
    }
    if (close(_notif_fd) != 0) {
        perror("Failed to close notification FIFO!");
    }

    return 1;
  }

  // Verifica se o opcode da resposta é o esperado
  if (buffer_response[0] == charOPCODE) {
    if (buffer_response[2] == '1') {
      // Resposta do servidor indica sucesso
      fprintf(stdout, "Server returned 1 for operation: %s\n", SUBSCRIBE);
    } else {
      // Resposta do servidor indica erro na operação (chave não encontrada)
      fprintf(stdout, "Server returned 0 for operation: %s\n", SUBSCRIBE);
      fprintf(stderr, "Could not subscribe to key! (Key not Found)\n");
      return 1;
    }
  } else {
    // O opcode recebido não corresponde ao esperado
    fprintf(stderr, "Opcode not recognized for operation: %s\n", SUBSCRIBE);
    return 1;
  }

  return 0;
}

/**
 * Cancela a subscrição de notificações para uma chave específica.
 * 
 * Esta função remove o registro de notificações do cliente para a chave
 * especificada, deixando de receber atualizações quando seu valor for alterado.
 * 
 * @param key A chave para a qual se deseja cancelar a subscrição
 * 
 * @return int Retorna 0 em caso de sucesso, 1 em caso de erro
 * 
 * @errors
 *    - Falha ao enviar pedido de cancelamento
 *    - Falha ao ler resposta do servidor
 *    - Erro na resposta do servidor
 *    - Cliente não estava subscrito à chave
 * 
 */
int kvs_unsubscribe(const char* key) {
  // Envia a mensagem de desinscrição para o FIFO de pedidos e espera pela resposta no FIFO de resposta
  char buffer_request[MAX_STRING_SIZE];
  snprintf(buffer_request, MAX_STRING_SIZE, "%d|%s", OP_CODE_UNSUBSCRIBE, key);
  
  // Envia a mensagem de desinscrição para o FIFO de pedidos
  if (write(_req_fd, buffer_request, MAX_STRING_SIZE) < 0) {
    fprintf(stderr, "Failed to write to request FIFO\n");
    if (close(_req_fd) != 0) {
      perror("Failed to close request FIFO!");
    }

    if (close(_resp_fd) != 0) {
      perror("Failed to close response FIFO!");
    }

    if (close(_notif_fd) != 0) {
      perror("Failed to close notification FIFO!");
    }
    return 1;
  }

  char buffer_response[MAX_STRING_SIZE], charOPCODE = '0' + OP_CODE_UNSUBSCRIBE;

  // Lê a resposta do servidor (a operação de leitura é atómica, pois lê menos de 4096 bytes)
  if (read(_resp_fd, buffer_response, 3) == -1) {
    fprintf(stderr, "Failed to read from response FIFO\n");
    if (close(_req_fd) != 0) {
      perror("Failed to close request FIFO!");
    }

    if (close(_resp_fd) != 0) {
      perror("Failed to close response FIFO!");
    }

    if (close(_notif_fd) != 0) {
      perror("Failed to close notification FIFO!");
    }
    return 1;
  }

  // Verifica se o opcode da resposta é o esperado
  if (buffer_response[0] == charOPCODE) {
    if (buffer_response[2] == '0') {
      // Resposta do servidor indica sucesso
      fprintf(stdout, "Server returned 0 for operation: %s\n", UNSUBSCRIBE);
    } else {
      // Resposta do servidor indica erro na operação (não conseguiu desinscrever)
      fprintf(stdout, "Server returned 1 for operation: %s\n", UNSUBSCRIBE);
      fprintf(stderr, "Could not unsubscribe to key!\n");
      return 1;
    }
  } else {
    // O opcode recebido não corresponde ao esperado
    fprintf(stderr, "Opcode not recognized for operation: %s\n", UNSUBSCRIBE);
    return 1;
  }

  return 0;
}
