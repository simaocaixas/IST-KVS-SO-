#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "parser.h"
#include "src/client/api.h"
#include "src/common/constants.h"
#include "src/common/io.h"

/**
 * Thread responsável por gerenciar as notificações recebidas do servidor
 * @param arguments - Ponteiro para o fd do arquivo de notificações
 * @return NULL após a conclusão (não utilizado)
 */
void* manage_notifications(void* arguments) {

  if (arguments == NULL) {
    fprintf(stderr, "Error: NULL argument received\n");
    exit(1);
  }

  int* notify_fd = (int*)arguments;
  if (*notify_fd < 0) {
    fprintf(stderr, "Error: Invalid file descriptor\n");
    exit(1);
  }
  
  // Ciclo infinito para continuar a receber notificações
  while (1) {
    char buffer[MAX_STRING_SIZE];
    ssize_t bytes_read = read(*notify_fd, buffer, MAX_STRING_SIZE);

  if (bytes_read < 0) {
        // EINTR: Chamada foi interrompida por um sinal
        if (errno == EINTR) {
          continue;  
        }
        // EAGAIN Não há dados disponíveis
        else if (errno == EAGAIN) {
          continue;  // Tenta ler novamente
        }
        // EPIPE: Pipe foi fechado do outro lado
        else if (errno == EPIPE) {
          printf("Connection lost! (Server received a SIGURS1)\n");
          exit(1);
        }
        // EBADF: File descriptor inválido
        else if (errno == EBADF) {
          printf("Error: Invalid notification pipe descriptor!\n");
          exit(1);
        }
        // EIO: Erro de I/O no sistema de ficheiros
        else if (errno == EIO) {
          printf("Error: I/O error on notification pipe!\n");
          exit(1);
        }
        // Outros erros não especificados
        else {
          printf("Error reading from notification pipe: %s\n", strerror(errno));
          exit(1);
        }
      } 
      // EOF - pipe foi fechado normalmente
      else if (bytes_read == 0) {
        printf("Connection lost!\n");
        exit(1);
      }

    // Adiciona terminador da string e imprime
    buffer[bytes_read] = '\0';
    printf("%s\n", buffer);
  }

  return NULL;
}

/**
 * Função principal do programa - Gere a inicialização do cliente e processamento de comandos
 * 
 * @param argc - Número de argumentos da linha de comandos
 * @param argv - Array de strings com os argumentos da linha de comandos
 * @return 0 em caso de sucesso, 1 em caso de erro
 */
int main(int argc, char* argv[]) {
  // Verifica se há argumentos suficientes na linha de comandos
  if (argc < 3) {
    fprintf(stderr, "Usage: %s <client_unique_id> <register_pipe_path>\n", argv[0]);
    return 1;
  }

  /*
  Para garantir a unicidade dos fifos no cluster em que será 
  avaliado o projeto adicionamos o nosso numero de grupo.
  */
  char req_pipe_path[256] = "/tmp/req033";
  char resp_pipe_path[256] = "/tmp/resp033";
  char notif_pipe_path[256] = "/tmp/notif033";
  char server_pipe_path[256] = "/tmp/server033";

  char keys[MAX_NUMBER_SUB][MAX_STRING_SIZE] = {0};
  unsigned int delay_ms;
  size_t num;

  // Concatena os IDs aos caminhos dos pipes
  strncat(req_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));
  strncat(resp_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));
  strncat(notif_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));
  strncat(server_pipe_path, argv[2], strlen(argv[2]) * sizeof(char));

  // Estabelece a ligação com o servidor
  if (kvs_connect(req_pipe_path, resp_pipe_path, server_pipe_path, notif_pipe_path) != 0) {
    fprintf(stderr, "Failed to connect to the server\n");
    return 1;
  }

  // thread para gestão de notificações
  pthread_t thread_notify_me;
  int* notify_fd = get_notify_fd();
  pthread_create(&thread_notify_me, NULL, manage_notifications, notify_fd);

  // Ciclo principal para o processamento de comandos
  while (1) {
    switch (get_next(STDIN_FILENO)) {
      case CMD_DISCONNECT:
      
        if (kvs_disconnect() != 0) {
          fprintf(stderr, "Failed to disconnect to the server\n");
          return 1;
        }
    
        // Aguarda a finalização da thread de notificações, dessa forma garantimos que terminamos o programa sem thread zombie
        pthread_join(thread_notify_me, NULL);
        return 0;

      case CMD_SUBSCRIBE:
        num = parse_list(STDIN_FILENO, keys, 1, MAX_STRING_SIZE);
        if (num == 0) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (kvs_subscribe(keys[0])) {
          fprintf(stderr, "Command subscribe failed\n");
        }

        break;

      case CMD_UNSUBSCRIBE:
        num = parse_list(STDIN_FILENO, keys, 1, MAX_STRING_SIZE);
        if (num == 0) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (kvs_unsubscribe(keys[0])) {
          fprintf(stderr, "Command unsubscribe failed\n");
        }

        break;

      case CMD_DELAY:
        // Feita completamente no esquelo inicial fornecido 
        if (parse_delay(STDIN_FILENO, &delay_ms) == -1) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (delay_ms > 0) {
          printf("Waiting...\n");
          delay(delay_ms);
        }
        break;

      case CMD_INVALID:
        fprintf(stderr, "Invalid command. See HELP for usage\n");
        break;

      case CMD_EMPTY:
        // Trata do comando vazio - nenhuma acção é necessária
        break;

      case EOC:
        // Fim dos comandos - ciclo continua até desconnect
        break;
    }
  }
}