#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "io.h"
#include "kvs.h"
#include "operations.h"
#include "parser.h"
#include "pthread.h"
#include "semaphore.h"
#include "src/common/constants.h"
#include "src/common/protocol.h"
#include "src/server/constants.h"

int write_server_flag = 0;  // Flag de controlo para indicar se o servidor está pronto para escrever
int sig_flag = 0;  // Flag para o sinal SIGUSR1
int read_index = 0;  // Índice de leitura no buffer
int write_index = 0;  // Índice de escrita no buffer
sem_t empty;  // Semáforo para indicar espaços vazios no buffer
sem_t full;  // Semáforo para indicar espaços preenchidos no buffer
pthread_mutex_t semExMut = PTHREAD_MUTEX_INITIALIZER;  // Mutex para exclusão mútua no controlo de semáforos
sem_t consumed;  // Semáforo para indicar que os dados foram consumidos


struct SharedData {
  DIR* dir;  
  char* dir_name;  
  pthread_mutex_t directory_mutex;
};

// Estrutura para representar um cliente
typedef struct Client {
  int client_req_fd;
  int client_resp_fd;  
  int client_notif_fd;  
  KeySubNode* subscriptions;
} Client;

// Buffers para armazenar os clientes
Client* client_server_buffer[MAX_SESSION_COUNT];  // Buffer de clientes a serem servidos
Client* clients_list[MAX_SESSION_COUNT];  // Lista de clientes conectados

// Função para inserir uma chave na lista de subscrições
int key_insert(KeySubNode** head, const char* key) {
  KeySubNode* new_node = (KeySubNode*)malloc(sizeof(KeySubNode));  // Criação de um novo nó
  if (new_node == NULL) {
    perror("Failed to create key node!");
    return 1;
  }

  new_node->key = strdup(key);  // Copia da chave para o nó
  if (new_node->key == NULL) {
    free(new_node);
    return 1;
  }

  new_node->next = NULL;  // Atribuição do próximo nó como NULL

  if (*head == NULL) {
    *head = new_node;  // Se a lista estiver vazia, o novo nó será o primeiro
  } else {
    new_node->next = *head;  // Inserção a esquerda na lista
    *head = new_node;  // O novo nó é agora o primeiro da lista
  }

  return 0;
}

// Função para tratar sinais (SIGUSR1)
void sig_handle() {
  sig_flag = 1;  // Altera a flag de sinal
  if (signal(SIGUSR1, sig_handle) == SIG_ERR) {  // Volta a registar o manipulador do sinal
    perror("signal could not be resolved\n");
    exit(EXIT_FAILURE);
  }
}

// Função para eliminar uma chave da lista de subscrições
int key_delete(KeySubNode** head, const char* key) {
  if (head == NULL || key == NULL) return 1;  // Verifica se os parâmetros são válidos

  KeySubNode *current = *head, *previous = NULL;  // Ponteiros para percorrer a lista
  while (current != NULL) {
    if (strcmp(current->key, key) == 0) {  // Verifica se a chave foi encontrada
      if (previous == NULL) {
        *head = current->next;  // Se for o primeiro nó, atualiza o head da lista
        free(current->key);
        free(current);
        return 0;
      } else {
        previous->next = current->next;  // Se não for o primeiro, ajusta o ponteiro do anterior
        free(current->key);
        free(current);
        return 0;
      }
    }
    previous = current;
    current = current->next;
  }

  return 1;  // Caso a chave não seja encontrada
}

pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t n_current_backups_lock = PTHREAD_MUTEX_INITIALIZER;

size_t active_backups = 0;  // Number of active backups
size_t max_backups;         // Maximum allowed simultaneous backups
size_t max_threads;         // Maximum allowed simultaneous threads
char* jobs_directory = NULL;
char* fifo_server;
char server_pipe_path[256] = "/tmp/server033";

int filter_job_files(const struct dirent* entry) {
  const char* dot = strrchr(entry->d_name, '.');
  if (dot != NULL && strcmp(dot, ".job") == 0) {
    return 1;  // Keep this file (it has the .job extension)
  }
  return 0;
}

static int entry_files(const char* dir, struct dirent* entry, char* in_path, char* out_path) {
  const char* dot = strrchr(entry->d_name, '.');
  if (dot == NULL || dot == entry->d_name || strlen(dot) != 4 || strcmp(dot, ".job")) {
    return 1;
  }

  if (strlen(entry->d_name) + strlen(dir) + 2 > MAX_JOB_FILE_NAME_SIZE) {
    fprintf(stderr, "%s/%s\n", dir, entry->d_name);
    return 1;
  }

  strcpy(in_path, dir);
  strcat(in_path, "/");
  strcat(in_path, entry->d_name);

  strcpy(out_path, in_path);
  strcpy(strrchr(out_path, '.'), ".out");

  return 0;
}

static int run_job(int in_fd, int out_fd, char* filename) {
  size_t file_backups = 0;
  while (1) {
    char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    unsigned int delay;
    size_t num_pairs;

    switch (get_next(in_fd)) {
      case CMD_WRITE:
        num_pairs = parse_write(in_fd, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
        if (num_pairs == 0) {
          write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (kvs_write(num_pairs, keys, values)) {
          write_str(STDERR_FILENO, "Failed to write pair\n");
        }
        break;

      case CMD_READ:
        num_pairs = parse_read_delete(in_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

        if (num_pairs == 0) {
          write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (kvs_read(num_pairs, keys, out_fd)) {
          write_str(STDERR_FILENO, "Failed to read pair\n");
        }
        break;

      case CMD_DELETE:
        num_pairs = parse_read_delete(in_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

        if (num_pairs == 0) {
          write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (kvs_delete(num_pairs, keys, out_fd)) {
          write_str(STDERR_FILENO, "Failed to delete pair\n");
        }

        // Iterar sobre todos os pares de chaves
        for (size_t i = 0; i < num_pairs; i++) {
          char* key = keys[i];  // Obter a chave atual
          // Iterar sobre todos os clientes na lista
          for (int j = 0; j < MAX_SESSION_COUNT; j++) {
            if (clients_list[j] != NULL) {  // Verificar se o cliente existe
              KeySubNode* current = clients_list[j]->subscriptions;  // Iniciar a iteração sobre as subscrições do cliente
              // Iterar sobre as subscrições do cliente
              while (current != NULL) {
                if (strcmp(current->key, key) == 0) {  // Comparar as strings das chaves (não os ponteiros)
                  key_delete(&(clients_list[j]->subscriptions), key);  // Eliminar a chave da lista de subscrições do cliente
                  break;  // Sair do loop após a remoção da chave
                }
                current = current->next;  // Passar para a próxima subscrição
              }
            }
          }
        }
        break;

      case CMD_SHOW:
        kvs_show(out_fd);
        break;

      case CMD_WAIT:
        if (parse_wait(in_fd, &delay, NULL) == -1) {
          write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (delay > 0) {
          printf("Waiting %d seconds\n", delay / 1000);
          kvs_wait(delay);
        }
        break;

      case CMD_BACKUP:
        pthread_mutex_lock(&n_current_backups_lock);
        if (active_backups >= max_backups) {
          wait(NULL);
        } else {
          active_backups++;
        }
        pthread_mutex_unlock(&n_current_backups_lock);
        int aux = kvs_backup(++file_backups, filename, jobs_directory);

        if (aux < 0) {
          write_str(STDERR_FILENO, "Failed to do backup\n");
        } else if (aux == 1) {
          return 1;
        }
        break;

      case CMD_INVALID:
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        break;

      case CMD_HELP:
        write_str(STDOUT_FILENO,
                  "Available commands:\n"
                  "  WRITE [(key,value)(key2,value2),...]\n"
                  "  READ [key,key2,...]\n"
                  "  DELETE [key,key2,...]\n"
                  "  SHOW\n"
                  "  WAIT <delay_ms>\n"
                  "  BACKUP\n"
                  "  HELP\n");

        break;

      case CMD_EMPTY:
        break;

      case EOC:
        printf("EOF\n");
        return 0;
    }
  }
}

// frees arguments
static void* get_file(void* arguments) {
  
  // Definir um conjunto de sinais a ser bloqueado
  sigset_t set;
  sigemptyset(&set);  // Inicializa o conjunto de sinais com nenhum sinal
  sigaddset(&set, SIGUSR1);  // Adiciona SIGUSR1 ao conjunto de sinais
  sigaddset(&set, SIGPIPE);   // Adiciona SIGPIPE ao conjunto de sinais

  // Bloquear os sinais definidos no conjunto
  pthread_sigmask(SIG_BLOCK, &set, NULL);  // Bloqueia os sinais especificados no conjunto 'set' para a thread atual


  struct SharedData* thread_data = (struct SharedData*)arguments;  //codigo do esqueleto (não comentado)
  DIR* dir = thread_data->dir;
  char* dir_name = thread_data->dir_name;

  if (pthread_mutex_lock(&thread_data->directory_mutex) != 0) {
    fprintf(stderr, "Thread failed to lock directory_mutex\n");
    return NULL;
  }

  struct dirent* entry;
  char in_path[MAX_JOB_FILE_NAME_SIZE], out_path[MAX_JOB_FILE_NAME_SIZE];
  while ((entry = readdir(dir)) != NULL) {
    if (entry_files(dir_name, entry, in_path, out_path)) {
      continue;
    }

    if (pthread_mutex_unlock(&thread_data->directory_mutex) != 0) {
      fprintf(stderr, "Thread failed to unlock directory_mutex\n");
      return NULL;
    }

    int in_fd = open(in_path, O_RDONLY);
    if (in_fd == -1) {
      write_str(STDERR_FILENO, "Failed to open input file: ");
      write_str(STDERR_FILENO, in_path);
      write_str(STDERR_FILENO, "\n");
      pthread_exit(NULL);
    }

    int out_fd = open(out_path, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (out_fd == -1) {
      write_str(STDERR_FILENO, "Failed to open output file: ");
      write_str(STDERR_FILENO, out_path);
      write_str(STDERR_FILENO, "\n");
      pthread_exit(NULL);
    }

    int out = run_job(in_fd, out_fd, entry->d_name);

    close(in_fd);
    close(out_fd);

    if (out) {
      if (closedir(dir) == -1) {
        fprintf(stderr, "Failed to close directory\n");
        return 0;
      }

      exit(0);
    }

    if (pthread_mutex_lock(&thread_data->directory_mutex) != 0) {
      fprintf(stderr, "Thread failed to lock directory_mutex\n");
      return NULL;
    }
  }

  if (pthread_mutex_unlock(&thread_data->directory_mutex) != 0) {
    fprintf(stderr, "Thread failed to unlock directory_mutex\n");
    return NULL;
  }

  pthread_exit(NULL);
}

// Função para lidar com desconexão súbita de um cliente
int client_sudden_disconnect(Client* client) {
  int client_req_fd = client->client_req_fd;    
  int client_resp_fd = client->client_resp_fd;
  int client_notif_fd = client->client_notif_fd;

  // Verificar se os FDs dos pipes são válidos
  if (client_req_fd < 0 || client_resp_fd < 0 || client_notif_fd < 0) {
    return 0;  // Retornar 0 se algum pipe não for válido
  }

  // Percorrer a lista de subscrições do cliente e tentar cancelar todas as subscrições
  KeySubNode* current = client->subscriptions;
  KeySubNode* next;

  while (current != NULL) {
    next = current->next;

    // Tentar cancelar a subscrição do cliente para a chave atual
    if (kvs_unsubscription(current->key, client->client_notif_fd) != 0) {
      fprintf(stderr, "Falha ao cancelar subscrição da chave: %s\n", current->key);
    }

    // Tentar eliminar a chave da lista de subscrições
    if (key_delete(&client->subscriptions, current->key) != 0) {
      fprintf(stderr, "Falha ao eliminar subscrição da chave: %s\n", current->key);
    }

    current = next;  // Avançar para a próxima subscrição
  }

  // Remover o cliente da lista de clientes
  for (int i = 0; i < MAX_SESSION_COUNT; i++) {
    if (clients_list[i] == client) {
      clients_list[i] = NULL;  // Definir a posição do cliente como NULL
      free(client);  // Libertar a memória associada ao cliente
      break;
    }
  }

  // Fechar os FDs dos pipes do cliente
  close(client_req_fd);
  close(client_resp_fd);
  close(client_notif_fd);

  return 0;  // Retornar 0 indicando que a desconexão foi processada com sucesso
}

static void* manage_clients(Client* temp_client) {
  printf("[Thread %ld] Iniciada\n", pthread_self());

  // Obtemos os descritores de ficheiro do cliente
  int client_req_fd = temp_client->client_req_fd;
  int client_resp_fd = temp_client->client_resp_fd;
  int client_notif_fd = temp_client->client_notif_fd;

  while (1) {
    
    printf("[Thread %ld] Aguardando comando...\n", pthread_self());
    char buffer[MAX_READ_SIZE];
    ssize_t bytes_read;

    // Lemos o pedido do cliente
    bytes_read = read(client_req_fd, buffer, MAX_READ_SIZE);

    // Se o cliente desconectar-se
    if (bytes_read == 0) {
      printf("[Thread %ld] Cliente desconectou abruptamente\n", pthread_self());
      client_sudden_disconnect(temp_client);
      return 0;
    }

    // Se foram lidos dados do cliente
    if (bytes_read > 0) {
      printf("[Thread %ld] Recebido: %s\n", pthread_self(), buffer);

      int res, cleanup_success;
      char *saveptr = NULL, answer[MAX_WRITE_SIZE];
      char* token = strtok_r(buffer, "|", &saveptr);
      const char* key = NULL;

      // Processa o comando baseado no código de operação
      switch (atoi(token)) {
        case OP_CODE_DISCONNECT:
          printf("[Thread %ld] Processando DISCONNECT\n", pthread_self());
          cleanup_success = 1;

          KeySubNode* current = temp_client->subscriptions;
          KeySubNode* next;

          // Remove todas as subscrições do cliente
          while (current != NULL) {
            next = current->next;
            printf("[Thread %ld] Removendo subscrição: %s\n", pthread_self(), current->key);
            if (kvs_unsubscription(current->key, temp_client->client_notif_fd) != 0) {
              fprintf(stderr, "[Thread %ld] Falha unsubscribe: %s\n", pthread_self(), current->key);
              cleanup_success = 0;
            }

            if (key_delete(&temp_client->subscriptions, current->key) != 0) {
              fprintf(stderr, "[Thread %ld] Falha remover chave: %s\n", pthread_self(), current->key);
              cleanup_success = 0;
            }
            current = next;
          }

          // Envia a resposta ao cliente
          snprintf(answer, MAX_WRITE_SIZE, "%d|%d", OP_CODE_DISCONNECT, cleanup_success ? 0 : 1);
          printf("[Thread %ld] Enviando resposta: %s\n", pthread_self(), answer);

          if (write(client_resp_fd, answer, strlen(answer)) == -1) {
            if (errno == EPIPE) {
              fprintf(stderr, "[Thread %ld] Houve um Kill, Epipe foi lancado\n", pthread_self());
              return 0;
            }

            fprintf(stderr, "[Thread %ld] Falha enviar resposta disconnect\n", pthread_self());
            return 0;
          }

          // Remover o cliente da lista de clientes
          for (int i = 0; i < MAX_SESSION_COUNT; i++) {
            if (clients_list[i] == temp_client) {
              clients_list[i] = NULL;
              free(temp_client);
              break;
            }
          }

          // Fechar as conexões com o cliente
          printf("[Thread %ld] Fechando conexão\n", pthread_self());
          close(client_req_fd);
          close(client_resp_fd);
          close(client_notif_fd);
          return 0;

        case OP_CODE_SUBSCRIBE:

          // Processamento do comando de subscrição
          key = strtok_r(NULL, "|", &saveptr);
          printf("[Thread %ld] Processando SUBSCRIBE: %s\n", pthread_self(), key);

          res = kvs_subscription(key, client_notif_fd);
          printf("[Thread %ld] kvs_subscription retornou: %d\n", pthread_self(), res);

          // Resposta sobre a subscrição
          if (res == 0) {
            if (key_insert(&temp_client->subscriptions, key) != 0) {
              fprintf(stderr, "[Thread %ld] Falha inserir chave\n", pthread_self());
              return 0;
            }
            snprintf(answer, MAX_WRITE_SIZE, "%d|1", OP_CODE_SUBSCRIBE);
          } else {
            snprintf(answer, MAX_WRITE_SIZE, "%d|0", OP_CODE_SUBSCRIBE);
          }

          printf("[Thread %ld] Enviando resposta: %s\n", pthread_self(), answer);
          if (write(client_resp_fd, answer, strlen(answer)) == -1) {
            if (errno == EPIPE) {
              fprintf(stderr, "[Thread %ld] Houve um Kill, Epipe foi lancado\n", pthread_self());
              return 0;
            }

            fprintf(stderr, "[Thread %ld] Falha enviar resposta subscribe\n", pthread_self());
            return 0;
          }
          
          // Exemplo de impressão de clientes e suas subscrições
          for (int i = 0; i < MAX_SESSION_COUNT; i++) {
            if (clients_list[i] == NULL) {
              printf("Client[%d]: NULL\n", i);
              continue;
            }

            printf("Client[%d]:\n", i);
            printf("  Request FD: %d\n", clients_list[i]->client_req_fd);
            printf("  Response FD: %d\n", clients_list[i]->client_resp_fd);
            printf("  Notification FD: %d\n", clients_list[i]->client_notif_fd);

            // Imprimir as subscrições
            printf("  Subscriptions: ");
            KeySubNode* current2 = clients_list[i]->subscriptions;
            if (current2 == NULL) {
              printf("None\n");
            } else {
              printf("\n");
              while (current2 != NULL) {
                printf("    - Key: %s\n", current2->key);
                current2 = current2->next;
              }
            }
            printf("\n");
          }

          break;

        case OP_CODE_UNSUBSCRIBE:
          // Processamento do comando de desinscrição
          key = strtok_r(NULL, "|", &saveptr);
          printf("[Thread %ld] Processando UNSUBSCRIBE: %s\n", pthread_self(), key);

          res = kvs_unsubscription(key, client_notif_fd);
          printf("[Thread %ld] kvs_unsubscription retornou: %d\n", pthread_self(), res);

          // Resposta sobre a desinscrição
          if (res == 0) {
            if (key_delete(&temp_client->subscriptions, key) != 0) {
              fprintf(stderr, "[Thread %ld] Falha remover chave\n", pthread_self());
              return 0;
            }
            snprintf(answer, MAX_WRITE_SIZE, "%d|0", OP_CODE_UNSUBSCRIBE);
          } else {
            snprintf(answer, MAX_WRITE_SIZE, "%d|1", OP_CODE_UNSUBSCRIBE);
          }

          printf("[Thread %ld] Enviando resposta: %s\n", pthread_self(), answer);
          if (write(client_resp_fd, answer, strlen(answer)) == -1) {
            if (errno == EPIPE) {
              fprintf(stderr, "[Thread %ld] Houve um Kill, Epipe foi lancado\n", pthread_self());
              return 0;
            }

            fprintf(stderr, "[Thread %ld] Falha enviar resposta unsubscribe\n", pthread_self());
            return 0;
          }

          // Exemplo de impressão de clientes e suas subscrições
          for (int i = 0; i < MAX_SESSION_COUNT; i++) {
            if (clients_list[i] == NULL) {
              printf("Client[%d]: NULL\n", i);
              continue;
            }

            printf("Client[%d]:\n", i);
            printf("  Request FD: %d\n", clients_list[i]->client_req_fd);
            printf("  Response FD: %d\n", clients_list[i]->client_resp_fd);
            printf("  Notification FD: %d\n", clients_list[i]->client_notif_fd);

            // Imprimir as subscrições
            printf("  Subscriptions: ");
            KeySubNode* current3 = clients_list[i]->subscriptions;
            if (current3 == NULL) {
              printf("None\n");
            } else {
              printf("\n");
              while (current3 != NULL) {
                printf("    - Key: %s\n", current3->key);
                current3 = current3->next;
              }
            }
            printf("\n");
          }

          break;

        default:
          fprintf(stderr, "[Thread %ld] Opcode inválido\n", pthread_self());
          break;
      }
    }
  }
  return 0;
}

void consume() {
  // Espera por um item disponível (não vazio) no buffer
  sem_wait(&full);

  // Bloqueia o mutex para acessar de forma segura o índice e o buffer
  pthread_mutex_lock(&semExMut);

  // Lê o cliente presente na posição 'read_index' no buffer
  Client* C = client_server_buffer[read_index];
  // Atualiza o índice de leitura (circular)
  read_index = (read_index + 1) % MAX_SESSION_COUNT;

  // Liberta o mutex após atualizar o índice
  pthread_mutex_unlock(&semExMut);

  // Liberta o semáforo que indica que o buffer tem espaço para mais itens
  sem_post(&empty);
  
  // Indica que o item foi consumido
  sem_post(&consumed);

  // Processa o cliente lido (chama a função para gerenciar o cliente)
  manage_clients(C);
}

void produce(Client* c) {
  // Espera por um espaço disponível (não cheio) no buffer
  sem_wait(&empty);

  // Bloqueia o mutex para acessar de forma segura o índice e o buffer
  pthread_mutex_lock(&semExMut);

  // Adiciona o cliente à posição 'write_index' no buffer
  client_server_buffer[write_index] = c;
  // Atualiza o índice de escrita (circular)
  write_index = (write_index + 1) % MAX_SESSION_COUNT;

  // Liberta o mutex após atualizar o índice
  pthread_mutex_unlock(&semExMut);

  // Liberta o semáforo que indica que o buffer tem itens disponíveis para consumir
  sem_post(&full);

  // Espera até que o item seja consumido
  sem_wait(&consumed);
}

void* clients_loop() {
  // Define o conjunto de sinais a bloquear
  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, SIGUSR1);  // Bloqueia SIGUSR1
  sigaddset(&set, SIGPIPE);   // Bloqueia SIGPIPE

  // Bloqueia os sinais SIGUSR1 e SIGPIPE na thread atual
  pthread_sigmask(SIG_BLOCK, &set, NULL);

  // Loop infinito para consumir os itens do buffer
  while (1) {
    consume();  // Chama a função 'consume' para processar o próximo cliente
  }
}

static int dispatch_threads(DIR* dir) {
  
  pthread_t* threads = malloc(max_threads * sizeof(pthread_t));

  if (threads == NULL) {
    fprintf(stderr, "Falha ao alocar memória para os threads\n");
    return 1;
  }

  struct SharedData thread_data = {dir, jobs_directory, PTHREAD_MUTEX_INITIALIZER};
  
  for (size_t i = 0; i < max_threads; i++) {
    if (pthread_create(&threads[i], NULL, get_file, (void*)&thread_data) != 0) {
      fprintf(stderr, "Falha ao criar thread %zu\n", i);
      pthread_mutex_destroy(&thread_data.directory_mutex);
      free(threads);
      return 1;
    }
  }

  // Cria threads para o loop de gestão de clientes
  pthread_t manager_threads[MAX_SESSION_COUNT];
  for (int i = 0; i < MAX_SESSION_COUNT; i++) {
    pthread_create(&manager_threads[i], NULL, clients_loop, NULL);
  }

  // Concatena o caminho do pipe do servidor
  strncat(server_pipe_path, fifo_server, strlen(fifo_server) * sizeof(char));

  // Remove o arquivo FIFO caso já exista
  if (unlink(server_pipe_path) != 0 && errno != ENOENT) {
    fprintf(stderr, "Falha ao remover o FIFO do servidor!\n");
    return 1;
  }

  // Cria o FIFO com as permissões adequadas
  if (mkfifo(server_pipe_path, 0640) != 0) {
    write_str(STDERR_FILENO, "Falha ao criar o FIFO\n");
    unlink(server_pipe_path);
    return 1;
  }

  // Abre o FIFO para leitura
  int fifo_fd_read = open(server_pipe_path, O_RDONLY);
  if (fifo_fd_read == -1) {
    write_str(STDERR_FILENO, "Falha ao abrir o FIFO\n");
    return 1;
  }

  // Abrir para escrita para mais clientes

  // LER A MENSAGEM DE CONNECT
  while (1) {
    char buffer[MAX_READ_SIZE];

    // Loop para processar mais clientes
  if (read(fifo_fd_read, buffer, MAX_READ_SIZE) == -1) {
    // Verifica se houve erro ao ler do FIFO, especificamente se foi causado por um sinal interrompido (EINTR)
    if (errno == EINTR) {
      // Se a flag de sinal (sig_flag) estiver ativada, processa a desconexão súbita dos clientes
      if (sig_flag == 1) {
        for (int i = 0; i < MAX_SESSION_COUNT; i++) {
          if (clients_list[i] != NULL) {
            client_sudden_disconnect(clients_list[i]);
          }
        }
        // Reseta a flag e continua o loop
        sig_flag = 0;
        continue;
      }
    } else {
      write_str(STDERR_FILENO, "Erro ao ler do FIFO\n");
    }
  }

  // Verifica se o servidor já foi configurado para escrita no FIFO
  if (write_server_flag == 0) {
    // Tenta abrir o FIFO para escrita
    int fifo_fd_write = open(server_pipe_path, O_WRONLY);
    if (fifo_fd_write == -1) {
      write_str(STDERR_FILENO, "Falha ao abrir FIFO\n");
      return 1;
    }
    // Marca que o servidor está aberto para escrever no FIFO
    write_server_flag = 1;
  }

  // Processamento da mensagem recebida no buffer
  char* saveptr = NULL;
  char* token = strtok_r(buffer, "|", &saveptr);

  // Verifica se a primeira parte da mensagem é o OP_CODE do connect
  if (token == NULL || strcmp(token, "1") != 0) {
    write_str(STDERR_FILENO, "Mensagem inválida\n");
    return 1;
  }

  // Divisão da mensagem recebida em três partes, utilizando o delimitador '|'
  char* token1 = strtok_r(NULL, "|", &saveptr);  // Token 1: o caminho do arquivo de pedidos do cliente
  char* token2 = strtok_r(NULL, "|", &saveptr);  // Token 2: o caminho do arquivo de respostas do cliente
  char* token3 = strtok_r(NULL, "|", &saveptr);  // Token 3: o caminho do arquivo de notificações do cliente

  // Aloca memória para um novo cliente
  Client* new_client = malloc(sizeof(Client));

  // Abre os arquivos correspondentes aos descritores de arquivo de leitura e escrita para o novo cliente
  new_client->client_resp_fd = open(token2, O_WRONLY);
  new_client->client_notif_fd = open(token3, O_WRONLY);  
  new_client->client_req_fd = open(token1, O_RDONLY);  

  // Inicializa a lista de subscrições do cliente (inicialmente vazia)
  new_client->subscriptions = NULL;

  // Prepara uma resposta que será enviada ao cliente
  char answer[MAX_WRITE_SIZE];
  snprintf(answer, MAX_WRITE_SIZE, "%d|0", OP_CODE_CONNECT);  // Resposta de conexão com código de operação e status (0)

  // Coloca o novo cliente na fila de produção (adiciona ao buffer compartilhado)
  produce(new_client);

  // Envia a resposta ao cliente via o descritor de arquivo de resposta
  if (write(new_client->client_resp_fd, answer, strlen(answer)) == -1) {
    fprintf(stderr, "Falha ao escrever resposta no fd: %s\n", CONNECT);
    return 1;  // Retorna em caso de erro
  }

  // Procura uma posição vazia na lista de clientes (clients_list) e adiciona o novo cliente
  for (int i = 0; i < MAX_SESSION_COUNT; i++) {
    if (clients_list[i] == NULL) {
      clients_list[i] = new_client;  // Adiciona o cliente na primeira posição vazia
      break;
    }
  }

    for (int i = 0; i < MAX_SESSION_COUNT; i++) {
      if (clients_list[i] == NULL) {
        printf("Client[%d]: NULL\n", i);
        continue;
      }

      printf("Client[%d]:\n", i);
      printf("  Request FD: %d\n", clients_list[i]->client_req_fd);
      printf("  Response FD: %d\n", clients_list[i]->client_resp_fd);
      printf("  Notification FD: %d\n", clients_list[i]->client_notif_fd);

      // Imprimir as subscrições
      printf("  Subscriptions: ");
      KeySubNode* current = clients_list[i]->subscriptions;
      if (current == NULL) {
        printf("None\n");
      } else {
        printf("\n");
        while (current != NULL) {
          printf("    - Key: %s\n", current->key);
          current = current->next;
        }
      }
      printf("\n");
    }
  }

  for (unsigned int i = 0; i < MAX_SESSION_COUNT; i++) {
    if (pthread_join(manager_threads[i], NULL) != 0) {
      fprintf(stderr, "Failed to join thread");
      return 1;
    }
  }

  for (unsigned int i = 0; i < max_threads; i++) {
    if (pthread_join(threads[i], NULL) != 0) {
      fprintf(stderr, "Failed to join thread %u\n", i);
      pthread_mutex_destroy(&thread_data.directory_mutex);
      free(threads);
      return 1;
    }
  }

  if (pthread_mutex_destroy(&thread_data.directory_mutex) != 0) {
    fprintf(stderr, "Failed to destroy directory_mutex\n");
  }

  free(threads);
  return 0;
}

int main(int argc, char** argv) {
  if (signal(SIGUSR1, sig_handle) == SIG_ERR) {
    perror("signal could not be resolved\n");
    exit(EXIT_FAILURE);
  }

  if (argc < 5) {
    write_str(STDERR_FILENO, "Usage: ");
    write_str(STDERR_FILENO, argv[0]);
    write_str(STDERR_FILENO, " <jobs_dir>");
    write_str(STDERR_FILENO, " <max_threads>");
    write_str(STDERR_FILENO, " <max_backups>");
    write_str(STDERR_FILENO, " <fifo_register_name> \n");
    return 1;
  }

  jobs_directory = argv[1];
  fifo_server = argv[4];

  char* endptr;
  max_backups = strtoul(argv[3], &endptr, 10);

  if (*endptr != '\0') {
    fprintf(stderr, "Invalid max_proc value\n");
    return 1;
  }

  max_threads = strtoul(argv[2], &endptr, 10);

  if (*endptr != '\0') {
    fprintf(stderr, "Invalid max_threads value\n");
    return 1;
  }

  if (max_backups <= 0) {
    write_str(STDERR_FILENO, "Invalid number of backups\n");
    return 0;
  }

  if (max_threads <= 0) {
    write_str(STDERR_FILENO, "Invalid number of threads\n");
    return 0;
  }

  if (kvs_init()) {
    write_str(STDERR_FILENO, "Failed to initialize KVS\n");
    return 1;
  }

  sem_init(&consumed, 0, 0);
  sem_init(&empty, 0, MAX_SESSION_COUNT);
  sem_init(&full, 0, 0);

  DIR* dir = opendir(argv[1]);
  if (dir == NULL) {
    fprintf(stderr, "Failed to open directory: %s\n", argv[1]);
    return 0;
  }

  dispatch_threads(dir);

  if (closedir(dir) == -1) {
    fprintf(stderr, "Failed to close directory\n");
    return 0;
  }

  while (active_backups > 0) {
    wait(NULL);
    active_backups--;
  }

  kvs_terminate();

  return 0;
}
