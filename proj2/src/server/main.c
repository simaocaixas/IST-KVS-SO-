#include <unistd.h>
#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <stdio.h>
#include <sys/stat.h>
#include <errno.h>
#include <signal.h>

#include "src/server/constants.h"
#include "src/common/constants.h"
#include "src/common/protocol.h"
#include "parser.h"
#include "operations.h"
#include "io.h"
#include "pthread.h"
#include "kvs.h"
#include "semaphore.h"

int write_server_flag = 0;
int sig_flag = 0;
int read_index = 0;
int write_index = 0;
sem_t empty;
sem_t full;
pthread_mutex_t semExMut = PTHREAD_MUTEX_INITIALIZER;
sem_t consumed;  // Declare no início do arquivo com os outros semáforos
struct SharedData {
  DIR* dir;
  char* dir_name;
  pthread_mutex_t directory_mutex;
};

typedef struct {
  int client_req_fd;
  int client_resp_fd; 
  int client_notif_fd;
  KeySubNode *subscriptions;
} Client;

Client *client_server_buffer[MAX_SESSION_COUNT];
Client *clients_list[MAX_SESSION_COUNT];

typedef struct PipeData {
  pthread_t thread;
  Client client;
} PipeData;

int key_insert(KeySubNode **head, const char* key) {
  KeySubNode *new_node = (KeySubNode*)malloc(sizeof(KeySubNode));
  if (new_node == NULL) {
    perror("Failed to create key node!");
    return 1;
  } 

  new_node->key = strdup(key);
  if(new_node->key == NULL) {
    free(new_node);
    return 1;
  }

  new_node->next = NULL;

  if(*head == NULL) {
    *head = new_node;
  } else {
    new_node->next = *head; //inserção a esquerda
    *head = new_node;
  }

  return 0;
}

void sig_handle() {
  sig_flag = 1;
  if (signal(SIGUSR1,sig_handle) == SIG_ERR) {
    perror("signal could not be resolved\n");
    exit(EXIT_FAILURE);
  }
}

int key_delete(KeySubNode **head, const char* key) {
  if (head == NULL || key == NULL) return 1;

  KeySubNode *current = *head, *previous = NULL;
  while(current != NULL) {
    if(strcmp(current->key, key) == 0) {
      if (previous == NULL) {
        *head = current->next;
        free(current->key);
        free(current);
        return 0;
      } else {
        previous->next = current->next;
        free(current->key);
        free(current);
        return 0; 
      }
    }
    previous = current;
    current = current->next;
  }

  return 1;
}

// we have a list of clients: [client1_req_fd, client1_resp_fd, client1_notif_fd], [client2_req_fd, client2_resp_fd, client2_notif_fd], ...]

pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t n_current_backups_lock = PTHREAD_MUTEX_INITIALIZER;

size_t active_backups = 0;     // Number of active backups
size_t max_backups;            // Maximum allowed simultaneous backups
size_t max_threads;            // Maximum allowed simultaneous threads
char* jobs_directory = NULL;
char* fifo_server;
char server_pipe_path[256] = "/tmp/server";

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

        for (size_t i = 0; i < num_pairs; i++) {
            char* key = keys[i];
            for (int j = 0; j < MAX_SESSION_COUNT; j++) {
                if (clients_list[j] != NULL) {  // Verificar se o cliente existe
                    KeySubNode* current = clients_list[j]->subscriptions;
                    while (current != NULL) {
                        if (strcmp(current->key, key) == 0) {  // Comparar strings em vez de ponteiros
                            key_delete(&(clients_list[j]->subscriptions), key);
                            break;
                        }
                        current = current->next;
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

//frees arguments
static void* get_file(void* arguments) {

  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, SIGUSR1);
  sigaddset(&set, SIGPIPE);
  pthread_sigmask(SIG_BLOCK, &set, NULL);

  struct SharedData* thread_data = (struct SharedData*) arguments;
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

int client_sudden_disconnect(Client *client){
  
  int client_req_fd = client->client_req_fd; // we (server) will write to this
  int client_resp_fd = client->client_resp_fd; // we will read from this
  int client_notif_fd = client->client_notif_fd; 
  
  if (client_req_fd < 0 || client_resp_fd < 0 || client_notif_fd < 0) {
    return 0;
  }

  KeySubNode *current = client->subscriptions;
  KeySubNode *next;

  while (current != NULL) {
    next = current->next;
    if (kvs_unsubscription(current->key, client->client_notif_fd) != 0) {
      fprintf(stderr, "Failed to unsubscribe key: %s\n", current->key);
    }

    if(key_delete(&client->subscriptions, current->key) != 0) {
      fprintf(stderr, "Failed to delete subscription from key: %s\n", current->key);
    }
    current = next;
  }

  for (int i = 0; i < MAX_SESSION_COUNT; i++) {
    if(clients_list[i] == client) {
      clients_list[i] = NULL;
      free(client);
      break;
    }
  }

  close(client_req_fd); close(client_resp_fd); close(client_notif_fd);

  return 0;
}

static void* manage_clients(Client *temp_client) {
 
 printf("[Thread %ld] Iniciada\n", pthread_self());
 
 int client_req_fd = temp_client->client_req_fd;
 int client_resp_fd = temp_client->client_resp_fd;
 int client_notif_fd = temp_client->client_notif_fd; 
  
 while (1) {
   printf("[Thread %ld] Aguardando comando...\n", pthread_self());
   char buffer[MAX_READ_SIZE];
   ssize_t bytes_read;
   bytes_read = read(client_req_fd, buffer, MAX_READ_SIZE); 
   
   if(bytes_read == -1 && errno == EPIPE) {
      printf("DEU MERDA!!!!\n");
      return 0;
   }

   if (bytes_read == 0) {
      printf("[Thread %ld] Cliente desconectou abruptamente\n", pthread_self()); 
      client_sudden_disconnect(temp_client);
      return 0;
    }
   
   
   if (bytes_read > 0) {
     printf("[Thread %ld] Recebido: %s\n", pthread_self(), buffer);
       
     int res, cleanup_success;
     char *saveptr = NULL, answer[MAX_WRITE_SIZE];
     char* token = strtok_r(buffer, "|", &saveptr);
     const char* key = NULL;

     switch (atoi(token)) {
       case OP_CODE_DISCONNECT:
         printf("[Thread %ld] Processando DISCONNECT\n", pthread_self());
         cleanup_success = 1;
         KeySubNode *current = temp_client->subscriptions;
         KeySubNode *next;

         while (current != NULL) {
           next = current->next;
           printf("[Thread %ld] Removendo subscrição: %s\n", pthread_self(), current->key);
           if (kvs_unsubscription(current->key, temp_client->client_notif_fd) != 0) {
             fprintf(stderr, "[Thread %ld] Falha unsubscribe: %s\n", pthread_self(), current->key);
             cleanup_success = 0;
           }

           if(key_delete(&temp_client->subscriptions, current->key) != 0) {
             fprintf(stderr, "[Thread %ld] Falha remover chave: %s\n", pthread_self(), current->key);
             cleanup_success = 0;
           }
           current = next;
         }

         snprintf(answer, MAX_WRITE_SIZE, "%d|%d", OP_CODE_DISCONNECT, cleanup_success ? 0 : 1);
         printf("[Thread %ld] Enviando resposta: %s\n", pthread_self(), answer);

         if(write(client_resp_fd, answer, strlen(answer)) == -1) {
            if (errno == EPIPE) {
              fprintf(stderr, "[Thread %ld] Houve um Kill, Epipe foi lancado\n", pthread_self());
              return 0;
            }

           fprintf(stderr, "[Thread %ld] Falha enviar resposta disconnect\n", pthread_self());
           return 0;
         }
         
         for (int i = 0; i < MAX_SESSION_COUNT; i++) {
          if(clients_list[i] == temp_client) {
            clients_list[i] = NULL;
            free(temp_client);
            break;
          }
         }
         
         printf("[Thread %ld] Fechando conexão\n", pthread_self());
         close(client_req_fd); 
         close(client_resp_fd); 
         close(client_notif_fd);
         return 0;
         
       case OP_CODE_SUBSCRIBE:
         key = strtok_r(NULL, "|", &saveptr);
         printf("[Thread %ld] Processando SUBSCRIBE: %s\n", pthread_self(), key);
         
         res = kvs_subscription(key, client_notif_fd);
         printf("[Thread %ld] kvs_subscription retornou: %d\n", pthread_self(), res);

         if(res == 0) {
           if(key_insert(&temp_client->subscriptions, key) != 0) {
             fprintf(stderr, "[Thread %ld] Falha inserir chave\n", pthread_self());
             return 0;
           }
           snprintf(answer, MAX_WRITE_SIZE, "%d|1", OP_CODE_SUBSCRIBE);
         } else {
           snprintf(answer, MAX_WRITE_SIZE, "%d|0", OP_CODE_SUBSCRIBE);
         }

         printf("[Thread %ld] Enviando resposta: %s\n", pthread_self(), answer);
         if(write(client_resp_fd, answer, strlen(answer)) == -1) {
            if (errno == EPIPE) {
              fprintf(stderr, "[Thread %ld] Houve um Kill, Epipe foi lancado\n", pthread_self());
              return 0;
            }

           fprintf(stderr, "[Thread %ld] Falha enviar resposta subscribe\n", pthread_self());
           return 0;
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
            KeySubNode *current2 = clients_list[i]->subscriptions;
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
         key = strtok_r(NULL, "|", &saveptr);
         printf("[Thread %ld] Processando UNSUBSCRIBE: %s\n", pthread_self(), key);

         res = kvs_unsubscription(key, client_notif_fd);
         printf("[Thread %ld] kvs_unsubscription retornou: %d\n", pthread_self(), res);

         if(res == 0) {
           if(key_delete(&temp_client->subscriptions, key) != 0) {
             fprintf(stderr, "[Thread %ld] Falha remover chave\n", pthread_self());
             return 0;
           }
           snprintf(answer, MAX_WRITE_SIZE, "%d|0", OP_CODE_UNSUBSCRIBE);
         } else {
           snprintf(answer, MAX_WRITE_SIZE, "%d|1", OP_CODE_UNSUBSCRIBE);
         }

         printf("[Thread %ld] Enviando resposta: %s\n", pthread_self(), answer);
         if(write(client_resp_fd, answer, strlen(answer)) == -1) {
            if (errno == EPIPE) {
              fprintf(stderr, "[Thread %ld] Houve um Kill, Epipe foi lancado\n", pthread_self());
              return 0;
          }

           fprintf(stderr, "[Thread %ld] Falha enviar resposta unsubscribe\n", pthread_self());
           return 0;
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
            KeySubNode *current3 = clients_list[i]->subscriptions;
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
  sem_wait(&full);
  pthread_mutex_lock(&semExMut);
  Client* C = client_server_buffer[read_index];
  read_index = (read_index + 1) % MAX_SESSION_COUNT;
  pthread_mutex_unlock(&semExMut);
  sem_post(&empty);
  sem_post(&consumed);
  manage_clients(C);
}

void produce(Client *c) {
  sem_wait(&empty);
  pthread_mutex_lock(&semExMut);
  client_server_buffer[write_index] = c;
  write_index = (write_index + 1) % MAX_SESSION_COUNT;
  pthread_mutex_unlock(&semExMut);
  sem_post(&full);
  sem_wait(&consumed);
}

void* clients_loop() {

  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, SIGUSR1);
  sigaddset(&set, SIGPIPE);
  pthread_sigmask(SIG_BLOCK, &set, NULL);

  while(1) {
    consume();
  }
} 

static int dispatch_threads(DIR* dir) {
  pthread_t* threads = malloc(max_threads * sizeof(pthread_t));

  if (threads == NULL) {
    fprintf(stderr, "Failed to allocate memory for threads\n");
    return 1;
  }

  struct SharedData thread_data = {dir, jobs_directory, PTHREAD_MUTEX_INITIALIZER};
  
  for (size_t i = 0; i < max_threads; i++) {
    if (pthread_create(&threads[i], NULL, get_file, (void*)&thread_data) != 0) {
      fprintf(stderr, "Failed to create thread %zu\n", i);
      pthread_mutex_destroy(&thread_data.directory_mutex);
      free(threads);
      return 1;
    }
  }

  pthread_t manager_threads[MAX_SESSION_COUNT];

  for(int i = 0; i < MAX_SESSION_COUNT; i++) {
    pthread_create(&manager_threads[i], NULL, clients_loop, NULL);
  }

  strncat(server_pipe_path, fifo_server, strlen(fifo_server) * sizeof(char));

  if(unlink(server_pipe_path) != 0 && errno != ENOENT) {
    fprintf(stderr, "Failed to unlink server FIFO!\n");
    return 1;
  }

  if(mkfifo(server_pipe_path, 0640) != 0) {
    write_str(STDERR_FILENO, "Failed to create FIFO\n");
    unlink(server_pipe_path);
    return 1;
  }

  int fifo_fd_read = open(server_pipe_path, O_RDONLY);
  if(fifo_fd_read == -1) {
    write_str(STDERR_FILENO, "Failed to open FIFO\n");
    return 1;
  }

  // Abrir para escrita para mais clientes

  // LER A MENSAGEM DE CONNECT
  while (1) {
    char buffer[MAX_READ_SIZE];

    // Loop para mais clientes
    if(read(fifo_fd_read, buffer, MAX_READ_SIZE) == -1) { // 1|<PipeCliente(pedidos)>|<PipeCliente(respostas)>|<PipeCliente(notificacoes)>
      if (errno == EINTR) {
        if (sig_flag == 1) {
          for (int i = 0; i < MAX_SESSION_COUNT; i++) {
            if(clients_list[i] != NULL) {
              client_sudden_disconnect(clients_list[i]);
            }
          }
          sig_flag = 0;
          continue;
        }
      } else {
        write_str(STDERR_FILENO, "Error reading from FIFO\n");
      }
    }
    
    if(write_server_flag == 0) {
      int fifo_fd_write = open(server_pipe_path, O_WRONLY);
      if(fifo_fd_write == -1) {
        write_str(STDERR_FILENO, "Failed to open FIFO\n");
        return 1;
      }
      write_server_flag = 1;
    } 
      
    char* saveptr = NULL;
    char* token = strtok_r(buffer, "|", &saveptr);

    if (token == NULL || strcmp(token, "1") != 0) {
      write_str(STDERR_FILENO, "Invalid message\n");
      return 1;
    }

    char* token1 = strtok_r(NULL, "|", &saveptr);
    char* token2 = strtok_r(NULL, "|", &saveptr);
    char* token3 = strtok_r(NULL, "|", &saveptr);
    
    Client *new_client = malloc(sizeof(Client));
    new_client->client_resp_fd = open(token2, O_WRONLY);
    new_client->client_notif_fd = open(token3, O_WRONLY);
    new_client->client_req_fd = open(token1, O_RDONLY);
    new_client->subscriptions = NULL;

    char answer[MAX_WRITE_SIZE];
    snprintf(answer, MAX_WRITE_SIZE, "%d|0", OP_CODE_CONNECT);
    
    produce(new_client);
    if(write(new_client->client_resp_fd, answer, strlen(answer)) == -1) {
      fprintf(stderr, "Failed to write answer to fd: %s\n", CONNECT);
      return 1;
    }

    for (int i = 0; i < MAX_SESSION_COUNT; i++) {
      if(clients_list[i] == NULL) {
        clients_list[i] = new_client;
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
      KeySubNode *current = clients_list[i]->subscriptions;
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

  if (signal(SIGUSR1,sig_handle) == SIG_ERR) {
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
  // Loop 

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
