#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>

#include "kvs.h"
#include "constants.h"


static struct HashTable* kvs_table = NULL;


/// writes str into file descriptor
/// @param str string to write 
/// @param fd file descriptor 
int write_to_fd(int fd, const char *str) {
    size_t len = strlen(str);
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

/// Calculates a timespec from a delay in milliseconds.
/// @param delay_ms Delay in milliseconds.
/// @return Timespec with the given delay.
static struct timespec delay_to_timespec(unsigned int delay_ms) {
  return (struct timespec){delay_ms / 1000, (delay_ms % 1000) * 1000000};
}

int kvs_init() {
  if (kvs_table != NULL) {
    fprintf(stderr, "KVS state has already been initialized\n");
    return 1;
  }

  kvs_table = create_hash_table();
  return kvs_table == NULL;
}

int kvs_terminate() {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  free_table(kvs_table);
  return 0;
}

int kvs_write(size_t num_pairs, char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE]) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  for (size_t i = 0; i < num_pairs; i++) {
    if (write_pair(kvs_table, keys[i], values[i]) != 0) {
      fprintf(stderr, "Failed to write keypair (%s,%s)\n", keys[i], values[i]);
    }
  }

  return 0;
}

int compare_keys(const void *a, const void *b) {
  return strcmp((const char *)a, (const char *)b);
}


int kvs_read(size_t num_pairs, char keys[][MAX_STRING_SIZE], int fd) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  qsort(keys, num_pairs, MAX_STRING_SIZE, compare_keys);

  // Escreve o '[' inicial
  if (write_to_fd(fd, "[") != 0) {
    return 1;
  }

  for (size_t i = 0; i < num_pairs; i++) {
    char* result = read_pair(kvs_table, keys[i]);
    char line[MAX_STRING_SIZE];

    if (result == NULL) {
      snprintf(line, MAX_STRING_SIZE, "(%s,KVSERROR)", keys[i]); 
    } else {
      snprintf(line, MAX_STRING_SIZE, "(%s,%s)", keys[i], result);
    }

    // Escreve o par atual
    if (write_to_fd(fd, line) != 0) {
      free(result);
      return 1;
    }

    free(result);
  }

  // Escreve o ']' final
  if (write_to_fd(fd, "]\n") != 0) {
    return 1;
  }

  return 0;
}

int kvs_delete(size_t num_pairs, char keys[][MAX_STRING_SIZE], int fd) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }
  
  int aux = 0;
  char line[MAX_STRING_SIZE];

  for (size_t i = 0; i < num_pairs; i++) {
    if (delete_pair(kvs_table, keys[i]) != 0) {
      if (!aux) {
        // Escreve o "[" inicial
        if (write_to_fd(fd, "[") != 0) {
          fprintf(stderr, "Could not write\n");
          return 1;
        }
        aux = 1;
      }

      snprintf(line, MAX_STRING_SIZE, "(%s,KVSMISSING)", keys[i]);

      // Escreve o par atual
      if (write_to_fd(fd, line) != 0) {
        fprintf(stderr, "Could not delete key-value\n");
        return 1; // Sai imediatamente se ocorrer falha no write
      }
    }
  }

  if (aux) {
    // Escreve o "]\n" final
    if (write_to_fd(fd, "]\n") != 0) {
      fprintf(stderr, "Could not write\n");
      return 1;
    }
  }

  return 0;
}


void kvs_show(int fd) {

  for (int i = 0; i < TABLE_SIZE; i++) {
    KeyNode *keyNode = kvs_table->table[i];
    while (keyNode != NULL) {
      char buffer[MAX_WRITE_SIZE];
      int written = snprintf(buffer, MAX_WRITE_SIZE, "(%s, %s)\n", keyNode->key, keyNode->value);
      
      if (written < 0) {
        fprintf(stderr, "Failed to format data\n");
        return;
      }

      if (write_to_fd(fd, buffer) != 0) {
        perror("Failed to write to file");
        return;
      }
      
      keyNode = keyNode->next; // Move to the next node
    }
  }
}

int kvs_backup(char* dir_name, char* file_name, int total_backups) {

    char line[MAX_STRING_SIZE];
    char path[MAX_STRING_SIZE];

    char *dot = strrchr(file_name, '.');  //APAGA O .JOB OMEUDEUS
    if (dot && strcmp(dot, ".job") == 0) {
        *dot = '\0';
    }

    // Verifica se o tamanho dos argumentos não excede o tamanho do buffer
    if (snprintf(line, MAX_STRING_SIZE, "%s-%d", file_name, total_backups) >= MAX_STRING_SIZE) {
        fprintf(stderr, "Error: File name exceeds buffer size.\n");
        return 1;
    }

    if (snprintf(path, MAX_STRING_SIZE, "%s/%s.bck", dir_name, line) >= MAX_STRING_SIZE) {
        fprintf(stderr, "Error: Path exceeds buffer size.\n");
        return 1;
    }

    int fd_out = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0700);
    if (fd_out < 0) {
        perror("Failed to open backup file");
        return 1;
    }
    kvs_show(fd_out);
    if (close(fd_out) < 0) {
        fprintf(stderr, "Failed to close backup file\n");
        return 1;
    } 
    return 0;
}

void kvs_wait(unsigned int delay_ms) {
  struct timespec delay = delay_to_timespec(delay_ms);
  nanosleep(&delay, NULL);
}