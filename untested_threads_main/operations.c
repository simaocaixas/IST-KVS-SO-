#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <pthread.h>

#include "kvs.h"
#include "constants.h"

#define WRLOCK_HASH_LOOP(keys, size, kvs_table, hash) \
    for (size_t i = 0; i < (size); i++) { \
        int index = (hash)((keys)[i]); \
        if (pthread_rwlock_wrlock(&(kvs_table)->hash_lock[index]) != 0) { \
            fprintf(stderr, "Failed to lock write!\n"); \
        } \
    }

#define ALL_RDLOCK_HASH_LOOP(num_pairs, kvs_table) \
    for (size_t i = 0; i < (num_pairs); i++) { \
        if (pthread_rwlock_rdlock(&(kvs_table)->hash_lock[i]) != 0) { \
            fprintf(stderr, "Failed to unlock lock!\n"); \
        } \
    }

#define RDLOCK_HASH_LOOP(keys, num_pairs, kvs_table, hash) \
    for (size_t i = 0; i < (num_pairs); i++) { \
        int index = (hash)((keys)[i]); \
        if (pthread_rwlock_rdlock(&(kvs_table)->hash_lock[index]) != 0) { \
            fprintf(stderr, "Failed to lock read!\n"); \
        } \
    }

#define UNLOCK_HASH_LOOP(keys, size, kvs_table, hash) \
    for (size_t i = 0; i < (size); i++) { \
        int index = (hash)((keys)[i]); \
        if (pthread_rwlock_unlock(&(kvs_table)->hash_lock[index]) != 0) { \
            fprintf(stderr, "Failed to unlock lock!\n"); \
        } \
    }
    
#define ALL_UNLOCK_HASH_LOOP(num_pairs, kvs_table) \
    for (size_t i = 0; i < (num_pairs); i++) { \
        if (pthread_rwlock_unlock(&(kvs_table)->hash_lock[i]) != 0) { \
            fprintf(stderr, "Failed to unlock lock!\n"); \
        } \
    }


static struct HashTable* kvs_table = NULL;


int compare_keys(const void *a, const void *b) {
  if (a != NULL && b != NULL) {
    return strcmp((const char *)a, (const char *)b);
  }
  return 0;
}

// Checks if element is in a list hashes_seen
int check_element(int *hashes_seen, size_t size, int element) {
    for (size_t i = 0; i < size; i++) {
        if (hashes_seen[i] == element) {
            return 1;
        }
    }
    return 0;
}

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

  // Sort Keys
  qsort(keys, num_pairs, MAX_STRING_SIZE, compare_keys); 

  char *sorted_keys[num_pairs];  
  int hashes_seen[num_pairs];
  size_t size = 0;
  
  // Inicializes hashes_seen with a negative number to prevent lost of hashes
  for (size_t i = 0; i < num_pairs; i++) {
    hashes_seen[i] = -10000;
  }

  // Add hash key sorted to hashes_seen
  for (size_t i = 0; i < num_pairs; i++) {
    int hash_index = hash(keys[i]);
    if (!check_element(hashes_seen, num_pairs, hash_index)) {
        hashes_seen[size] = hash_index;
        sorted_keys[size] = keys[i];
        size++; 
    }
  }

  // Lock selected hash keys of kvs_table for writing
  WRLOCK_HASH_LOOP(sorted_keys, size, kvs_table, hash);

  // Write each pair in kvs_table
  for (size_t i = 0; i < num_pairs; i++) {
    if (write_pair(kvs_table, keys[i], values[i]) != 0) {
      fprintf(stderr, "Failed to write keypair (%s,%s)\n", keys[i], values[i]);
    }
  }

  // Unock selected hash keys of kvs_table
  UNLOCK_HASH_LOOP(sorted_keys, size, kvs_table, hash);
  
  return 0;
}

int kvs_read(size_t num_pairs, char keys[][MAX_STRING_SIZE], int fd) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  // Sort Keys
  qsort(keys, num_pairs, MAX_STRING_SIZE, compare_keys);

  char *sorted_keys[num_pairs];
  int hashes_seen[num_pairs];
  size_t size = 0;
  
  // Inicializes hashes_seen with a negative number to prevent lost of hashes
  for (size_t i = 0; i < num_pairs; i++) {
    hashes_seen[i] = -1000;
  }

  // Add hash key sorted to hashes_seen
  for (size_t i = 0; i < num_pairs; i++) {
    int hash_index = hash(keys[i]);
    if (!check_element(hashes_seen, num_pairs, hash_index)) {
        hashes_seen[size] = hash_index;
        sorted_keys[size] = keys[i];
        size++; 
    }
  }

  // Lock selected hash keys of kvs_table for reading
  RDLOCK_HASH_LOOP(sorted_keys, size, kvs_table, hash);

  // Write initial '['
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

    // Write pair to fd
    if (write_to_fd(fd, line) != 0) {
      free(result);
      return 1;
    }

    free(result);
  }

  // Unlock selected hash keys of kvs_table
  UNLOCK_HASH_LOOP(sorted_keys, size, kvs_table, hash);

  // Write last ']\n' 
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

  // Sort Keys
  qsort(keys, num_pairs, MAX_STRING_SIZE, compare_keys); 

  char *sorted_keys[num_pairs]; 
  int hashes_seen[num_pairs];
  size_t size = 0;

  // Inicializes hashes_seen with a negative number to prevent lost of hashes
  for (size_t i = 0; i < num_pairs; i++) {
    hashes_seen[i] = -10000;
  }
  
  // Add hash key sorted to hashes_seen
  for (size_t i = 0; i < num_pairs; i++) {
    int hash_index = hash(keys[i]);
    if (!check_element(hashes_seen, num_pairs, hash_index)) {
        hashes_seen[size] = hash_index;
        sorted_keys[size] = keys[i];
        size++; 
    }
  }

  // Lock selected hash keys of kvs_table for writing
  WRLOCK_HASH_LOOP(sorted_keys, size, kvs_table, hash);

  int aux = 0;
  char line[MAX_STRING_SIZE];

  for (size_t i = 0; i < num_pairs; i++) {
    if (delete_pair(kvs_table, keys[i]) != 0) {
      if (!aux) {
        // Write initial "["
        if (write_to_fd(fd, "[") != 0) {
          fprintf(stderr, "Could not write\n");
          return 1;
        }
        aux = 1;
      }

      snprintf(line, MAX_STRING_SIZE, "(%s,KVSMISSING)", keys[i]);

      // Write pair to fd
      if (write_to_fd(fd, line) != 0) {
        fprintf(stderr, "Could not delete key-value\n");
        return 1;
      }
    }
  }

  if (aux) {
    // Write final ']\n'
    if (write_to_fd(fd, "]\n") != 0) {
      fprintf(stderr, "Could not write\n");
      return 1;
    }
  }

  // Unlock selected hash keys of kvs_table
  UNLOCK_HASH_LOOP(sorted_keys, size, kvs_table, hash);

  return 0;
}


void kvs_show(int fd) {

  // Lock every hash_index from kvs_table
  ALL_RDLOCK_HASH_LOOP(TABLE_SIZE, kvs_table);
  
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
      
      keyNode = keyNode->next;
    }
  }

  // Unlock every hash_index from kvs_table
  ALL_UNLOCK_HASH_LOOP(TABLE_SIZE, kvs_table);
}

void kvs_show_safe(int fd) {
  
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
      
      keyNode = keyNode->next;
    }
  }
}

int kvs_backup(char* path) {

    int fd_out = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0700);
    if (fd_out < 0) {
        perror("Failed to open backup file");
        return 1;
    }

    // No need for locks (prevents risks)
    kvs_show_safe(fd_out);
    
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



