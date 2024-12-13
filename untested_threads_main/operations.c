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

// * MACROS * //

// WRITE LOCKS 
#define WRLOCK_HASH_LOOP(sorted_keys, size, kvs_table, hash) \
    for (size_t i = 0; i < (size); i++) { \
        int index = (hash)((sorted_keys)[i]); \
        if (pthread_rwlock_wrlock(&(kvs_table)->hash_lock[index]) != 0) { \
            fprintf(stderr, "Failed to lock write!\n"); \
        } \
    }

// READ LOCKS EVERY HASH
#define ALL_RDLOCK_HASH_LOOP(num_pairs, kvs_table) \
    for (size_t i = 0; i < (num_pairs); i++) { \
        if (pthread_rwlock_rdlock(&(kvs_table)->hash_lock[i]) != 0) { \
            fprintf(stderr, "Failed to unlock lock!\n"); \
        } \
    }

// READ LOCKS
#define RDLOCK_HASH_LOOP(keys, num_pairs, kvs_table, hash) \
    for (size_t i = 0; i < (num_pairs); i++) { \
        int index = (hash)((keys)[i]); \
        if (pthread_rwlock_rdlock(&(kvs_table)->hash_lock[index]) != 0) { \
            fprintf(stderr, "Failed to lock read!\n"); \
        } \
    }

// UNLOCK LOCKS
#define UNLOCK_HASH_LOOP(sorted_keys, size, kvs_table, hash) \
    for (size_t i = 0; i < (size); i++) { \
        int index = (hash)((sorted_keys)[i]); \
        if (pthread_rwlock_unlock(&(kvs_table)->hash_lock[index]) != 0) { \
            fprintf(stderr, "Failed to unlock lock!\n"); \
        } \
    }
    
// UNLOCK LOCKS EVERY HASH
#define ALL_UNLOCK_HASH_LOOP(num_pairs, kvs_table) \
    for (size_t i = 0; i < (num_pairs); i++) { \
        if (pthread_rwlock_unlock(&(kvs_table)->hash_lock[i]) != 0) { \
            fprintf(stderr, "Failed to unlock lock!\n"); \
        } \
    }

static struct HashTable* kvs_table = NULL;

/**
 * Compares two keys for kvs_write and kvs_delete. Compares a char of chars.
 * 
 * @param a Pointer to the first key. 
 * @param b Pointer to the second key.
 * @return Negative value if a < b, zero if a == b, positive value if a > b.
 */
int compare_keys(const void *a, const void *b) {
  if (a != NULL && b != NULL) {
    return strcmp(*(const char **)a, *(const char **)b);
  }
  return 0;
}

/**
 * Compares two keys for kvs_read. Compares a char of strings.
 * 
 * @param a Pointer to the first key.
 * @param b Pointer to the second key.
 * @return Negative value if a < b, zero if a == b, positive value if a > b.
 */
int compare_keys_read(const void *a, const void *b) {
  if (a != NULL && b != NULL) {
    return strcmp((const char *)a, (const char *)b);
  }
  return 0;
}

/**
 * Checks if an element is in a list of hashes_seen to prevent deadlocks.
 * 
 * @param hashes_seen Array of seen hashes.
 * @param size Size of the array.
 * @param element Element to check.
 * @return 1 if the element is in the list, 0 otherwise.
 */
int check_element(int *hashes_seen, size_t size, int element) {
    for (size_t i = 0; i < size; i++) {
        if (hashes_seen[i] == element) {
            return 1;
        }
    }
    return 0;
}

/// Checks if hash is valid
/// @param hash string to write
/// @return 1 if hash is valid, 0 otherwise 
int check_hash(int hash) {
  if(hash < 0 || hash > 25) {
    fprintf(stderr, "Invalid hash value!\n");
    return 0;
  }
  return 1;
}

/**
 * Writes a string to a file descriptor.
 * 
 * @param fd File descriptor.
 * @param str String to write.
 * @return 0 on success, 1 on failure.
 */
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

/**
 * Initializes the KVS state.
 * 
 * @return 0 on success, 1 if the KVS state has already been initialized.
 */
int kvs_init() {
  if (kvs_table != NULL) {
    fprintf(stderr, "KVS state has already been initialized\n");
    return 1;
  }

  kvs_table = create_hash_table();

  return kvs_table == NULL;
}

/**
 * Terminates the KVS state.
 * 
 * @return 0 on success, 1 if the KVS state has not been initialized.
 */
int kvs_terminate() {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  free_table(kvs_table);
  return 0;
}

/**
 * Writes key-value pairs to the KVS.
 * 
 * @param num_pairs Number of key-value pairs.
 * @param keys Array of keys.
 * @param values Array of values.
 * @return 0 on success, 1 on failure.
 */
int kvs_write(size_t num_pairs, char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE]) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  char *sorted_keys[num_pairs];  
  char *approved_keys[num_pairs];  
  char *approved_values[num_pairs];  

  int hashes_seen[num_pairs];
  size_t size = 0;
  size_t approved_index = 0;

  // Inicializes hashes_seen with a negative number to prevent lost of hashes
  for (size_t i = 0; i < num_pairs; i++) {
    hashes_seen[i] = -10000;
  }

  // Add hash key sorted to hashes_seen
  for (size_t i = 0; i < num_pairs; i++) {
    int hash_index = hash(keys[i]);

    if (check_hash(hash_index) == 1) {
      approved_keys[approved_index] = keys[i];
      approved_values[approved_index] = values[i];
      approved_index++;
      
      if (!check_element(hashes_seen, num_pairs, hash_index)) {
        hashes_seen[size] = hash_index;
        sorted_keys[size] = keys[i];
        size++;       
      }
    }
  }

  // Sort Keys 
  qsort(sorted_keys, size, sizeof(char *), compare_keys); 

  // Lock selected hash keys of kvs_table for writing
  WRLOCK_HASH_LOOP(sorted_keys, size, kvs_table, hash);

  // Write each pair in kvs_table
  for (size_t i = 0; i < approved_index; i++) {
    if (write_pair(kvs_table, approved_keys[i], approved_values[i]) != 0) {
      fprintf(stderr, "Failed to write keypair (%s,%s)\n", approved_keys[i], approved_values[i]);
    }
  }

  // Unock selected hash keys of kvs_table
  UNLOCK_HASH_LOOP(sorted_keys, size, kvs_table, hash);
  
  return 0;
}

/**
 * Reads key-value pairs from the KVS.
 * 
 * @param num_pairs Number of keys.
 * @param keys Array of keys.
 * @param fd File descriptor to write the output.
 * @return 0 on success, 1 on failure.
 */
int kvs_read(size_t num_pairs, char keys[][MAX_STRING_SIZE], int fd) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }
  
  // Sort Keys
  qsort(keys, num_pairs, MAX_STRING_SIZE, compare_keys_read);
  
  char *sorted_keys[num_pairs];  
  char *approved_keys[num_pairs];  

  int hashes_seen[num_pairs];
  size_t size = 0;
  size_t approved_index = 0;
  
  // Inicializes hashes_seen with a negative number to prevent lost of hashes
  for (size_t i = 0; i < num_pairs; i++) {
    hashes_seen[i] = -10000;
  }

  // Add hash key sorted to hashes_seen
  for (size_t i = 0; i < num_pairs; i++) {
    int hash_index = hash(keys[i]);

    if (check_hash(hash_index) == 1) {
      approved_keys[approved_index] = keys[i];
      approved_index++;
      
      if (!check_element(hashes_seen, num_pairs, hash_index)) {
        hashes_seen[size] = hash_index;
        sorted_keys[size] = keys[i];
        size++;       
      }
    }
  }
 
  // Lock selected hash keys of kvs_table for reading
  RDLOCK_HASH_LOOP(sorted_keys, size, kvs_table, hash);

  // Write initial '['
  if (write_to_fd(fd, "[") != 0) {
    return 1;
  }

  for (size_t i = 0; i < approved_index; i++) {
    char* result = read_pair(kvs_table, approved_keys[i]);
    char line[MAX_STRING_SIZE];

    if (result == NULL) {
      snprintf(line, MAX_STRING_SIZE, "(%s,KVSERROR)", approved_keys[i]); 
    } else {
      snprintf(line, MAX_STRING_SIZE, "(%s,%s)", approved_keys[i], result);
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

/**
 * Deletes key-value pairs from the KVS.
 * 
 * @param num_pairs Number of keys.
 * @param keys Array of keys.
 * @param fd File descriptor to write the output.
 * @return 0 on success, 1 on failure.
 */
int kvs_delete(size_t num_pairs, char keys[][MAX_STRING_SIZE], int fd) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  char *sorted_keys[num_pairs];  
  char *approved_keys[num_pairs];  

  int hashes_seen[num_pairs];
  size_t size = 0;
  size_t approved_index = 0;
  
  // Inicializes hashes_seen with a negative number to prevent lost of hashes
  for (size_t i = 0; i < num_pairs; i++) {
    hashes_seen[i] = -10000;
  }

  // Add hash key sorted to hashes_seen
  for (size_t i = 0; i < num_pairs; i++) {
    int hash_index = hash(keys[i]);

    if (check_hash(hash_index) == 1) {
      approved_keys[approved_index] = keys[i];
      approved_index++;
      
      if (!check_element(hashes_seen, num_pairs, hash_index)) {
        hashes_seen[size] = hash_index;
        sorted_keys[size] = keys[i];
        size++;       
      }
    }
  }

  // Sort Keys
  qsort(sorted_keys, size, sizeof(char *), compare_keys); 

  // Lock selected hash keys of kvs_table for writing
  WRLOCK_HASH_LOOP(sorted_keys, size, kvs_table, hash);

  int aux = 0;
  char line[MAX_WRITE_SIZE];

  for (size_t i = 0; i < approved_index; i++) {
    if (delete_pair(kvs_table, approved_keys[i]) != 0) {
      if (!aux) {
        // Write initial "["
        if (write_to_fd(fd, "[") != 0) {
          fprintf(stderr, "Could not write\n");
          return 1;
        }
        aux = 1;
      }

      snprintf(line, MAX_WRITE_SIZE, "(%s,KVSMISSING)", approved_keys[i]);

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

/**
 * Shows all key-value pairs in the KVS.
 * 
 * @param fd File descriptor to write the output.
 */
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

/**
 * Shows all key-value pairs in the KVS without locking.
 * 
 * @param fd File descriptor to write the output.
 */
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

/**
 * Creates a backup of the KVS.
 * 
 * @param path Path to the backup file.
 * @return 0 on success, 1 on failure.
 */
int kvs_backup(char* path) {
    // this is only executed by a child process
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

/**
 * Waits for a specified delay.
 * 
 * @param delay_ms Delay in milliseconds.
 */
void kvs_wait(unsigned int delay_ms) {
  struct timespec delay = delay_to_timespec(delay_ms);
  nanosleep(&delay, NULL);
}



