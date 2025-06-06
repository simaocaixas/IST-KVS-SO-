#include "kvs.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "string.h"
// Hash function based on key initial.
// @param key Lowercase alphabetical string.
// @return hash.
// NOTE: This is not an ideal hash function, but is useful for test purposes of the project
int hash(const char *key) {
  int firstLetter = tolower(key[0]);
  if (firstLetter >= 'a' && firstLetter <= 'z') {
    return firstLetter - 'a';
  } else if (firstLetter >= '0' && firstLetter <= '9') {
    return firstLetter - '0';
  }
  return -1;  // Invalid index for non-alphabetic or number strings
}

struct HashTable *create_hash_table() {
  HashTable *ht = malloc(sizeof(HashTable));
  if (!ht) return NULL;
  for (int i = 0; i < TABLE_SIZE; i++) {
    ht->table[i] = NULL;
  }
  pthread_rwlock_init(&ht->tablelock, NULL);
  return ht;
}

int notify_fds(int notifications[MAX_SESSION_COUNT], const char *key, const char *value, int bit) {
  // Declaração de um buffer para armazenar a mensagem a ser enviada.
  char buffer[MAX_STRING_SIZE];

  // Cria a mensagem a ser enviada com base no valor de 'bit'.
  if (bit == 0) {
    // Caso 'bit' seja 0, indica que a chave foi alterada.
    snprintf(buffer, MAX_STRING_SIZE, "(%s,%s)", key, value);
  } else {
    // Caso 'bit' seja diferente de 0, indica que a chave foi eliminada.
    snprintf(buffer, MAX_STRING_SIZE, "(%s,DELETED)", key);
  }

  // Itera sobre todos os descritores de notificação.
  for (int i = 0; i < MAX_SESSION_COUNT; i++) {
    // Verifica se o descritor é válido (maior que 0).
    if (notifications[i] > 0) {
      // Escreve a mensagem no descritor e verifica erros.
      if (write(notifications[i], buffer, MAX_STRING_SIZE) == -1) {
        return 1;  // Retorna erro se a escrita falhar.
      }
    }
  }

  return 0;
}

int write_pair(HashTable *ht, const char *key, const char *value) {
  int index = hash(key);

  // Search for the key node
  KeyNode *keyNode = ht->table[index];
  KeyNode *previousNode;

  while (keyNode != NULL) {
    if (strcmp(keyNode->key, key) == 0) {
      // overwrite value
      free(keyNode->value);
      keyNode->value = strdup(value);
      notify_fds(keyNode->notifications, key, value, 0);
      return 0;
    }
    previousNode = keyNode;
    keyNode = previousNode->next;  // Move to the next node
  }
  // Key not found, create a new key node
  keyNode = malloc(sizeof(KeyNode));
  keyNode->key = strdup(key);      // Allocate memory for the key
  keyNode->value = strdup(value);  // Allocate memory for the value

  // Initializes every entry on notifications as empty with -3
  for (int i = 0; i < MAX_SESSION_COUNT; i++) {
    keyNode->notifications[i] = -3;
  }

  keyNode->next = ht->table[index];  // Link to existing nodes
  ht->table[index] = keyNode;        // Place new key node at the start of the list
  return 0;
}

char *read_pair(HashTable *ht, const char *key) {
  int index = hash(key);

  KeyNode *keyNode = ht->table[index];
  KeyNode *previousNode;
  char *value;

  while (keyNode != NULL) {
    if (strcmp(keyNode->key, key) == 0) {
      value = strdup(keyNode->value);
      return value;  // Return the value if found
    }
    previousNode = keyNode;
    keyNode = previousNode->next;  // Move to the next node
  }

  return NULL;  // Key not found
}

int delete_pair(HashTable *ht, const char *key) {
  int index = hash(key);

  // Search for the key node
  KeyNode *keyNode = ht->table[index];
  KeyNode *prevNode = NULL;

  while (keyNode != NULL) {
    if (strcmp(keyNode->key, key) == 0) {
      // Key found; delete this node
      if (prevNode == NULL) {
        // Node to delete is the first node in the list
        ht->table[index] = keyNode->next;  // Update the table to point to the next node
      } else {
        // Node to delete is not the first; bypass it
        prevNode->next = keyNode->next;  // Link the previous node to the next node
      }

      // Notifies every descriptor of every client subscribed to the key
      notify_fds(keyNode->notifications, key, NULL, 1);

      // Free the memory allocated for the key and value
      free(keyNode->key);
      free(keyNode->value);
      free(keyNode);  // Free the key node itself
      return 0;       // Exit the function
    }
    prevNode = keyNode;       // Move prevNode to current node
    keyNode = keyNode->next;  // Move to the next node
  }

  return 1;
}

void free_table(HashTable *ht) {
  for (int i = 0; i < TABLE_SIZE; i++) {
    KeyNode *keyNode = ht->table[i];
    while (keyNode != NULL) {
      KeyNode *temp = keyNode;
      keyNode = keyNode->next;
      free(temp->key);
      free(temp->value);
      free(temp);
    }
  }
  pthread_rwlock_destroy(&ht->tablelock);
  free(ht);
}
