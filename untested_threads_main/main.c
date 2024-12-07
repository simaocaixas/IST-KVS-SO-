#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <dirent.h>
#include <string.h>
#include <fcntl.h>
#include <sys/wait.h>
#include <pthread.h>


#include "constants.h"
#include "parser.h"
#include "operations.h"

int max_backups;
int backup_counter = 0;
int max_threads;

typedef struct {
  char *dir_name;
  struct dirent *entry;
  char in_path[MAX_JOB_FILE_NAME_SIZE];
  char out_path[MAX_JOB_FILE_NAME_SIZE];

} thread_args;


int parse_file(int fd_in,int fd_out,char* dir_name,char* file_name,int total_backups) {

  while (1) {
    char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    unsigned int delay;
    size_t num_pairs;

    const char *help_string = 
      "Available commands:\n"
      "  WRITE [(key,value),(key2,value2),...]\n"
      "  READ [key,key2,...]\n"
      "  DELETE [key,key2,...]\n"
      "  SHOW\n"
      "  WAIT <delay_ms>\n"
      "  BACKUP\n"
      "  HELP\n";

    fflush(stdout);

    switch (get_next(fd_in)) {

      case CMD_WRITE:
        num_pairs = parse_write(fd_in, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
        if (num_pairs == 0) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }

        sort_key_value_pairs(keys, values, num_pairs);

        if (kvs_write(num_pairs, keys, values)) {
          fprintf(stderr, "Failed to write pair\n");
        }

        break;

      case CMD_READ:
        num_pairs = parse_read_delete(fd_in, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

        if (num_pairs == 0) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (kvs_read(num_pairs, keys, fd_out)) {
          fprintf(stderr, "Failed to read pair\n");
        }
        break;

      case CMD_DELETE:
        num_pairs = parse_read_delete(fd_in, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

        if (num_pairs == 0) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (kvs_delete(num_pairs, keys, fd_out)) {
          fprintf(stderr, "Failed to delete pair\n");
        }
        break;

      case CMD_SHOW:

        kvs_show(fd_out);
        break;

      case CMD_WAIT:
        if (parse_wait(fd_in, &delay, NULL) == -1) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (delay > 0) {

        char line[] = "Waiting...\n";
        size_t lineSize = strlen(line);
        size_t total_written = 0;

        while (total_written < lineSize) {
            ssize_t written = write(fd_out, line + total_written, lineSize - total_written);
            if (written < 0) {
                fprintf(stderr, "Was not able to wait!\n");
                break;
            }
            total_written += (size_t)written;
        }

        kvs_wait(delay);
        }
        break;

      case CMD_BACKUP: {
          if (backup_counter >= max_backups) {
              int status;
              pid_t finished_pid = wait(&status); 
              if (finished_pid > 0) {
                  if (WIFEXITED(status) != 1) {
                      fprintf(stderr, "Backup process %d terminated abnormally.\n", finished_pid);
                      return 0;
                  } 
                backup_counter--;
              }
          }

          pid_t pid = fork();
          if (pid < 0) {
              fprintf(stderr, "Failed to fork.\n");
              return 0;
          } else if (pid == 0) {
              
              if (kvs_backup(dir_name,file_name,total_backups + 1)) {
                  fprintf(stderr, "Failed to perform backup.\n");
                  exit(1); 
              }
              exit(0); 
          } else {
              total_backups++;
              backup_counter++; 
          }
          break;
      }
      case CMD_INVALID:
        fprintf(stderr, "Invalid command. See HELP for usage\n");
        break;

      case CMD_HELP: 

          if (write(fd_out, help_string, strlen(help_string)) < 1) {
              fprintf(stderr, "Failed to write to output. See HELP for usage\n");
              break;
          }

          break;
          
      case CMD_EMPTY:
        break;

      case EOC:
        return 0;
    }
  }

  return 1;
}

int generate_paths(char *dir_name, struct dirent *entry, char *in_path, char *out_path) {
  char *file_name = entry->d_name;

  // Check if the file has ".job" extension
  char *ext = strstr(file_name, ".job");
  if (!ext || strcmp(ext, ".job") != 0) {
      return 0; // Not a .job file
  }

  // Build input path: combine directory name and file name
  if (snprintf(in_path, MAX_JOB_FILE_NAME_SIZE, "%s/%s", dir_name, file_name) >= MAX_JOB_FILE_NAME_SIZE) { // man snprintf says that we will write in in_path to maximum size MAX_JOB_FILE_NAME_SIZE in this format "%s/%s" dir_name, file_name so "dir_name/file_name"
      fprintf(stderr, "Error: Path exceeds buffer size.\n");
      return 0;
  }

  // Build output path by replacing ".job" with ".out"
  strncpy(out_path, in_path, MAX_JOB_FILE_NAME_SIZE);
  char *out_ext = strstr(out_path, ".job");
  if (out_ext) {
      strncpy(out_ext, ".out", (size_t)(MAX_JOB_FILE_NAME_SIZE - (out_ext - out_path)));
  }

  return 1;
}

void process_file(void *args) {
  
  thread_args* t_args = (thread_args*) args;

  if (generate_paths(t_args->dir_name, t_args->entry, t_args->in_path, t_args->out_path)) {    
      int fd_in = open(t_args->in_path, O_RDONLY);      
      int fd_out = open(t_args->out_path, O_WRONLY | O_CREAT, 00700);
      int total_backups = 0;
      parse_file(fd_in, fd_out, t_args->dir_name, t_args->entry->d_name, total_backups);
      if (close(fd_in) || close(fd_out) < 0) {
        fprintf(stderr, "Failed to close in file...\n");
      }      
  }
}

void sort_key_value_pairs(char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE], size_t num_pairs) {
    for (size_t i = 0; i < num_pairs - 1; i++) {
        for (size_t j = 0; j < num_pairs - i - 1; j++) {
            if (strcmp(keys[j], keys[j + 1]) > 0) {
                
                char temp_key[MAX_STRING_SIZE];
                strcpy(temp_key, keys[j]);
                strcpy(keys[j], keys[j + 1]);
                strcpy(keys[j + 1], temp_key);

                char temp_value[MAX_STRING_SIZE];
                strcpy(temp_value, values[j]);
                strcpy(values[j], values[j + 1]);
                strcpy(values[j + 1], temp_value);
            }
        }
    }
}


int main(int argc, char** argv) {

  if (argc < 3) {
      fprintf(stderr, "Usage: %s <arg1> <max_backups>\n", argv[0]);
      return 1;
  }

  if (kvs_init()) {
    fprintf(stderr, "Failed to initialize KVS\n");
    return 1;
  }

  max_threads = atoi(argv[3]);
  max_backups = atoi(argv[2]); 
  char *dir_name = argv[1];
  
  pthread_t threads[max_threads];
  DIR *dir = opendir(dir_name);

  if (!dir) {
    perror("Failed to open directory");
    return 1;
  }

  struct dirent *entry;
  int active_threads = 0;
  

  while ((entry = readdir(dir)) != NULL) {

        thread_args *args = malloc(sizeof(thread_args));
        
        if (!args) {
            fprintf(stderr, "Failed to allocate memory for args\n");
            return 1;
        }

        args->dir_name = malloc(strlen(dir_name) + 1);
        if (!args->dir_name) {
          fprintf(stderr, "Failed to allocate memory for dir_name\n");
          free(args);
          continue;
        }

        strcpy(args->dir_name, dir_name);
        memcpy(&args->entry, entry, sizeof(struct dirent));

        if (pthread_create(&threads[active_threads], NULL, process_file, args) != 0) {
            perror("Failed to create thread");
            free(args->dir_name);
            free(args);
            continue;
        }
        
        active_threads++;

        // Melhorar esta parte!!!!!!
        if (active_threads >= max_threads) {
          for (int i = 0; i < active_threads; i++)
              pthread_join(threads[i], NULL);
          active_threads = 0;
        }
  }

  kvs_terminate();
  closedir(dir);
    
}
