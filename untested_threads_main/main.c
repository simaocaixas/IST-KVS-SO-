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

int max_backups, max_threads;
int backup_counter = 0;
DIR *dir;
struct dirent *entry;
pthread_mutex_t dir_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t backup_lock = PTHREAD_MUTEX_INITIALIZER;

int parse_file(int fd_in,int fd_out,const char *dir_name,char* file_name,int total_backups) {

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

          pthread_mutex_lock(&backup_lock);
          
          if(max_backups == 0) {
            fprintf(stderr, "There are no available processes to begin backup!\n");
            pthread_mutex_unlock(&backup_lock);
            return 0;
          }

          if (backup_counter >= max_backups) {
              int status;
              pid_t finished_pid = wait(&status); 
              if (finished_pid > 0) {
                  if (WIFEXITED(status) != 1) {
                      fprintf(stderr, "Backup process %d terminated abnormally.\n", finished_pid);
                      pthread_mutex_unlock(&backup_lock);
                      return 0;
                  } 
                backup_counter--;
              }
          }
    
          char line[MAX_STRING_SIZE];
          char path[MAX_STRING_SIZE];

          char *dot = strrchr(file_name, '.');  
          if (dot && strcmp(dot, ".job") == 0) {
              *dot = '\0';
          } 

          // Verifica se o tamanho dos argumentos não excede o tamanho do buffer
          if (snprintf(line, MAX_STRING_SIZE, "%s-%d", file_name, total_backups + 1) >= MAX_STRING_SIZE) {
            fprintf(stderr, "Error: File name exceeds buffer size.\n");
            return 1;
          }

          if (snprintf(path, MAX_STRING_SIZE, "%s/%s.bck", dir_name, line) >= MAX_STRING_SIZE) {
            fprintf(stderr, "Error: Path exceeds buffer size.\n");
            return 1;
          }

          pid_t pid = fork();
          if (pid < 0) {
              fprintf(stderr, "Failed to fork.\n");
              pthread_mutex_unlock(&backup_lock);
              return 0;
          } else if (pid == 0) {

              if (kvs_backup(path)) {
                  fprintf(stderr, "Failed to perform backup.\n");
                  pthread_mutex_unlock(&backup_lock);
                  close(fd_out);
                  close(fd_in);
                  closedir(dir);
                  kvs_terminate();
                  exit(1); 
              }
              close(fd_out); 
              close(fd_in);
              pthread_mutex_unlock(&backup_lock);
              kvs_terminate();
              closedir(dir);
              exit(0); 
          } else {
              total_backups++;
              backup_counter++; 
          }
          pthread_mutex_unlock(&backup_lock);
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

int generate_paths(const char *dir_name, struct dirent *dir_entry, char *in_path, char *out_path) {
    char *file_name = dir_entry->d_name;

    // Check if the file has ".job" extension
    char *ext = strstr(file_name, ".job");
    if (!ext || strcmp(ext, ".job") != 0) {
        return 0; // Not a .job file
    }

    // Build input path: combine directory name and file name
    if (snprintf(in_path, MAX_JOB_FILE_NAME_SIZE, "%s/%s", dir_name, file_name) >= MAX_JOB_FILE_NAME_SIZE) {
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

void *process_file(void *arg) {
    struct dirent *local_entry;
    const char *dir_name = (const char*)arg;
    char in_path[MAX_JOB_FILE_NAME_SIZE], out_path[MAX_JOB_FILE_NAME_SIZE];

    while (1) {
        pthread_mutex_lock(&dir_lock);
        local_entry = readdir(dir);
        pthread_mutex_unlock(&dir_lock);

        if (local_entry == NULL) {
            break;
        }

        if (generate_paths(dir_name, local_entry, in_path, out_path)) {
            int fd_in = open(in_path, O_RDONLY);
            int fd_out = open(out_path, O_WRONLY | O_CREAT, 00700);
            int total_backups = 0;
            parse_file(fd_in, fd_out, dir_name, local_entry->d_name, total_backups);

            if (close(fd_in) < 0) {
              fprintf(stderr, "Failed to close file...\n");
            } 

            if (close(fd_out) < 0) {
              fprintf(stderr, "Failed to close file...\n");
            }
        }
      }
    return NULL;
}

int main(int argc, char** argv) {
  
  // checks if correct number of parameters 
  if (argc < 3) {
      fprintf(stderr, "Usage: %s <arg1> <max_backups>\n", argv[0]);
      return 1;
  }

  if (kvs_init()) {
    fprintf(stderr, "Failed to initialize KVS\n");
    return 1;
  }

  char *dir_name = argv[1];             
  
  // max_threads and max_backups minimum values should be 1,0
  max_backups = atoi(argv[2]);
  max_threads = atoi(argv[3]);
  pthread_t threads[max_threads];
  
  if ((dir = opendir(dir_name)) == NULL) {
    perror("Failed to open directory");
    return 1;
  }

  // create all threads and each one will start processing a file from dir
  for (int i = 0; i < max_threads; i++) {
    
    if (pthread_create(&threads[i], NULL, process_file,(void*)dir_name) != 0) {
      perror("Failed to create thread");
      closedir(dir);
      continue;
    }
  }

  for (int i = 0; i < max_threads; i++) {
    pthread_join(threads[i], NULL);
  }

  while (wait(NULL) > 0); 

  closedir(dir);
  kvs_terminate();

  return 0;
}
