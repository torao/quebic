#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>

int main(int argc, char** argv){
  char* file_name = NULL;
  int seconds = 10;
  if(argc <= 1){
    fprintf(stderr, "USAGE: %s [file] {[sec]}\n", argv[0]);
    return EXIT_FAILURE;
  } else {
    file_name = argv[1];
    if(argc == 3){
      seconds = atoi(argv[2]);
    } else if(argc != 2){
      fprintf(stderr, "ERROR: unknown option %s\n", argv[3]);
      return EXIT_FAILURE;
    }
  }
  int fd = open(file_name, O_RDONLY);
  if(fd == -1){
    fprintf(stderr, "ERROR: fail to open file %s\n", file_name);
    return EXIT_FAILURE;
  }
  if(lockf(fd, F_LOCK, 0) < 0){
    fprintf(stderr, "ERROR: fail to lock file %s\n", file_name);
    close(fd);
    return EXIT_FAILURE;
  }
  sleep(seconds);
  lockf(fd, F_ULOCK, 0);
  close(fd);
  return EXIT_SUCCESS;
}
