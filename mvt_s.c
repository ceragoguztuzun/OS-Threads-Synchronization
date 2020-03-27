#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>
#include <math.h>
#include <semaphore.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#define MAX_FILE_NAME_LEN 10
#define MAX_N 10000 
#define MAX_K 10
#define MIN_B 100
#define MAX_B 10000

int *midresult_arr;
int file_no;
int matrix_dimension;
void *reducer_fn(void *param);
void *mapper_fn(int param);
int *vector_arr;
char file_name[MAX_FILE_NAME_LEN];
int **buf;
int *full;
int *in;
int *out;
int buf_size;
int buf_read;
sem_t sem_mutex_r;
sem_t *sem_mutex_m[MAX_K];
sem_t *sem_empty[MAX_K];
sem_t *sem_full[MAX_K];

void * reducer_fn (void *resultfile)
{
  int i;
  int read_val; 
  int midresult_arr_index;
  int no_of_read_items;

  //process lines
  printf("in red %d\n",file_no);
  for (i = 0; i < file_no; ++i)
  {
    sem_wait(&sem_full[i]); 
  }
  sem_wait(&sem_mutex_r);

  // ***** READ *****
  printf("%d %d\n",full[0] ,full[1]);
  no_of_read_items = 0;
  while ( no_of_read_items < buf_size*file_no )
  {
    for(i = 0; i < file_no; ++i)
    {
      midresult_arr_index = buf[i][out[i]];
      out[i] = (out[i] + 1) %buf_size;
      read_val = buf[i][out[i]];
      out[i] = (out[i] + 1) %buf_size;

      printf("%d ... %d ... %d ( %d )\n", i, midresult_arr_index, read_val ,out[i]);
      midresult_arr[midresult_arr_index] += read_val;
      no_of_read_items += 2;
    }
  }

  sem_post(&sem_mutex_r);
  for (i = 0; i < file_no; ++i)
  {
    sem_post(&sem_empty[i]); 
  }
  printf("* \n"); 
  for (i = 0; i < matrix_dimension; ++i)
  {
    printf("READ: %d ... %d \n", i, midresult_arr[i]);
  }
  //generate file
  FILE *fp_resultfile = fopen(resultfile,"w");
  int c2;
  for( c2=0; c2 < matrix_dimension; ++c2)
  {
      fprintf(fp_resultfile, "%d\t", c2+1);
      fprintf(fp_resultfile, "%d\n", midresult_arr[c2]);
    }
    fclose(fp_resultfile);
    printf("out red \n");
   
    pthread_exit(0);
}

void * mapper_fn (int no_of_file)
{
    printf("in mp %d %d\n",no_of_file,buf_size);
  
    //generate data
    int row_no;
    int midresult_value;
    int no;
    int no_of_written_values;
    FILE *fp_mapperfile;

    sem_wait(&sem_empty[no_of_file-1]);
    for (no = 0; no < file_no; ++no)
    {
      sem_wait(&sem_mutex_m[no]);
    }
    //sem_wait(&sem_mutex_m[no_of_file-1]);

    int col_no = 0;
    sprintf(file_name, "file_%d", no_of_file);
    fp_mapperfile = fopen(file_name,"r"); 
    
    // ***** WRITE *****
    while ((fscanf (fp_mapperfile, "%d", &no)) != EOF)
    {
      row_no = no;
      if (fscanf (fp_mapperfile, "%d", &no) != EOF)
      {
        col_no = no;
      } 
      if (fscanf (fp_mapperfile, "%d", &no) != EOF)
      {
        midresult_value = no * vector_arr[col_no-1];

        buf[no_of_file-1][in[no_of_file-1]] = row_no-1;
        in[no_of_file-1] = (in[no_of_file-1] + 1 ) %buf_size;
        buf[no_of_file-1][in[no_of_file-1]] = midresult_value;
        in[no_of_file-1] = (in[no_of_file-1] + 1 ) %buf_size;

        printf("%d --> %d -> %d ( %d )\n", no_of_file-1, row_no-1, midresult_value, in[no_of_file-1]);

      }
    }
    full[no_of_file-1] = 1;
    printf("MAPPER %d FINISHED WRITING\n",no_of_file);
    /*-----------
    printf("printing BUFF for %d\n",no_of_file);
    for(no = 0; no < buf_size;++no)
    {
      printf("buf [ %d ] [ %d ] = %d \n",no_of_file-1,no,buf[no_of_file-1][no]);
    }*/
    

    //sem_post(&sem_mutex_m[no_of_file-1]);
    for (no = 0; no < file_no; ++no)
    {
      sem_post(&sem_mutex_m[no]);
    }
    sem_post(&sem_full[no_of_file-1]);
    fflush (stdout); 
  
    printf("out mp %d\n",no_of_file);
    pthread_exit(0); 
}

int main(int argc, char *argv[]) 
{   
    //variables used
    int k;
    int no;
    int i;
    int s;
    int l;
    FILE *fp_matrixfile, *fp_matrixfile1, *fp_vectorfile, *fp_vectorfile1, *k_files;

    //check of argument number
    if (argc != 6)
    {
      printf("user must enter correct number of arguments\n");
      return 0;
    }
    buf_size = atoi(argv[5]);
    k = atoi(argv[4]);
    fp_matrixfile = fopen(argv[1],"r");
    fp_matrixfile1 = fopen(argv[1],"r");
    fp_vectorfile = fopen(argv[2],"r");
    fp_vectorfile1 = fopen(argv[2],"r");

    //check if file ptrs are null
    if (fp_matrixfile == NULL || fp_vectorfile == NULL)
    {
      printf("input file is NULL\n");
      return 0;
    }

    i = 0;
    l = 0;
    //get L
    while ( (fscanf (fp_matrixfile, "%d", &no)) != EOF ) 
    {
      i++;
      if (i%3 == 0)
      {
        l++;
      }
    }
    s = ceil( (float)l/k );

    //create vector array 
    matrix_dimension = 0;
    while ((fscanf (fp_vectorfile, "%d", &no)) != EOF)
    {
      matrix_dimension++;
    }
    matrix_dimension = matrix_dimension/2;
    vector_arr = malloc(sizeof (int *) * matrix_dimension);

    i = 0;
    while ( fscanf (fp_vectorfile1, "%d", &no) != EOF && fscanf (fp_vectorfile1, "%d", &no) != EOF ) //to dodge row val
    {
      vector_arr[i] = no;
      i++;
    }

    //create K files
    file_no = 1;
    i = 0;
    k_files = NULL;
    while ( (fscanf (fp_matrixfile1, "%d", &no)) != EOF ) 
    {
      if ( i%(3*s) == 0 )
      {
        if (k_files != NULL)
        {
          fclose(k_files);
          k_files = NULL;
        }
        //create file
        sprintf(file_name, "file_%d", file_no);
        k_files = fopen(file_name, "w");
        
        if ( k_files == NULL)
        {
          printf("file %d couldn't be created!\n", file_no);
        }
        file_no++;
      }
      i++;
      
      //fill table
      fprintf(k_files, "%d\t", no);
      if (i%3 == 0)
      {
        fputs("\n", k_files);
      }
      
    }
    fclose(k_files);
    file_no--;

    midresult_arr=(int*)calloc(matrix_dimension,sizeof(int));

    //creating reducer and mapper threads
    pthread_t tid_r;
    pthread_t tid_m[file_no]; 
    int c2;

    //create semaphores
    sem_init(&sem_mutex_r, 0, 1); 
    for (i = 0; i < file_no; ++i)
    {
      sem_init(&sem_mutex_m[i], 0, 1); 
      sem_init(&sem_empty[i],0,buf_size);
      sem_init(&sem_full[i],0,0);
    }
    printf("sems opened\n");

    buf = malloc(sizeof (int *) * buf_size);
    for (int i = 0; i < buf_size; i++)
    {
      buf[i] = malloc((sizeof(int)) * buf_size);
    }

    in =(int*)calloc(file_no,sizeof(int));
    out =(int*)calloc(file_no,sizeof(int));
    full =(int*)calloc(file_no,sizeof(int));

    for (c2 = 0; c2 < file_no; ++c2)
    {
      pthread_create(&tid_m[c2], NULL, (void *) mapper_fn, c2+1);
    }
    usleep(1);                                                            
    pthread_create(&tid_r, NULL, (void *) reducer_fn, argv[3]); 
  
    //wait for threads
    for (c2 = 0; c2 < file_no; ++c2)
    {
      pthread_join(tid_m[c2], NULL);
    }
    pthread_join(tid_r, 0);
    
    sem_destroy(&sem_mutex_r);
    for (i = 0; i < file_no; ++i)
    {
      sem_close(sem_mutex_m[i]); 
      sem_close(sem_empty[i]);
      sem_close(sem_full[i]);
    }

    //free buf
    for (i = 0; i < buf_size; ++i)
    {
      free(buf[i]);
    }
    free(buf);
    printf("program done\n");

    exit(0);
}

/*  stuff 2 do
/ --------------
2. implement buffer logic:
    !!not working with buffer size < input size
3. test
*/
