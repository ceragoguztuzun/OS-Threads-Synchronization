#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>
#include <math.h>
#include <semaphore.h>

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
FILE *fp_mapperfile;
int **buf;
int *flags;
int buf_size;
int buf_read;
int cond = 0;
sem_t sem_mutex_r;
sem_t sem_mutex_m;

void * reducer_fn (void *resultfile)
{
	int i;
	int j;
	int read_val; 
	int midresult_arr_index;

	//process lines
	printf("in red %d %d\n",cond,file_no);
	//pthread_mutex_lock( &mutex2);  
	sem_wait(&sem_mutex_r);
	

	printf("cond: %d\n",cond);

	for(i = 0; i < file_no; ++i)
	{
		buf_read = 0;
		while ( buf_read +1 < buf_size && flags[i][buf_read] != 1)
		{
			midresult_arr_index = buf[i][buf_read];
			read_val = buf[i][buf_read+1];

		  printf("%d ... %d ... %d \n", i, midresult_arr_index, read_val );
		 	midresult_arr[midresult_arr_index] += read_val;
		  buf_read += 2;
		}
	}

	//pthread_mutex_unlock( &mutex2);
	sem_post(&sem_mutex_r);
	printf("* \n"); 
  
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
  	printf("in mp %d\n",no_of_file);
  
  	//generate data
  	int row_no;
  	int midresult_value;
  	int no;

  	//pthread_mutex_lock( &mutex1 );
  	sem_wait(&sem_mutex_m);

  	int col_no = 0;
  	buf_read = 0;
  	sprintf(file_name, "file_%d", no_of_file);
  	fp_mapperfile = fopen(file_name,"r"); 
	  
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
  			if (buf_read +2 < buf_size)
  			{
  			 buf[no_of_file-1][buf_read++] = row_no-1;
  			 buf[no_of_file-1][buf_read++] = midresult_value;
  			 printf("%d --> %d -> %d \n", no_of_file-1,row_no-1,midresult_value);
  			}
        if (buf_read + 1== buf_size)
        {
          //meaning buffer for mapper is full.
          flags[no_of_file-1][buf_read] = 1;
        }
  		}
  	}
  	cond++; 
  	//------------
  	printf("printing BUFF for %d\n",no_of_file);
  	for(no = 0; no < buf_size;++no)
  	{
  		printf("buf [ %d ] [ %d ] = %d \n",no_of_file-1,no,buf[no_of_file-1][no]);
  	}
  	
  	//pthread_mutex_unlock( &mutex1 );
  	sem_post(&sem_mutex_m);
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
  	struct timeval tvs;
  	struct timeval tve;
  	unsigned long reportedTime = 0;

  	//gettimeofday(&tvs, NULL); 
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
    for (i = 0; i < matrix_dimension; ++i)
    {
      printf("v: %d\n",vector_arr[i]);
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
  	sem_init(&sem_mutex_m, 0, 1); 
  	sem_init(&sem_mutex_r, 0, 1); 

  	//buf[file_no][buf_size];
    flags =(int*)calloc(file_no,sizeof(int));
  	buf = malloc(sizeof (int *) * buf_size);
  	for (int i = 0; i < buf_size; i++)
  	{
    	buf[i] = malloc((sizeof(int)) * buf_size);
  	}

  	//pthread_mutex_lock( &mutex2);
  	for (c2 = 0; c2 < file_no; ++c2)
  	{
    	pthread_create(&tid_m[c2], NULL, (void *) mapper_fn, c2+1);
  	}
  	sleep(1);															//remove this later!!!!
  	pthread_create(&tid_r, NULL, (void *) reducer_fn, argv[3]); 
  
  	//wait for threads
  	for (c2 = 0; c2 < file_no; ++c2)
  	{
  		pthread_join(tid_m[c2], NULL);
  	}
  	pthread_join(tid_r, 0);
  	
  	sem_destroy(&sem_mutex_m);
  	sem_destroy(&sem_mutex_r);
  	
    //free buf
  	for (i = 0; i < buf_size; ++i)
  	{
  		free(buf[i]);
  	}
  	free(buf);
  	printf("program done\n");

  	//calculate time
  	//gettimeofday(&tve, NULL);
  	//reportedTime = tve.tv_usec - tvs.tv_usec;
  	//printf("COST OF MVT.C \nmatrix dimension : %d\nk : %d\ntime elapsed: %ld\n", matrix_dimension, k, reportedTime); 

  	exit(0);
}

/*	stuff 2 do
/	-----------
/	reducer should wait for all mappers to finish
/  concurracy
/	buffer logic -> linked list 
/	TEST
*/
