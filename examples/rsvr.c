#include <rdma/rsocket.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/types.h>
#include <netinet/in.h>

#define MAX_CLIENTS 2

int main()
{
   char sbuf[80];
   int limit, num_clients, s, sc;
   int clients[MAX_CLIENTS];
   struct sockaddr_in srvaddr;
   time_t t;
  
   /* Create the socket */
   if ((s = rsocket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0) {
      perror("rsocket()");
      exit(EXIT_FAILURE);
   }

   /* Bind */
   memset(&srvaddr, '0', sizeof(srvaddr));
   srvaddr.sin_family = AF_INET;
   srvaddr.sin_addr.s_addr = htonl(INADDR_ANY);
   srvaddr.sin_port = htons(7777);

   if (rbind(s,(struct sockaddr*)&srvaddr,sizeof(srvaddr)) < 0) {
      perror("rbind()");
      close(s);
      exit(EXIT_FAILURE);
   }

   if (rlisten(s,MAX_CLIENTS) < 0) {
     perror("listen()");
     exit(EXIT_FAILURE);
   }

   num_clients=0;
   while(1) {

      limit=1;
      while(limit) {
         if (num_clients < MAX_CLIENTS) {
            sc = raccept(s, (struct sockaddr*)NULL, NULL); 
            clients[num_clients] = sc;
            num_clients++;
            t = time(NULL);
            snprintf(sbuf, sizeof(sbuf), "%.24s\r\n", ctime(&t));
            rwrite(sc, sbuf, strlen(sbuf)); 
         } else {
            limit=0;
         }

         sleep(1);
      }
      
      if (limit == 0 && num_clients == MAX_CLIENTS) {
         printf("Max number of clients, %i, has been reached\n", MAX_CLIENTS);
         num_clients = 9999;
      }
      sleep(1);
   }

}

