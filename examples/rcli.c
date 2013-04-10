#include <rdma/rsocket.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>


int main(int argc, char *argv[])
{
   char rbuf[80];
   int n, s;
   int optval;
   socklen_t optlen = sizeof(optval);
   struct sockaddr_in srvaddr;
  
   if (argc != 2) {
      printf("Usage: %s ipaddr\n", argv[0]);
      exit(EXIT_FAILURE);
   }

   /* Create the socket */
   if ((s = rsocket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0) {
      perror("rsocket()");
      exit(EXIT_FAILURE);
   }
 
   /* Check the status for the keepalive option */
   if (rgetsockopt(s, SOL_SOCKET, SO_KEEPALIVE, &optval, &optlen) < 0) {
      perror("rgetsockopt()");
      close(s);
      exit(EXIT_FAILURE);
   }
   printf("SO_KEEPALIVE is %s\n", (optval ? "ON" : "OFF"));

   memset(&srvaddr, '0', sizeof(srvaddr));

   if (inet_pton(AF_INET, argv[1], &srvaddr.sin_addr) < 0) {
      printf("IP Conversion failed\n");
      exit(EXIT_FAILURE);
   }

   srvaddr.sin_family = AF_INET;
   srvaddr.sin_port = htons(7777);

   /* Connect */
   if (rconnect(s,(struct sockaddr*)&srvaddr,sizeof(srvaddr)) < 0) {
      perror("rconnect()");
      close(s);
      exit(EXIT_FAILURE);

   }

   /* Set the option active */
   optval = 1;
   optlen = sizeof(optval);
   if (rsetsockopt(s, SOL_SOCKET, SO_KEEPALIVE, &optval, optlen) < 0) {
      perror("rsetsockopt()");
      close(s);
      exit(EXIT_FAILURE);
   }
   printf("SO_KEEPALIVE set on socket\n");

   /* Check the status again */
   if (rgetsockopt(s, SOL_SOCKET, SO_KEEPALIVE, &optval, &optlen) < 0) {
      perror("rgetsockopt()");
      close(s);
      exit(EXIT_FAILURE);
   }
   printf("SO_KEEPALIVE is %s\n", (optval ? "ON" : "OFF"));

   memset(rbuf, '0', sizeof(rbuf));

   while ((n=rread(s,rbuf,sizeof(rbuf)-1)) > 0) {
      rbuf[n] = 0;
      if (fputs(rbuf, stdout) == EOF)
         printf("Error writing receive buffer stdout\n");
   }

   if (n<0) printf("No data\n");         

   return 0;

   exit(EXIT_SUCCESS);
}

