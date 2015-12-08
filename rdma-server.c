#include "rdma-common.h"

static int on_connect_request(struct rdma_cm_id *id);
static int on_connection(struct rdma_cm_id *id);
static int on_disconnect(struct rdma_cm_id *id);
static int on_event(struct rdma_cm_event *event);
static void usage(const char *argv0);

int main(int argc, char **argv)
{
  struct sockaddr_in addr;
  struct rdma_cm_event *event = NULL;
  struct rdma_cm_id *listener = NULL;
  struct rdma_event_channel *ec = NULL;
  uint16_t port = 0;
  uint16_t port2 = 0;
  int porcik;
  int udpMode = 0;

  printf("%d\n", argc);

  if (argc != 4)
    usage(argv[0]);

  if (strcmp(argv[1], "write") == 0)
    set_mode(M_WRITE);
  else if (strcmp(argv[1], "read") == 0)
    set_mode(M_READ);
  else
    usage(argv[0]);

  if (strcmp(argv[2], "tcp") == 0) {
  	setUdp(0);
	udpMode = 0;
  } else if (strcmp(argv[2], "udp") == 0) {
    setUdp(1);
	udpMode = 1;
  } else
    usage(argv[0]);

  setClient(0);

  printf("Podano adres %s i port: %s\n", argv[2], argv[3]); 
  porcik = atoi(argv[3]);
  printf("Parsed port: %d\n", porcik);
  port2 = porcik;
  printf("Converted port number: %u\n", port2);


  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_port = htons((uint16_t)porcik);

  TEST_Z(ec = rdma_create_event_channel());
  if (udpMode)
  	TEST_NZ(rdma_create_id(ec, &listener, NULL, RDMA_PS_UDP));
  else
  	TEST_NZ(rdma_create_id(ec, &listener, NULL, RDMA_PS_TCP));

  TEST_NZ(rdma_bind_addr(listener, (struct sockaddr *)&addr));
  TEST_NZ(rdma_listen(listener, 10)); /* backlog=10 is arbitrary */

  port = ntohs(rdma_get_src_port(listener));

  printf("listening on port %d.\n", port);

  while (rdma_get_cm_event(ec, &event) == 0) {
    struct rdma_cm_event event_copy;

    memcpy(&event_copy, event, sizeof(*event));
    rdma_ack_cm_event(event);

    if (on_event(&event_copy)) {
	  printf("Does that happen once or more?\n");
      break;
	}

  }

  rdma_destroy_id(listener);
  rdma_destroy_event_channel(ec);

  return 0;
}

int on_connect_request(struct rdma_cm_id *id)
{
  struct rdma_conn_param cm_params;

  printf("received connection request.\n");
  build_connection(id);
  build_params(&cm_params);
  sprintf(get_local_message_region(id->context), "message from passive/server side with pid %d", getpid());
  TEST_NZ(rdma_accept(id, &cm_params));

  return 0;
}

int on_connection(struct rdma_cm_id *id)
{
  on_connect(id->context);

  return 0;
}

int on_disconnect(struct rdma_cm_id *id)
{
  printf("peer disconnected.\n");

  destroy_connection(id->context);
  return 0;
}

int on_event(struct rdma_cm_event *event)
{
  int r = 0;

  if (event->event == RDMA_CM_EVENT_CONNECT_REQUEST)
    r = on_connect_request(event->id);
  else if (event->event == RDMA_CM_EVENT_ESTABLISHED)
    r = on_connection(event->id);
  else if (event->event == RDMA_CM_EVENT_DISCONNECTED)
    r = on_disconnect(event->id);
  else
    die("on_event: unknown event.");

  return r;
}

void usage(const char *argv0)
{
  fprintf(stderr, "usage: %s <mode> <tcp/udp> <port no.>\n  mode = \"read\", \"write\"\n", argv0);
  exit(1);
}
