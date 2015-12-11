#include "rdma-common.h"
#include <unistd.h>

//parameters to edit
#define RING_BUFFER_SIZE 30
static const int RDMA_BUFFER_SIZE = 8192;
static const int debug = 1;
static const int delay = 1000;

//global variables, don't touch
static int nowSending = 0;
static int udpMode = 0;
static long counter;
static int currMR = 0;

struct message {
  enum {
    MSG_MR,
    MSG_DONE
  } type;

struct rdmaWrite {
	long sequence;
	char textMessage[130];
} singleWrite;

  union {
    struct ibv_mr mr[RING_BUFFER_SIZE];
  } data;
};

struct context {
  struct ibv_context *ctx;
  struct ibv_pd *pd;
  struct ibv_cq *cq;
  struct ibv_comp_channel *comp_channel;

  pthread_t cq_poller_thread;
};

struct connection {
  struct rdma_cm_id *id;
  struct ibv_qp *qp;

  int connected;

  struct ibv_mr *recv_mr;
  struct ibv_mr *send_mr;
  struct ibv_mr *rdma_local_mr[RING_BUFFER_SIZE];
  struct ibv_mr *rdma_remote_mr[RING_BUFFER_SIZE];

  struct ibv_mr peer_mr[RING_BUFFER_SIZE];

  struct message *recv_msg;
  struct message *send_msg;

  char *rdma_local_region[RING_BUFFER_SIZE];
  char *rdma_remote_region[RING_BUFFER_SIZE];

  enum {
    SS_INIT,
    SS_MR_SENT,
    SS_RDMA_SENT,
    SS_DONE_SENT
  } send_state;

  enum {
    RS_INIT,
    RS_MR_RECV,
    RS_DONE_RECV
  } recv_state;
};

static void build_context(struct ibv_context *verbs);
static void build_qp_attr(struct ibv_qp_init_attr *qp_attr);
static char * get_peer_message_region(struct connection *conn);
static void on_completion(struct ibv_wc *);
static void * poll_cq(void *);
static void post_receives(struct connection *conn);
static void register_memory(struct connection *conn);
static void send_message(struct connection *conn);

static struct context *s_ctx = NULL;
static enum mode s_mode = M_WRITE;
static int clientSide;

void die(const char *reason)
{
  fprintf(stderr, "%s\n", reason);
  exit(EXIT_FAILURE);
}

void build_connection(struct rdma_cm_id *id)
{
  struct connection *conn;
  struct ibv_qp_init_attr qp_attr;

  build_context(id->verbs);
  build_qp_attr(&qp_attr);

  TEST_NZ(rdma_create_qp(id, s_ctx->pd, &qp_attr));

  id->context = conn = (struct connection *)malloc(sizeof(struct connection));

  conn->id = id;
  conn->qp = id->qp;

  conn->send_state = SS_INIT;
  conn->recv_state = RS_INIT;

  conn->connected = 0;

  register_memory(conn);
  post_receives(conn);
}

void build_context(struct ibv_context *verbs)
{
  if (s_ctx) {
    if (s_ctx->ctx != verbs)
      die("cannot handle events in more than one context.");

    return;
  }

  s_ctx = (struct context *)malloc(sizeof(struct context));

  s_ctx->ctx = verbs;

  TEST_Z(s_ctx->pd = ibv_alloc_pd(s_ctx->ctx));
  TEST_Z(s_ctx->comp_channel = ibv_create_comp_channel(s_ctx->ctx));
  TEST_Z(s_ctx->cq = ibv_create_cq(s_ctx->ctx, 10, NULL, s_ctx->comp_channel, 0)); /* cqe=10 is arbitrary */
  TEST_NZ(ibv_req_notify_cq(s_ctx->cq, 0));

  TEST_NZ(pthread_create(&s_ctx->cq_poller_thread, NULL, poll_cq, NULL));
}

void build_params(struct rdma_conn_param *params)
{
  memset(params, 0, sizeof(*params));

  params->initiator_depth = params->responder_resources = 1;
  params->rnr_retry_count = 7; /* infinite retry */
}

void build_qp_attr(struct ibv_qp_init_attr *qp_attr)
{
  memset(qp_attr, 0, sizeof(*qp_attr));

  qp_attr->send_cq = s_ctx->cq;
  qp_attr->recv_cq = s_ctx->cq;
  if (udpMode)
  	qp_attr->qp_type = IBV_QPT_UC;
  else
  	qp_attr->qp_type = IBV_QPT_RC;

  qp_attr->cap.max_send_wr = 15;
  qp_attr->cap.max_recv_wr = 15;
  qp_attr->cap.max_send_sge = 3;
  qp_attr->cap.max_recv_sge = 3;
}

void destroy_connection(void *context)
{
  struct connection *conn = (struct connection *)context;

  rdma_destroy_qp(conn->id);

  ibv_dereg_mr(conn->send_mr);
  ibv_dereg_mr(conn->recv_mr);

  int a;

  for (a = 0; a < RING_BUFFER_SIZE; a++)
  	ibv_dereg_mr(conn->rdma_local_mr[a]);

  for (a = 0; a < RING_BUFFER_SIZE; a++)
      ibv_dereg_mr(conn->rdma_remote_mr[a]);

  free(conn->send_msg);
  free(conn->recv_msg);
  
  for (a = 0; a < RING_BUFFER_SIZE; a++)
  	free(conn->rdma_local_region[a]);
  
  for (a = 0; a < RING_BUFFER_SIZE; a++)
	free(conn->rdma_remote_region[a]);

  rdma_destroy_id(conn->id);

  free(conn);
}

void * get_local_message_region(void *context)
{
  if (debug)
  	printf("CurrMR %d, get local message region\n", currMR);
  if (s_mode == M_WRITE)
    return ((struct connection *)context)->rdma_local_region[currMR];
  else
    return ((struct connection *)context)->rdma_remote_region[currMR];
}

char * get_peer_message_region(struct connection *conn)
{
  if (debug)
      printf("CurrMR %d, get remote message region\n", currMR);

  if (s_mode == M_WRITE)
    return conn->rdma_remote_region[currMR];
  else
    return conn->rdma_local_region[currMR];
}

void on_completion(struct ibv_wc *wc)
{
  struct connection *conn = (struct connection *)(uintptr_t)wc->wr_id;
  int count;

  if (debug) {
	printf("Entering on completition. Clientside: %d.\n", clientSide);
    printf("WC Status: %d\n", wc->status);
  }

  if (wc->status != IBV_WC_SUCCESS) 
    die("on_completion: status is not IBV_WC_SUCCESS.");
 

  if (wc->opcode & IBV_WC_RECV) {
    conn->recv_state++;
     if (debug) 
	printf("Opcode WC_RECV read, recv state increased to %d\n", conn->recv_state);

    if (conn->recv_msg->type == MSG_MR) {
      
	  for (int a=0; a<RING_BUFFER_SIZE; a++) {
	  	memcpy(&conn->peer_mr[a], &conn->recv_msg->data.mr[a], sizeof(struct ibv_mr));
		if (debug)
			printf("received Mem region number %d, address: %ld \n", currMR, (long)conn->recv_msg->data.mr[a].addr);

	  }

      post_receives(conn); /* only rearm for MSG_MR */
      if (debug)
			printf("currMR = %d, posted receive after receiving Mem region\n", currMR);
	  

      if (conn->send_state == SS_INIT) {
	  /* received ALL  peer's MRs before sending ours, so send ours back */
        send_mr(conn);
        
		if (debug)
		printf("Are we ever here? Sent Mem Region. Conditions: send_state %d, currMR %d\n", conn->send_state, currMR);
       }

    }

  } else {
    conn->send_state++;
   if (debug) 
     printf("send state increased to %dy.\n", conn->send_state);
  }

  if (debug)	
  	printf(" - Send state: %d, Recv state: %d\n", conn->send_state, conn->recv_state);

  if ((conn->send_state == SS_MR_SENT && conn->recv_state == RS_MR_RECV) || (nowSending))  {

  	nowSending = 1;

    if (clientSide) { //we're on client side
    
    	struct ibv_send_wr wr, *bad_wr = NULL;
    	struct ibv_sge sge;

    	if (s_mode == M_WRITE)  { if (debug) 
      		printf("received MSG_MR. writing message to remote memory...\n"); }
    	else { if (debug)
      		printf("received MSG_MR. reading message from remote memory...\n"); }

  		if (debug) {
      		printf("CurrMR %d, writing a message in local region MR \n", currMR);
			printf("Local buffer contents: %s\n", conn->rdma_local_region[currMR]);
		}	

		struct rdmaWrite mesg;
		mesg.sequence = counter;

        sprintf(mesg.textMessage, "Message number %ld", counter);
		memcpy(conn->rdma_local_region[currMR], &mesg, sizeof(mesg));

		counter++;

//		sprintf(conn->rdma_local_region[currMR], "Message number %ld\n", counter);

    	memset(&wr, 0, sizeof(wr));

		wr.wr_id = (uintptr_t)conn;
    	wr.opcode = (s_mode == M_WRITE) ? IBV_WR_RDMA_WRITE : IBV_WR_RDMA_READ;
    	wr.sg_list = &sge;
    	wr.num_sge = 1;
    	wr.send_flags = IBV_SEND_SIGNALED;
    	wr.wr.rdma.remote_addr = (uintptr_t)conn->peer_mr[currMR].addr;
    	wr.wr.rdma.rkey = conn->peer_mr[currMR].rkey;
        if (debug)
			printf("Created WR and added remote region pointer %ld\n", (long)conn->peer_mr[currMR].addr);


  		if (debug)
      		printf("CurrMR %d, created SGE and added local region pointer %ld\n", currMR, (long)conn->rdma_local_region[currMR]);
    	sge.addr = (uintptr_t)conn->rdma_local_region[currMR];
    	sge.length = RDMA_BUFFER_SIZE;
    	sge.lkey = conn->rdma_local_mr[currMR]->lkey;

		if (debug)
			printf("CurrMR: %d\n",currMR);

	for (count = 0; count < 1; count ++)
    		TEST_NZ(ibv_post_send(conn->qp, &wr, &bad_wr));
    
    	if (debug)
      		printf("Sent message, post send just now\n");

		incCurrMR();

	usleep(delay);

    	if (debug)
      		printf("Receives posted\n");

//    	send_mr(conn);
   
     } else { //we're on server side and we're receiving the messages

		struct rdmaWrite tmpmesg;

		while (1) {
			for (int a = 0; a < RING_BUFFER_SIZE; a++) {
				//char tmpbuff[5];

				//printf("Server buffer number %d: %s", a, conn->rdma_remote_region[a]);

				memcpy(&tmpmesg, conn->rdma_remote_region[a], sizeof(tmpmesg));
				//memcpy(tmpbuff2, tmpbuff, sizeof(tmpbuff2));
				printf("%ld - %s\n", tmpmesg.sequence, tmpmesg.textMessage);

				//Message number 1827
				usleep(delay);
			}
		}

	}

  } else if ((conn->send_state == SS_DONE_SENT) && (conn->recv_state == RS_DONE_RECV)) {
    printf("This never happens, right?\n");
    printf("remote buffer: %s\n", get_peer_message_region(conn));
  }
}

void on_connect(void *context)
{
  ((struct connection *)context)->connected = 1;
}

void * poll_cq(void *ctx)
{
  struct ibv_cq *cq;
  struct ibv_wc wc;

  while (1) {
    TEST_NZ(ibv_get_cq_event(s_ctx->comp_channel, &cq, &ctx));
    ibv_ack_cq_events(cq, 1);
    TEST_NZ(ibv_req_notify_cq(cq, 0));

    while (ibv_poll_cq(cq, 1, &wc))
      on_completion(&wc);
  }

  return NULL;
}

void post_receives(struct connection *conn)
{
  struct ibv_recv_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;

  wr.wr_id = (uintptr_t)conn;
  wr.next = NULL;
  wr.sg_list = &sge;
  wr.num_sge = 1;

  sge.addr = (uintptr_t)conn->recv_msg;
  sge.length = sizeof(struct message);
  sge.lkey = conn->recv_mr->lkey;

  TEST_NZ(ibv_post_recv(conn->qp, &wr, &bad_wr));
}

void register_memory(struct connection *conn)
{
  conn->send_msg = malloc(sizeof(struct message));
  conn->recv_msg = malloc(sizeof(struct message));

  int a;
  for (a = 0; a < RING_BUFFER_SIZE; a++)
  	conn->rdma_local_region[a] = malloc(RDMA_BUFFER_SIZE);
  for (a = 0; a < RING_BUFFER_SIZE; a++)
  	conn->rdma_remote_region[a] = malloc(RDMA_BUFFER_SIZE);

  if (debug)
  	printf("Regesitered %d local_regions and remote_regions.\n",RING_BUFFER_SIZE);

  TEST_Z(conn->send_mr = ibv_reg_mr(
    s_ctx->pd, 
    conn->send_msg, 
    sizeof(struct message), 
    IBV_ACCESS_LOCAL_WRITE));

  TEST_Z(conn->recv_mr = ibv_reg_mr(
    s_ctx->pd, 
    conn->recv_msg, 
    sizeof(struct message), 
    IBV_ACCESS_LOCAL_WRITE | ((s_mode == M_WRITE) ? IBV_ACCESS_REMOTE_WRITE : IBV_ACCESS_REMOTE_READ)));

  for (int a = 0; a < RING_BUFFER_SIZE; a++)
  TEST_Z(conn->rdma_local_mr[a] = ibv_reg_mr(
    s_ctx->pd, 
    conn->rdma_local_region[a], 
    RDMA_BUFFER_SIZE, 
    IBV_ACCESS_LOCAL_WRITE));
  if (debug)
  	printf("Ibv_reg_mr local_mr %d times\n", RING_BUFFER_SIZE);

  for (int b = 0; b < RING_BUFFER_SIZE; b++)
  TEST_Z(conn->rdma_remote_mr[b] = ibv_reg_mr(
    s_ctx->pd, 
    conn->rdma_remote_region[b], 
    RDMA_BUFFER_SIZE, 
    IBV_ACCESS_LOCAL_WRITE | ((s_mode == M_WRITE) ? IBV_ACCESS_REMOTE_WRITE : IBV_ACCESS_REMOTE_READ)));

  if (debug)
	printf("Ibv_reg_mr remote_mr %d times\n", RING_BUFFER_SIZE);

}

void send_message(struct connection *conn)
{
  struct ibv_send_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;

  memset(&wr, 0, sizeof(wr));

  wr.wr_id = (uintptr_t)conn;
  wr.opcode = IBV_WR_SEND;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.send_flags = IBV_SEND_SIGNALED;

  sge.addr = (uintptr_t)conn->send_msg;
  sge.length = sizeof(struct message);
  sge.lkey = conn->send_mr->lkey;

  while (!conn->connected);

  TEST_NZ(ibv_post_send(conn->qp, &wr, &bad_wr));
}

void send_mr(void *context)
{
  struct connection *conn = (struct connection *)context;
  int ab;
  conn->send_msg->type = MSG_MR;
  
  for (ab = 0; ab < RING_BUFFER_SIZE; ab++) {
  	memcpy(&conn->send_msg->data.mr[ab], conn->rdma_remote_mr[ab], sizeof(struct ibv_mr));   
    if (debug)
  		printf(":Copied memory region: %ld into send_msg\n", (long)conn->rdma_remote_mr[ab]);
  }  	

  send_message(conn);  
}

void set_mode(enum mode m) {
  s_mode = m;
}

void setClient(int client) {
  clientSide = client;
}

void setUdp(int udp) {
  udpMode = udp;
}

void incCurrMR() {
  currMR++;
  if (currMR >= RING_BUFFER_SIZE)
  	currMR = 0;
  if (debug)
  	printf("incCurMR(), CurrMR %d\n", currMR);
}


