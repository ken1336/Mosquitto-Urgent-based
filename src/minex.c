#include"minex.h"

static int max_inflight = 20;
static unsigned long max_inflight_bytes = 0;
static int max_queued = 100;
static unsigned long max_queued_bytes = 0;

static bool db__ready_for_flight(struct mosquitto *context, int qos)
{
	if(qos == 0 || (max_inflight == 0 && max_inflight_bytes == 0)){
		return true;
	}

	bool valid_bytes = context->msg_bytes12 < max_inflight_bytes;
	bool valid_count = context->msg_count12 < max_inflight;

	if(max_inflight == 0){
		return valid_bytes;
	}
	if(max_inflight_bytes == 0){
		return valid_count;
	}

	return valid_bytes && valid_count;
}
static bool db__ready_for_queue(struct mosquitto *context, int qos)
{
	if(max_queued == 0 && max_queued_bytes == 0){
		return true;
	}

	unsigned long source_bytes = context->msg_bytes12;
	int source_count = context->msg_count12;
	unsigned long adjust_bytes = max_inflight_bytes;
	int adjust_count = max_inflight;

	/* nothing in flight for offline clients */
	if(context->sock == INVALID_SOCKET){
		adjust_bytes = 0;
		adjust_count = 0;
	}

	if(qos == 0){
		source_bytes = context->msg_bytes;
		source_count = context->msg_count;
	}

	bool valid_bytes = source_bytes - adjust_bytes < max_queued_bytes;
	bool valid_count = source_count - adjust_count < max_queued;

	if(max_queued_bytes == 0){
		return valid_count;
	}
	if(max_queued == 0){
		return valid_bytes;
	}

	return valid_bytes && valid_count;
}


void text_log(){
	
	printf("extension OK\n");
}

void display_ur_message(struct Urgent_db *udb) {
	if (udb->messages) {
		Min_mosq *temp = udb->messages;
		while (temp) {
			printf("topic: %s	count: %d\n",temp->stored->topic, udb->message_count);
			if(temp->subs){
				Subscriptionlist *list = temp->subs;
				while (list) {
					printf("	sub:%s\n",list->sub->id);
					list = list->next;
				}
			}
			temp = temp->next;
		}
	}
}
int add_udb_message(struct Urgent_db *udb, struct Min_mosq *message) {


	if (udb->messages == NULL) {
		printf("add fritst\n");
		udb->messages = message;
		udb->message_count += 1;
	}
	else {
		printf("add \n");
		Min_mosq *temp = udb->messages;
		while (temp->next) {
			temp = temp->next;
		}
		temp->next = message;
		udb->message_count += 1;


	}


	return NULL;
}
int remove_udb_message_first(struct Urgent_db *udb){
	if (udb->messages) {
		printf("remove\n");
		Min_mosq *del = udb->messages;
		Min_mosq *temp = del->next;
		del->subs = NULL;
		del->stored = NULL;
		
		free(del);
		udb->messages = temp;
		udb->message_count--;
	}
	display_ur_message(udb);
	return 0;



}
Min_mosq* create_ur_message(struct mosquitto_msg_store *stored, Subscriptionlist *subs) {
	Min_mosq* new_mosq = (Min_mosq*)malloc(sizeof(Min_mosq));
	new_mosq->stored = stored;
	new_mosq->subs = subs;
	new_mosq->next = NULL;
	new_mosq->sub_count = 0;

	return new_mosq;
}



void add_sub(Subscriptionlist **subs, struct mosquitto *context, struct mosquitto_client_msg* umsg) {
	
	//printf("contect->id: %s\n",context->id);
	if (*subs == NULL) {
		printf("insert fisrt\n");
		*subs = (Subscriptionlist*)malloc(sizeof(Subscriptionlist));
		(*subs)->sub = context;
		(*subs)->umsg = umsg;
		(*subs)->next = NULL;
		

	}
	else {
		printf("insert second\n");
		Subscriptionlist *temp = *subs;
		
		while (temp->next) {
			temp = temp->next;
			
		}
		Subscriptionlist* new_sub = (Subscriptionlist*)malloc(sizeof(Subscriptionlist));
		new_sub->sub = context;
		new_sub->umsg= umsg;
		new_sub->next = NULL;
		temp->next = new_sub;
	}

	
}
void display_subs(Subscriptionlist *subs) {
	printf("display\n");
	if (subs) {

		Subscriptionlist *temp = subs;
		while (temp) {
			printf("subs: %s\n", temp->sub->id);
				//printf("subs: %d\n", temp->next->sub);
			temp = temp->next;
		}
	}

}

void db__msg_store_deref_ex(struct mosquitto_db *db, struct mosquitto_msg_store **store)
{
	(*store)->ref_count--;
	if((*store)->ref_count == 0){
		db__msg_store_remove(db, *store);
		*store = NULL;
	}
}


static void db__message_remove(struct mosquitto_db *db, struct mosquitto *context, struct mosquitto_client_msg **msg, struct mosquitto_client_msg *last)
{
	if(!context || !msg || !(*msg)){
		return;
	}

	if((*msg)->store){
		context->msg_count--;
		context->msg_bytes -= (*msg)->store->payloadlen;
		if((*msg)->qos > 0){
			context->msg_count12--;
			context->msg_bytes12 -= (*msg)->store->payloadlen;
		}
		db__msg_store_deref(db, &(*msg)->store);
	}
	if(last){
		last->next = (*msg)->next;
		if(!last->next){
			context->last_inflight_msg = last;
		}
	}else{
		context->inflight_msgs = (*msg)->next;
		if(!context->inflight_msgs){
			context->last_inflight_msg = NULL;
		}
	}
	mosquitto__free(*msg);
	if(last){
		*msg = last->next;
	}else{
		*msg = context->inflight_msgs;
	}
}

void send_ur_message(struct mosquitto_db *db,struct Urgent_db *udb) {
	if (udb->messages) {
		Min_mosq *temp = udb->messages;
		
		while (temp) {
			
			//printf("topic: %s	count: %d\n", temp->stored->topic, udb->message_count);
			if (temp->subs) {
				Subscriptionlist *list = temp->subs;
				//printf("debug %s\n",list->sub->id);
				while (list) {
					//printf("debug %s\n",list->sub->id);
					if(list->umsg){
						db__message_write_ex(db,list->sub, list->umsg);
						
						list->umsg = NULL;
						
					}
					Subscriptionlist *sub_temp = list;
					free(sub_temp);
					list = list->next;
					

				}
			}
			//temp->stored = NULL;
			temp = temp->next;
			remove_udb_message_first(udb);
			
		}
	}
}
int db__message_write_ex(struct mosquitto_db *db,struct mosquitto *context, struct mosquitto_client_msg *msg) {
	int rc;
	struct mosquitto_client_msg *tail, *last = NULL;
	uint16_t mid;
	int retries;
	int retain;
	const char *topic;
	int qos;
	uint32_t payloadlen;
	const void *payload;
	int msg_count = 0;
	
	if (!context || context->sock == INVALID_SOCKET
		|| (context->state == mosq_cs_connected && !context->id)) {
		return MOSQ_ERR_INVAL;
	}

	if (context->state != mosq_cs_connected) {
		return MOSQ_ERR_SUCCESS;
	}

	tail = msg;
	msg_count++;
	mid = tail->mid;
	retries = tail->dup;
	retain = tail->retain;
	topic = tail->store->topic;
	qos = tail->qos;
	payloadlen = tail->store->payloadlen;
	payload = UHPA_ACCESS_PAYLOAD(tail->store);
	printf("paylen: %s\n",(char*)payload);

	switch (tail->state) {
		case mosq_ms_publish_qos0:
			rc = send__publish(context, mid, topic, payloadlen, payload, qos, retain, retries);
			if (!rc) {
				
				db__message_remove(db, context, &tail, last);
			}
			else {
				return rc;
			}
			tail = NULL;
			break;

		case mosq_ms_publish_qos1:
			rc = send__publish(context, mid, topic, payloadlen, payload, qos, retain, retries);
			if (!rc) {
				tail->timestamp = mosquitto_time();
				tail->dup = 1; /* Any retry attempts are a duplicate. */
				tail->state = mosq_ms_wait_for_puback;
			}
			else {
				return rc;
			}
			last = tail;
			//tail = tail->next;
			tail = NULL;
			break;

		case mosq_ms_publish_qos2:
			rc = send__publish(context, mid, topic, payloadlen, payload, qos, retain, retries);
			if (!rc) {
				tail->timestamp = mosquitto_time();
				tail->dup = 1; /* Any retry attempts are a duplicate. */
				tail->state = mosq_ms_wait_for_pubrec;
			}
			else {
				return rc;
			}
			last = tail;
			tail = NULL;
			//tail = tail->next;
			break;

		case mosq_ms_send_pubrec:
			rc = send__pubrec(context, mid);
			if (!rc) {
				tail->state = mosq_ms_wait_for_pubrel;
			}
			else {
				return rc;
			}
			last = tail;
			tail = NULL;
			//tail = tail->next;
			break;

		case mosq_ms_resend_pubrel:
			rc = send__pubrel(context, mid);
			if (!rc) {
				tail->state = mosq_ms_wait_for_pubcomp;
			}
			else {
				return rc;
			}
			last = tail;
			tail = NULL;
			//tail = tail->next;
			break;

		case mosq_ms_resend_pubcomp:
			rc = send__pubcomp(context, mid);
			if (!rc) {
				tail->state = mosq_ms_wait_for_pubrel;
			}
			else {
				return rc;
			}
			last = tail;
			tail = NULL;
			//tail = tail->next;
			break;

		default:
			last = tail;
			tail = NULL;
			//tail = tail->next;
			break;
	}
	while(context->queued_msgs && (max_inflight == 0 || msg_count < max_inflight)){
		msg_count++;
		tail = context->queued_msgs;
		if(tail->direction == mosq_md_out){
			switch(tail->qos){
				case 0:
					tail->state = mosq_ms_publish_qos0;
					break;
				case 1:
					tail->state = mosq_ms_publish_qos1;
					break;
				case 2:
					tail->state = mosq_ms_publish_qos2;
					break;
			}
			db__message_dequeue_first(context);
		}else{
			if(tail->qos == 2){
				tail->state = mosq_ms_send_pubrec;
				db__message_dequeue_first(context);
				rc = send__pubrec(context, tail->mid);
				if(!rc){
					tail->state = mosq_ms_wait_for_pubrel;
				}else{
					return rc;
				}
			}
		}
	}
}



int db__message_insert_ex(struct mosquitto_db *db, struct mosquitto *context, uint16_t mid, enum mosquitto_msg_direction dir, int qos, bool retain, struct mosquitto_msg_store *stored, struct mosquitto_client_msg **umsg)
{
	struct mosquitto_client_msg *msg;
	struct mosquitto_client_msg **msgs, **last_msg;
	enum mosquitto_msg_state state = mosq_ms_invalid;
	int rc = 0;
	int i;
	char **dest_ids;

	assert(stored);
	if (!context) return MOSQ_ERR_INVAL;
	if (!context->id) return MOSQ_ERR_SUCCESS; /* Protect against unlikely "client is disconnected but not entirely freed" scenario */

											   /* Check whether we've already sent this message to this client
											   * for outgoing messages only.
											   * If retain==true then this is a stale retained message and so should be
											   * sent regardless. FIXME - this does mean retained messages will received
											   * multiple times for overlapping subscriptions, although this is only the
											   * case for SUBSCRIPTION with multiple subs in so is a minor concern.
											   */
	if (db->config->allow_duplicate_messages == false
		&& dir == mosq_md_out && retain == false && stored->dest_ids) {

		for (i = 0; i<stored->dest_id_count; i++) {
			if (!strcmp(stored->dest_ids[i], context->id)) {
				/* We have already sent this message to this client. */
				return MOSQ_ERR_SUCCESS;
			}
		}
	}
	if (context->sock == INVALID_SOCKET) {
		/* Client is not connected only queue messages with QoS>0. */
		if (qos == 0 && !db->config->queue_qos0_messages) {
			if (!context->bridge) {
				return 2;
			}
			else {
				if (context->bridge->start_type != bst_lazy) {
					return 2;
				}
			}
		}
	}

	if (context->sock != INVALID_SOCKET) {
		if (db__ready_for_flight(context, qos)) {
			if (dir == mosq_md_out) {
				switch (qos) {
				case 0:
					state = mosq_ms_publish_qos0;
					break;
				case 1:
					state = mosq_ms_publish_qos1;
					break;
				case 2:
					state = mosq_ms_publish_qos2;
					break;
				}
			}
			else {
				if (qos == 2) {
					state = mosq_ms_wait_for_pubrel;
				}
				else {
					return 1;
				}
			}
		}
		else if (db__ready_for_queue(context, qos)) {
			state = mosq_ms_queued;
			rc = 2;
		}
		else {
			/* Dropping message due to full queue. */
			if (context->is_dropping == false) {
				context->is_dropping = true;
				log__printf(NULL, MOSQ_LOG_NOTICE,
					"Outgoing messages are being dropped for client %s.",
					context->id);
			}
			G_MSGS_DROPPED_INC();
			return 2;
		}
	}
	else {
		if (db__ready_for_queue(context, qos)) {
			state = mosq_ms_queued;
		}
		else {
			G_MSGS_DROPPED_INC();
			if (context->is_dropping == false) {
				context->is_dropping = true;
				log__printf(NULL, MOSQ_LOG_NOTICE,
					"Outgoing messages are being dropped for client %s.",
					context->id);
			}
			return 2;
		}
	}
	assert(state != mosq_ms_invalid);

#ifdef WITH_PERSISTENCE
	if (state == mosq_ms_queued) {
		db->persistence_changes++;
	}
#endif

	msg = mosquitto__malloc(sizeof(struct mosquitto_client_msg));
	if (!msg) return MOSQ_ERR_NOMEM;
	msg->next = NULL;
	msg->store = stored;
	msg->store->ref_count++;
	msg->mid = mid;
	msg->timestamp = mosquitto_time();
	msg->direction = dir;
	msg->state = state;
	msg->dup = false;
	msg->qos = qos;
	msg->retain = retain;
	msg->urgent = 1;

	if (state == mosq_ms_queued) {
		msgs = &(context->queued_msgs);
		last_msg = &(context->last_queued_msg);
	}
	else {
		msgs = &(context->inflight_msgs);
		last_msg = &(context->last_inflight_msg);
	}
	if (*last_msg) {
		(*last_msg)->next = msg;
		(*last_msg) = msg;
	}
	else {
		*msgs = msg;
		*last_msg = msg;
	}
	context->msg_count++;
	context->msg_bytes += msg->store->payloadlen;
	*umsg = msg;
	//printf("umsg: %s\n",umsg->store->topic);
	if (qos > 0) {
		context->msg_count12++;
		context->msg_bytes12 += msg->store->payloadlen;
	}

	if (db->config->allow_duplicate_messages == false && dir == mosq_md_out && retain == false) {
		/* Record which client ids this message has been sent to so we can avoid duplicates.
		* Outgoing messages only.
		* If retain==true then this is a stale retained message and so should be
		* sent regardless. FIXME - this does mean retained messages will received
		* multiple times for overlapping subscriptions, although this is only the
		* case for SUBSCRIPTION with multiple subs in so is a minor concern.
		*/
		dest_ids = mosquitto__realloc(stored->dest_ids, sizeof(char *)*(stored->dest_id_count + 1));
		if (dest_ids) {
			stored->dest_ids = dest_ids;
			stored->dest_id_count++;
			stored->dest_ids[stored->dest_id_count - 1] = mosquitto__strdup(context->id);
			if (!stored->dest_ids[stored->dest_id_count - 1]) {
				return MOSQ_ERR_NOMEM;
			}
		}
		else {
			return MOSQ_ERR_NOMEM;
		}
	}
#ifdef WITH_BRIDGE
	if (context->bridge && context->bridge->start_type == bst_lazy
		&& context->sock == INVALID_SOCKET
		&& context->msg_count >= context->bridge->threshold) {

		context->bridge->lazy_reconnect = true;
	}
#endif

#ifdef WITH_WEBSOCKETS
	if (context->wsi && rc == 0) {
		return db__message_write(db, context);
	}
	else {
		return rc;
	}
#else
	return rc;
#endif
}




int packet__read_urgent(struct mosquitto__packet *packet, uint8_t *byte)
{
	assert(packet);

	*byte = packet->payload[packet->pos];
	packet->pos++;

	return MOSQ_ERR_SUCCESS;
}

