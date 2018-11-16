#include<stdio.h>
#include <assert.h>
#include <stdio.h>


#include "mosquitto_broker_internal.h"
#include "memory_mosq.h"
#include "send_mosq.h"
#include "sys_tree.h"
#include "time_mosq.h"

#include "util_mosq.h"

#ifndef MOSQUITTO_MIN_EXTENSION
#define MOSQUITTO_MIN_EXTENSION
#endif

void text_log();

int ad_udb_message(struct Urgent_db udb,struct mosquitto_msg_store);

int sub__messages_queue_ex(struct mosquitto_db *db, const char *source_id, const char *topic, int qos, int retain, struct mosquitto_msg_store **stored, Subscriptionlist **subs);

void add_sub(Subscriptionlist **subs, struct mosquitto *context,struct mosquitto_client_msg *umsg);
void display_subs(Subscriptionlist *subs);
int add_udb_message(struct Urgent_db *udb, struct Min_mosq *message);
Min_mosq* create_ur_message(struct mosquitto_msg_store *stored, Subscriptionlist *subs);
void display_ur_message(struct Urgent_db *udb);
void db__msg_store_deref_ex(struct mosquitto_db *db, struct mosquitto_msg_store **store);
int db__message_insert_ex(struct mosquitto_db *db, struct mosquitto *context, uint16_t mid, enum mosquitto_msg_direction dir, int qos, bool retain, struct mosquitto_msg_store *stored, struct mosquitto_client_msg **umsg);
int db__message_write_ex(struct mosquitto_db *db,struct mosquitto *context, struct mosquitto_client_msg *msg);
void send_ur_message(struct mosquitto_db *db,struct Urgent_db *udb);
int remove_udb_message(struct Urgent_db *udb, struct Min_mosq *message);
int remove_udb_message_first(struct Urgent_db *udb);
int packet__read_urgent(struct mosquitto__packet *packet, uint8_t *byte);
