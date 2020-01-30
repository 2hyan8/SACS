#include "destor.h"
#include "jcr.h"
#include "rewrite_phase.h"
#include "backup.h"

static int64_t chunk_num;

static GHashTable *top;
static GHashTable *last;

static void cap_segment_get_top() {
	struct chunk* c;
	rewrite_buffer.container_record_seq = g_sequence_new(free);
	GList *list = rewrite_buffer.chunk_queue->head;
	while (list){
		c = list->data;
        if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END)
			|| CHECK_CHUNK(c, CHUNK_SEGMENT_START) || CHECK_CHUNK(c, CHUNK_SEGMENT_END)){
            list = list->next;    
            continue;
        }
        
		if (c->id != TEMPORARY_ID) {
		    assert(CHECK_CHUNK(c, CHUNK_DUPLICATE));
			struct containerRecord tmp_record;
			tmp_record.cid = c->id;
			GSequenceIter *iter = g_sequence_lookup(
				rewrite_buffer.container_record_seq, &tmp_record,
				g_record_cmp_by_id,
				NULL);
			if (iter == NULL) {
                struct containerRecord* record = malloc(
                        sizeof(struct containerRecord));
                record->cid = c->id;
                record->size = c->size;
                /* We first assume it is out-of-order */
                record->out_of_order = 1;
                record->chunk_num = 1;
                g_sequence_insert_sorted(rewrite_buffer.container_record_seq,
                        record, g_record_cmp_by_id, NULL);
            } else {
                struct containerRecord* record = g_sequence_get(iter);
                assert(record->cid == c->id);
                record->size += c->size;
                record->chunk_num++;
            }
		}
		list = list->next;
	}

    /* Descending order */
    g_sequence_sort(rewrite_buffer.container_record_seq,
            g_record_descmp_by_length, NULL);

    int length = g_sequence_get_length(rewrite_buffer.container_record_seq);
    int32_t num = length > destor.rewrite_capping_level ?
                    destor.rewrite_capping_level : length, i;
    GSequenceIter *iter = g_sequence_get_begin_iter(
            rewrite_buffer.container_record_seq);
    for (i = 0; i < num; i++) {
        assert(!g_sequence_iter_is_end(iter));
        struct containerRecord* record = g_sequence_get(iter);
        struct containerRecord* r = (struct containerRecord*) malloc(
                sizeof(struct containerRecord));
        memcpy(r, record, sizeof(struct containerRecord));
        r->out_of_order = 0;
        g_hash_table_insert(top, &r->cid, r);
        iter = g_sequence_iter_next(iter);
    }
    VERBOSE("Rewrite phase: Select Top-%d in %d containers", num, length);
    
	g_sequence_sort(rewrite_buffer.container_record_seq, g_record_cmp_by_id, NULL);
}

static int sum = 0;
void *lbw_slide_only(void* arg) {
	top = g_hash_table_new_full(g_int64_hash, g_int64_equal, NULL, free);
	last = g_hash_table_new_full(g_int64_hash, g_int64_equal, NULL, free);

	while (1) {	
		struct chunk *c = sync_queue_pop(dedup_queue);

		if (c == NULL)
			break;

		TIMER_DECLARE(1);
		TIMER_BEGIN(1);
		g_queue_push_tail(rewrite_buffer.chunk_queue, c);
		
		if(CHECK_CHUNK(c, CHUNK_SEGMENT_END)){
            if(++rewrite_buffer.num < 5){
                continue;
            }         
        }else{
            continue;
        }
				
		cap_segment_get_top();
	
		while (c = g_queue_pop_head(rewrite_buffer.chunk_queue)) {
			if(CHECK_CHUNK(c, CHUNK_SEGMENT_END))
				break;			
			if (CHECK_CHUNK(c, CHUNK_CANDIDATE)) {
				if (g_hash_table_lookup(top, &c->id) == NULL) {
					/* not in TOP */
					SET_CHUNK(c, CHUNK_OUT_OF_ORDER);
					VERBOSE("Rewrite phase: %lldth chunk is in out-of-order container %lld",
							chunk_num, c->id);
				}
				chunk_num++;
			}
			TIMER_END(1, jcr.rewrite_time);
			sync_queue_push(rewrite_queue, c);
			TIMER_BEGIN(1);
		}
		rewrite_buffer.num--;

		GList *list = rewrite_buffer.chunk_queue->head;
        while (list && sum < 3){           
            c = list->data;
            if(CHECK_CHUNK(c, CHUNK_CANDIDATE) && g_hash_table_lookup(top, &c->id) != NULL){
                SET_CHUNK(c, CHUNK_REWRITE_DENIED);
            }
			if(CHECK_CHUNK(c, CHUNK_SEGMENT_END)){
				sum++;
			}
            list = g_list_next(list);						
        }

		sum = 0;

        while (list){
            c = list->data;
            if (!CHECK_CHUNK(c,	CHUNK_FILE_START) 
					&& !CHECK_CHUNK(c, CHUNK_FILE_END)
					&& !CHECK_CHUNK(c, CHUNK_SEGMENT_START) 
					&& !CHECK_CHUNK(c, CHUNK_SEGMENT_END)
					&& CHECK_CHUNK(c, CHUNK_DUPLICATE)) {
				if (g_hash_table_lookup(top, &c->id) == NULL && g_hash_table_lookup(last, &c->id) == NULL) {
					
					SET_CHUNK(c, CHUNK_CANDIDATE);					
				}				
			}
            list = g_list_next(list);
        }

		g_hash_table_remove_all(top);

	}

	
	cap_segment_get_top();

	struct chunk *c;
	while ((c = g_queue_pop_head(rewrite_buffer.chunk_queue))) {
		if (!CHECK_CHUNK(c,	CHUNK_FILE_START) && !CHECK_CHUNK(c, CHUNK_FILE_END)
				&& !CHECK_CHUNK(c, CHUNK_SEGMENT_START) && !CHECK_CHUNK(c, CHUNK_SEGMENT_END)) {
			if (g_hash_table_lookup(top, &c->id) == NULL) {
				/* not in TOP */
				SET_CHUNK(c, CHUNK_OUT_OF_ORDER);
				VERBOSE("Rewrite phase: %lldth chunk is in out-of-order container %lld",
						chunk_num, c->id);
			}
			chunk_num++;
		}
		sync_queue_push(rewrite_queue, c);
	}

	g_hash_table_remove_all(top);

	sync_queue_term(rewrite_queue);

	return NULL;
}