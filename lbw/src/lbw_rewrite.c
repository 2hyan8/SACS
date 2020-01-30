#include "destor.h"
#include "jcr.h"
#include "rewrite_phase.h"
#include "backup.h"

static int64_t chunk_num;

static GHashTable *top;

int Nrw = 0;
int Hrw = 0;
int rewrite_sum = 0;
int container_reads_sum = 0;
int current_segment = 0;
int T_last_segment = 0;

static void cap_segment_get_top() {
	struct chunk* c;
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
            //printf("%d\n",c->id);
			GSequenceIter *iter = g_sequence_lookup(
				rewrite_buffer.container_record_seq, &tmp_record,
				g_record_cmp_by_id,
				NULL);
			if (iter == NULL) {
				struct containerRecord* record = malloc(
						sizeof(struct containerRecord));
				record->cid = c->id;
				record->size = c->size;							
				g_sequence_insert_sorted(rewrite_buffer.container_record_seq,
					record, g_record_cmp_by_id, NULL);						
			} else {	
				struct containerRecord* record = g_sequence_get(iter);
				assert(record->cid == c->id);
				record->size += c->size;
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


static void lbw_segment_get_top(int count) {
    struct chunk* c;
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
            //printf("%d\n",c->id);
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

    int rewrite_max;
    rewrite_max = Hrw * current_segment - rewrite_sum;
    g_sequence_sort(rewrite_buffer.container_record_seq,
                g_record_cmp_by_chunk_num, NULL);

    int length = g_sequence_get_length(rewrite_buffer.container_record_seq);

    int tmp_chunk_num = 0, i;

    GSequenceIter *iter = g_sequence_get_begin_iter(
            rewrite_buffer.container_record_seq);
    
    for (i = 0; i < length; i++) {
        struct containerRecord* record = g_sequence_get(iter);
        if(tmp_chunk_num + record->chunk_num < rewrite_max){
            tmp_chunk_num += record->chunk_num;
            iter = g_sequence_iter_next(iter);	
        }else{
            iter = g_sequence_iter_prev(iter);
            break;   
        }
    }

    int RCrw = 0;
    struct containerRecord* record;
    if (iter == NULL) {
        record = g_sequence_get(iter);
        RCrw = record->chunk_num;
    }

    g_sequence_sort(rewrite_buffer.container_record_seq,
            g_record_descmp_by_chunk_num, NULL);

    int Creads = destor.rewrite_capping_level * current_segment - container_reads_sum;
    int RCreads = 0;
    iter = g_sequence_get_begin_iter(
            rewrite_buffer.container_record_seq);
    if(Creads < length){
        for (i = 0; i < Creads; i++) {
            assert(!g_sequence_iter_is_end(iter));
            iter = g_sequence_iter_next(iter);            
        } 
        record = g_sequence_get(iter);
        RCreads = record->chunk_num;
    }


    int Tdc = RCrw < RCreads ? RCrw : 
    T_last_segment > RCreads && T_last_segment < RCrw ? 
    T_last_segment: (RCrw + RCreads) / 2;

    T_last_segment = Tdc;  

    
    iter = g_sequence_get_begin_iter(
            rewrite_buffer.container_record_seq);
    for (i = 0; i < length; i++) {
        assert(!g_sequence_iter_is_end(iter));
        struct containerRecord* record = g_sequence_get(iter);
        if(record->chunk_num > Tdc){
            struct containerRecord* r = (struct containerRecord*) malloc(
                    sizeof(struct containerRecord));
            memcpy(r, record, sizeof(struct containerRecord));
            r->out_of_order = 0;
            g_hash_table_insert(top, &r->cid, r);
            iter = g_sequence_iter_next(iter);
            //printf("%d(%d)   ",r->cid,r->size);   
        }else
            break;     
    } 
    
    if(count % 4 == 0){
        container_reads_sum += i;
    }

	g_sequence_sort(rewrite_buffer.container_record_seq, g_record_cmp_by_id, NULL);
}

int sum = 0;
int count = 0;
int32_t num;
void *lbw_rewrite(void* arg) {
	top = g_hash_table_new_full(g_int64_hash, g_int64_equal, NULL, free);
    
    num = destor.rewrite_capping_level;

    if(destor.last_unique_chunk_num > 0){
        Nrw = destor.last_unique_chunk_num * destor.rewrite_lbw_drd_limit / (1 - destor.rewrite_lbw_drd_limit);
        Hrw = Nrw / (destor.last_chunk_num / (SEGMENT_LENGTH * 5));       
    }

	while (1) {
		struct chunk *c = sync_queue_pop(dedup_queue);

		if (c == NULL)
			break;

		TIMER_DECLARE(1);
		TIMER_BEGIN(1);
        g_queue_push_tail(rewrite_buffer.chunk_queue, c);
         
        if(CHECK_CHUNK(c, CHUNK_SEGMENT_END)){
            if(rewrite_buffer.num < 5){
                rewrite_buffer.num++;
                continue;
            }         
        }else{
            continue;
        }
        //printf("%d\n",rewrite_buffer.num);
        
        if(++count % 5 == 0){
            current_segment ++;                     
        }
    
        if(destor.last_unique_chunk_num > 0){
		    lbw_segment_get_top(count);
        }else{
            cap_segment_get_top();
        }
        

		while (sum++ < SEGMENT_LENGTH) {
	        struct chunk* c = g_queue_pop_head(rewrite_buffer.chunk_queue);
			if (CHECK_CHUNK(c, CHUNK_CANDIDATE)) {
				if (g_hash_table_lookup(top, &c->id) == NULL) {
					/* not in TOP */
					SET_CHUNK(c, CHUNK_OUT_OF_ORDER);
                    rewrite_sum ++;
					VERBOSE("Rewrite phase: %lldth chunk is in out-of-order container %lld",
							chunk_num, c->id);
				}
				chunk_num++;
			}
			TIMER_END(1, jcr.rewrite_time);
			sync_queue_push(rewrite_queue, c);
			TIMER_BEGIN(1);
		}
        rewrite_buffer.num --;

        GList *list = rewrite_buffer.chunk_queue->head;
        while (sum++ < SEGMENT_LENGTH * 4){           
            c = list->data;
            if(CHECK_CHUNK(c, CHUNK_CANDIDATE) && g_hash_table_lookup(top, &c->id) != NULL){
                SET_CHUNK(c, CHUNK_REWRITE_DENIED);
            }
            list = g_list_next(list);
        }

        while (sum++ < SEGMENT_LENGTH * 5){
            c = list->data;
            if (!CHECK_CHUNK(c,	CHUNK_FILE_START) 
					&& !CHECK_CHUNK(c, CHUNK_FILE_END)
					&& !CHECK_CHUNK(c, CHUNK_SEGMENT_START) 
					&& !CHECK_CHUNK(c, CHUNK_SEGMENT_END)
					&& CHECK_CHUNK(c, CHUNK_DUPLICATE)) {
				if (g_hash_table_lookup(top, &c->id) == NULL) {
					/* not in TOP */
					SET_CHUNK(c, CHUNK_CANDIDATE);					
				}				
			}
            list = g_list_next(list);
        }
		sum = 0;

		g_hash_table_remove_all(top);

	}

	if(destor.last_unique_chunk_num > 0){
        lbw_segment_get_top(count);
    }else{
        cap_segment_get_top();
    }

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