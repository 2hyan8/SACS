#include "main.h"
#include "jcr.h"
#include "rewrite_phase.h"
#include "filter_phase.h"
#include "containerstore.h"

static int64_t chunk_num;

struct{
	/* accessed in dedup phase */
	struct container *container_buffer;
	/* In order to facilitate sampling in container,
	 * we keep a list for chunks in container buffer. */
	GSequence *chunks;
} storage_buffer;

int ck_size = 0;
double write_time;
int container_num = 1;
GQueue *chunk_queue;
GList *temp;
GSequence *chunks_candi;
GList *container_record_seq;
GList *part_container_record_seq;
extern FILE *recipe_file;

void fgw_init(){
	chunk_queue = g_queue_new();
	chunks_candi = g_sequence_new(NULL);
}

int write_recipe(struct chunkpoint *mtdata)
{
	fwrite(mtdata, 1, sizeof(struct chunkpoint), recipe_file);
	return 0;
}

struct meta_chunk {
	GList *container_record_seq;
};

struct meta_chunk* new_meta_chunk() {
	struct meta_chunk* ck = (struct meta_chunk*) malloc(sizeof(struct meta_chunk));
	ck->container_record_seq = g_list_copy(part_container_record_seq);
	return ck;
}

void Add_to_part_container_record_seq (struct chunk* c){
	if (c->id != TEMPORARY_ID) {
		struct containerRecord tmp_record;
		tmp_record.cid = c->id;

		GList *iter = g_list_find_custom(
				part_container_record_seq, &tmp_record,
				g_record_cmp_by_id);
		if (iter == NULL) {
			struct containerRecord* record = malloc(
					sizeof(struct containerRecord));
			record->cid = c->id;
			record->size = c->size;
						
			part_container_record_seq = g_list_append(part_container_record_seq, record);
			
		} else {	
			struct containerRecord* record = iter->data;
			assert(record->cid == c->id);
			record->size += c->size;
			
		}		
	}
}


void Add_part_to_total(){
	while(part_container_record_seq){
		struct containerRecord* tmp_record = part_container_record_seq->data;
		GList *iter = g_list_find_custom(
				container_record_seq, &tmp_record->cid,
				g_record_cmp_by_id);

		if (iter == NULL) {
			struct containerRecord* record = malloc(
					sizeof(struct containerRecord));
			record->cid = tmp_record->cid;
			record->size = tmp_record->size;			
			container_record_seq = g_list_append(container_record_seq, record);
		} else {		
			struct containerRecord* record = iter->data;				
			assert(record->cid == tmp_record->cid);
			record->size += tmp_record->size;
		}		
		
		part_container_record_seq = g_list_next(part_container_record_seq);
	}

}

void Sub_part_from_total(struct meta_chunk* meta){
	while(meta->container_record_seq){
		struct containerRecord* tmp_record = meta->container_record_seq->data;
		GList *iter = g_list_find_custom(
				container_record_seq, &tmp_record->cid,
				g_record_cmp_by_id);
		
		struct containerRecord* record = iter->data;				
		assert(record->cid == tmp_record->cid);
		record->size -= tmp_record->size;
		if(record->size == 0){
			container_record_seq = g_list_delete_link(container_record_seq, iter);
		}		
		meta->container_record_seq = g_list_next(meta->container_record_seq);
	}
}


static GHashTable * cap_segment_get_top() {
	container_record_seq = g_list_sort(container_record_seq, g_record_descmp_by_length);

	int length = g_list_length(container_record_seq);
	
	GHashTable *top = g_hash_table_new_full(g_int64_hash, g_int64_equal, NULL, free);

	GList *iter = g_list_first(container_record_seq);
	for (i = 0; i < length; i++) {
		struct containerRecord* record = iter->data;
		if(record->size > 1048576/16){
			struct containerRecord* r = (struct containerRecord*) malloc(
					sizeof(struct containerRecord));
			memcpy(r, record, sizeof(struct containerRecord));
			g_hash_table_insert(top, &r->cid, r);
		}
		iter = g_list_next(iter);
	}
	
	return top;
}

TIMER_DECLARE(1);
TIMER_DECLARE(2);
int mark;
void fgw_rewrite(struct chunk* c){
	TIMER_BEGIN(1);
	g_queue_push_tail(chunk_queue, c);	
	

    if(ck_size < destor.rewrite_algorithm[1] * CONTAINER_SIZE - c->size) {
		if(ck_size > CONTAINER_SIZE * container_num){
			struct chunk* se = new_chunk(0);
			SET_CHUNK(se, CHUNK_SEGMENT_END);
			g_queue_push_tail(chunk_queue, se);
			container_num ++;
		}
		ck_size += c->size;
		return;
	}
	

	struct chunk* se = new_chunk(0);
	SET_CHUNK(se, CHUNK_SEGMENT_END);
	g_queue_push_tail(chunk_queue, se);
	
	GHashTable *top = cap_segment_get_top();      

  	if(g_list_length(temp) == 0){
		temp = chunk_queue->head;
	}else{
		temp = g_list_next(temp);
	}

	while(temp != NULL){
		c = temp->data;  
		if(CHECK_CHUNK(c, CHUNK_SEGMENT_END)) {
			struct meta_chunk* ck = new_meta_chunk();
			if(temp->next){
				g_queue_insert_before(chunk_queue, temp->next, ck);
			}else{
				g_queue_push_tail(chunk_queue, ck);
			}
			Add_part_to_total();
			g_sequence_append(chunks_candi, c);
			temp = g_list_next(temp);
		}else if (CHECK_CHUNK(c, CHUNK_DUPLICATE) && g_hash_table_lookup(top, &c->id) == NULL 
								&& (!CHECK_CHUNK(c, CHUNK_REWRITE_DENIED))) {     
				mark++;           
				g_sequence_append(chunks_candi, c);
		}else if (!CHECK_CHUNK(c, CHUNK_DUPLICATE)){
				TIMER_END(1, jcr.rewrite_time);
				TIMER_BEGIN(2);
				TIMER_END(2, write_time);
				write_to_container(c); 
				TIMER_BEGIN(1);
		}else{
				jcr.dedup_size += c->size;
				jcr.dedup_chunk_num++;
		}		
		Add_to_part_container_record_seq(c);
		temp = g_list_next(temp);		
	}

	while(1){
		c = g_queue_pop_head(chunk_queue);
		if(CHECK_CHUNK(c, CHUNK_SEGMENT_END)){
			struct meta_chunk* ck = g_queue_pop_head(chunk_queue);			
			Sub_part_from_total(ck);			
			break;			
		}
        ck_size -= c->size;
		write_recipe(c->ck);
	}
    container_num --;

  	GSequenceIter *iter = g_sequence_get_begin_iter(chunks_candi);
	GSequenceIter *end = g_sequence_get_end_iter(chunks_candi);
    int flag = 0; 
    for(; iter != end  ; ){
        c = g_sequence_get(iter);  
        if(g_hash_table_lookup(top, &c->id)){
            jcr.dedup_size += c->size;
            jcr.dedup_chunk_num++;
            GSequenceIter *chunk_delete = iter;
            iter = g_sequence_iter_next(iter);
            g_sequence_remove(chunk_delete);
        }else if(flag == 0){
            if(!CHECK_CHUNK(c, CHUNK_SEGMENT_END)){
                SET_CHUNK(c, CHUNK_SEG);
				TIMER_END(1, jcr.rewrite_time);
				TIMER_BEGIN(2);
				TIMER_END(2, write_time);
                write_to_container(c);
				TIMER_BEGIN(1);
            }else{
                flag = 1;
            }
            GSequenceIter *chunk_delete = iter;
            iter = g_sequence_iter_next(iter);
            g_sequence_remove(chunk_delete);
        }else if(flag == 1){
            iter = g_sequence_iter_next(iter);
        }        
    }
    
    g_hash_table_remove_all(top);

    temp = chunk_queue->tail;
	TIMER_END(1, jcr.rewrite_time);
    return NULL;
}

void close_fgw_rewrite_buffer(){
	struct chunk *c = NULL;

    if(g_list_length(temp) == 0){
        temp = chunk_queue->head;
    }else{
		temp = g_list_next(temp);
	}

	GHashTable *top = cap_segment_get_top(); 

    if(temp != chunk_queue->tail){              
        while(temp != NULL){
            c = temp->data;
            if(!CHECK_CHUNK(c, CHUNK_SEGMENT_END)){                
                if (!CHECK_CHUNK(c, CHUNK_DUPLICATE)){
                    write_to_container(c);                    
                }else if (CHECK_CHUNK(c, CHUNK_DUPLICATE) && g_hash_table_lookup(top, &c->id) == NULL 
                            && (!CHECK_CHUNK(c, CHUNK_REWRITE_DENIED))) {                
                    SET_CHUNK(c, CHUNK_SEG);
                    write_to_container(c);
                }else{
                    jcr.dedup_size += c->size;
                    jcr.dedup_chunk_num++;
                }               
            }
            temp = g_list_next(temp);
        }
        g_hash_table_remove_all(top);
    }

    if(g_sequence_get_length(chunks_candi) > 0){
        struct chunk* se = new_chunk(0);
        SET_CHUNK(se, CHUNK_SEGMENT_END);
        rewrite_buffer_push(se);
        GSequenceIter *iter = g_sequence_get_begin_iter(chunks_candi);
        GSequenceIter *end = g_sequence_get_end_iter(chunks_candi);
         
        for(; iter != end  ; ){
            c = g_sequence_get(iter); 
            if(g_hash_table_lookup(top, &c->id)){
                jcr.dedup_size += c->size;
                jcr.dedup_chunk_num++;
            }else if(!CHECK_CHUNK(c, CHUNK_SEGMENT_END)){
                SET_CHUNK(c, CHUNK_SEG);
                write_to_container(c);          
            }
            GSequenceIter *chunk_delete = iter;
            iter = g_sequence_iter_next(iter);
            g_sequence_remove(chunk_delete);           
        }
    }

	while(c = g_queue_pop_head(chunk_queue)){
		if(CHECK_CHUNK(c, CHUNK_SEGMENT_END)){
			continue;
		}
		write_recipe(c->ck);
	}

	return NULL;	
}