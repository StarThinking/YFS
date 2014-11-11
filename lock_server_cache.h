#ifndef lock_server_cache_h
#define lock_server_cache_h

#include <string>

#include <map>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_server.h"

typedef struct{
   int  state;
   std::string owner;
   std::vector<std::string> waiting_list;
}server_item;

class lock_server_cache { 
 private:
  int nacquire;
  pthread_mutex_t mutex;
  pthread_cond_t cond;
  std::map<lock_protocol::lockid_t, server_item> lock_server_map;
  void revoke(std::string id, lock_protocol::lockid_t);
  int retry(std::vector<std::string>, lock_protocol::lockid_t);
  bool rpcing;
  
 public:
  lock_server_cache();
  lock_protocol::status stat(lock_protocol::lockid_t, int &);
  int acquire(lock_protocol::lockid_t, std::string id, int &);
  int release(lock_protocol::lockid_t, std::string id,  int &); 
  enum xxstatus {NONE, FREE, LOCKED, RELEASING};
  typedef int status;
};

#endif
