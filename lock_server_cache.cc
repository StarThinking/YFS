// the caching lock server implementation

#include "lock_server_cache.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "lang/verify.h"
#include "handle.h"
#include "tprintf.h"
#include <sys/wait.h>

lock_server_cache::lock_server_cache()
{
  VERIFY(pthread_mutex_init(&mutex, NULL) == 0);
  VERIFY(pthread_cond_init(&cond, NULL) == 0);
  rpcing = false;
}

int
lock_server_cache::retry(std::vector<std::string> wl, lock_protocol::lockid_t lid)
{
  tprintf("[msx-log] Retry. Waiting list is = \n");
  int r;
  std::vector<std::string>::iterator it = wl.begin();
    for(; it<wl.end(); )
    {
      std::string cid = *it;
      tprintf("[msx-log] The client id = %s, and RPC to call it.\n", cid.c_str());
      handle h(cid);
      rpcc *cl = h.safebind();
      if(cl)
      {   
        it = wl.erase(it);
        pthread_mutex_unlock(&mutex);
        int state = cl->call(rlock_protocol::retry, lid, r);
        pthread_mutex_lock(&mutex);
        tprintf("[msx-log RPC Retry Back!!!\n]");
      }
      else
        tprintf("[msx-log] RPC call failed.\n");
     }
  
  return lock_protocol::OK;
}

int lock_server_cache::acquire(lock_protocol::lockid_t lid, std::string id, 
                               int &)
{
  pthread_mutex_lock(&mutex);
  while(rpcing == true)
  {
    pthread_cond_wait(&cond, &mutex);
  }
  tprintf("[msx-log] Acquire, lid = %016llx, id = %s.\n", lid, id.c_str());
  std::map<lock_protocol::lockid_t, server_item>::iterator it;
  it = lock_server_map.find(lid);
  tprintf("[msx-log] wl size = %d.\n", it->second.waiting_list.size());
  if(it == lock_server_map.end())
  {
     tprintf("[msx-log] Lockid NONE in lock_server_map, insert it and set the its state to LOCKED, renturn OK.\n");
     server_item item;
     std::vector<std::string> wl;
     item.state = lock_server_cache::LOCKED;
     item.owner = id;
     item.waiting_list = wl;
     lock_server_map.insert(std::pair<lock_protocol::lockid_t, server_item>(lid, item));
     pthread_mutex_unlock(&mutex);
     return lock_protocol::OK;
  }
  else
  {
     if(it->second.state == lock_server_cache::FREE)
     {
        tprintf("[msx-log] The lock exists, and state is FREE, so change to LOCKED and return OK.\n");
        it->second.state = lock_server_cache::LOCKED;
        it->second.owner = id;
        pthread_mutex_unlock(&mutex);
        return lock_protocol::OK;
     }
     else if(it->second.state == lock_server_cache::LOCKED)
     {
        tprintf("[msx-log] State is LOCKED.\n");
        handle h(it->second.owner);
        rpcc *cl = h.safebind();
        int r;
        rlock_protocol::status state;
        //rpcing = true;
        if(it->second.state != lock_server_cache::RELEASING)
        {
          it->second.state = lock_server_cache::RELEASING; 
          if(cl)
          {
            pthread_mutex_unlock(&mutex);
            rpcing = true;
            tprintf("[msx-log] Call Revoke to owner %s.\n", it->second.owner.c_str());
            state = cl->call(rlock_protocol::revoke, lid, r);
            pthread_mutex_lock(&mutex);
          }
          else
            tprintf("[msx-log] RPC call Revoke failed.\n");
        }
        tprintf("[msx-log] Finish RPC Revoke Call.\n");
        rpcing = false;
        pthread_cond_signal(&cond);
        if(state == rlock_protocol::RPCERR)
        {
          it->second.state = lock_server_cache::LOCKED;
          it->second.owner = id;
          pthread_mutex_unlock(&mutex);
          return lock_protocol::OK;
        }
        it->second.waiting_list.push_back(id);
        pthread_mutex_unlock(&mutex);
        tprintf("[msx-log] wl size is %d.\n", it->second.waiting_list.size());
        return lock_protocol::RETRY;
     }
     else if(it->second.state == lock_server_cache::RELEASING)
     {
        tprintf("[msx-log] The lock exists, and state is RELEASING, so put the id into waiting list and return RETRY.\n");
        it->second.waiting_list.push_back(id);
        pthread_mutex_unlock(&mutex);
        tprintf("[msx-log] wl size is %d.\n", it->second.waiting_list.size());
        return lock_protocol::RETRY;
     }
  }
}

int 
lock_server_cache::release(lock_protocol::lockid_t lid, std::string id, 
         int &r)
{
  pthread_mutex_lock(&mutex);
  tprintf("[msx-log] Release, lid = %016llx, id = %s..\n", lid, id.c_str());
  std::map<lock_protocol::lockid_t, server_item>::iterator it;
  it = lock_server_map.find(lid);
  if(it == lock_server_map.end())
  {
    tprintf("[msx-log] It is wrong to access here!");
  }
  else
  {
    tprintf("[msx-log] Change State to FREE, and do Retry.\n");
    it->second.state = lock_server_cache::FREE;
    tprintf("[msx-log] Waiting list size = %d\n.", it->second.waiting_list.size());
    int state = retry(it->second.waiting_list, lid);
  }
  pthread_mutex_unlock(&mutex);
  return lock_protocol::OK;
}

lock_protocol::status
lock_server_cache::stat(lock_protocol::lockid_t lid, int &r)
{
  tprintf("stat request\n");
  r = nacquire;
  return lock_protocol::OK;
}

