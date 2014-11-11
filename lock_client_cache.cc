// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include "tprintf.h"


lock_client_cache::lock_client_cache(std::string xdst, 				     class lock_release_user *_lu)
  : lock_client(xdst), lu(_lu)
{
  VERIFY(pthread_mutex_init(&mutex, NULL) == 0);
  VERIFY(pthread_cond_init(&cond, NULL) == 0);
//  VERIFY(pthread_cond_init(&server_cond, NULL) == 0);
  rpcs *rlsrpc = new rpcs(0);
  rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache::revoke_handler);
  rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry_handler);

  const char *hname;
  hname = "127.0.0.1";
  std::ostringstream host;
  host << hname << ":" << rlsrpc->port();
  id = host.str();
}

lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid)
{
  pthread_mutex_lock(&mutex);
  tprintf("[msx-log] Acquire, id = %s, lid = %016llx.\n", id.c_str(), lid);
  int r;
  std::map<lock_protocol::lockid_t, lock_client_cache::status>::iterator it;
  it = lock_client_map.find(lid);
  if(it == lock_client_map.end())
  {  
     tprintf("[msx-log] NONE, id = %s, there is no lock locally, need to acquire to server.\n", id.c_str());
     lock_client_cache::status state = lock_client_cache::NONE; 
     lock_client_map.insert(std::pair<lock_protocol::lockid_t, lock_client_cache::status>(lid, lock_client_cache::NONE));
     while(state != lock_client_cache::LOCKED)
     {   
        
        std::map<lock_protocol::lockid_t, int>::iterator retry_it;
        retry_it = retry_map.find(lid);
        if(retry_it != retry_map.end())
          retry_map.erase(retry_it);
        pthread_mutex_unlock(&mutex);
        std::map<lock_protocol::lockid_t, lock_client_cache::status>::iterator ii;
        ii = lock_client_map.find(lid);
        ii->second = lock_client_cache::REQUESTING;
        lock_protocol::status server_state = cl->call(lock_protocol::acquire, lid, id, r);   
        pthread_mutex_lock(&mutex);
        tprintf("[msx-log] id = %s, The server state = %d.\n",id.c_str(),  server_state);   
        if(server_state == lock_protocol::OK)
        {   
           tprintf("[msx-log] id = %s, It is OK to own this lock, and  insert the lock LOCKED.\n", id.c_str()); 
           std::map<lock_protocol::lockid_t, lock_client_cache::status>::iterator itt;
           itt = lock_client_map.find(lid);
           itt->second = lock_client_cache::LOCKED;
        }
        else if(server_state == lock_protocol::RETRY)
        {   
           tprintf("[msx-log] id = %s, The lock is owned by other client, and need to wait.\n", id.c_str());
           std::map<lock_protocol::lockid_t, int>::iterator retry_it;
           retry_it = retry_map.find(lid);
           if(retry_it == retry_map.end())
             pthread_cond_wait(&cond, &mutex);
        }
        state = lock_client_map.find(lid)->second;
      }
   }
   else
   {
      tprintf("[msx-log] id = %s, The lock is local.\n", id.c_str());
      lock_client_cache::status state = it->second;
      if(state == lock_client_cache::FREE)
      {
         tprintf("[msx-log] id = %s, The lock is FREE, change to LOCKED\n", id.c_str());
         it->second = lock_client_cache::LOCKED;
      }
      else
      {
         while(state != lock_client_cache::FREE)
         {
            tprintf("[msx-log] id = %s, The lock is held by others locally, need to wait\n", id.c_str());
            pthread_cond_wait(&cond, &mutex);
            state = lock_client_map.find(lid)->second;
         }
         lock_client_map.find(lid)->second = lock_client_cache::LOCKED;
      }
   }
   pthread_mutex_unlock(&mutex);
   return lock_protocol::OK;
}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{
  pthread_mutex_lock(&mutex);
  tprintf("[msx-log] Release, id = %s, lid = %016llx.\n", id.c_str(), lid);
  int r;
  int ret = lock_protocol::OK;
  std::map<lock_protocol::lockid_t, lock_client_cache::status>::iterator it;
  std::map<lock_protocol::lockid_t, int>::iterator it_revoke;
  it = lock_client_map.find(lid);
  it_revoke = revoked_map.find(lid);

  if(it_revoke != revoked_map.end())
  {
    tprintf("[msx-log] Someone need the lock, Return to Server.\n");
   // pthread_mutex_unlock(&mutex);
    lu->dorelease(lid);
    pthread_mutex_unlock(&mutex);
    //sleep(1);
    int state = cl->call(lock_protocol::release, lid, id,  r);
    pthread_mutex_lock(&mutex);
    lock_client_map.erase(it);
    revoked_map.erase(it_revoke);
    pthread_mutex_unlock(&mutex);
    return ret;
  }
  else
  {
    tprintf("[msx-log] Release locally.\n");
    lock_client_cache::status state = it->second;
    it->second = lock_client_cache::FREE;
    pthread_cond_signal(&cond);
    pthread_mutex_unlock(&mutex);
    return ret;
  }
}

rlock_protocol::status
lock_client_cache::revoke_handler(lock_protocol::lockid_t lid, int &)
{
  pthread_mutex_lock(&mutex);
  int r;
  tprintf("[msx-log] Revoke, id = %s, lid = %016llx.\n", id.c_str(), lid);
  std::map<lock_protocol::lockid_t, lock_client_cache::status>::iterator it;
  it = lock_client_map.find(lid);
  if(it == lock_client_map.end())
  {
    tprintf("[msx-log] It is WRONG to access here!\n"); 
  }
  else
  {
    if(it->second == lock_client_cache::FREE)
    {
      tprintf("[msx-log] The lock is free, Release it to server.\n");
      lu->dorelease(lid);
      pthread_mutex_unlock(&mutex);
      //lu->dorelease(lid);
      pthread_mutex_lock(&mutex);
     // sleep(1);
      lock_client_map.erase(it);
      std::map<lock_protocol::lockid_t, int>::iterator i;
      i = revoked_map.find(lid);
      if(i != revoked_map.end())
      {
        revoked_map.erase(i);
        //revoked_map.insert(std::pair<lock_protocol::lockid_t, int>(lid, 1));
      } 
      
      pthread_mutex_unlock(&mutex);
      //cl->call(lock_protocol::release, lid, id, r);
      return rlock_protocol::RPCERR;
    }  
    lock_client_cache::status state = it->second;
    std::map<lock_protocol::lockid_t, int>::iterator it_revoke;
    it_revoke = revoked_map.find(lid);
    if(it_revoke == revoked_map.end())
    {
       tprintf("[msx-log] Insert a revoke.\n");
       revoked_map.insert(std::pair<lock_protocol::lockid_t, int>(lid, 1));

    }
    pthread_mutex_unlock(&mutex);
    return rlock_protocol::OK;
  }
}

rlock_protocol::status
lock_client_cache::retry_handler(lock_protocol::lockid_t lid, 
                                 int &)
{
  pthread_mutex_lock(&mutex);
  tprintf("[msx-log] Retry, id = %s, lid = %016llx.\n", id.c_str(), lid);
  std::map<lock_protocol::lockid_t, int>::iterator it_retry;
  it_retry = retry_map.find(lid);
  if(it_retry == retry_map.end())
    retry_map.insert(std::pair<lock_protocol::lockid_t, int>(lid, 1));
  pthread_cond_signal(&cond);
  pthread_mutex_unlock(&mutex);
  return lock_protocol::OK;
}



