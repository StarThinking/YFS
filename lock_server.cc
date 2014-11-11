// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

lock_server::lock_server():
  nacquire (0)
{
   VERIFY(pthread_mutex_init(&mutex, NULL) == 0);
   VERIFY(pthread_cond_init(&cond, NULL) == 0);
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("stat request from clt %d\n", clt);
  r = nacquire;
  return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("acquire request from clt %d\n",clt);
  r = nacquire;

  pthread_mutex_lock(&mutex);
  std::map<lock_protocol::lockid_t, int>::iterator it;
  it = lock_map.find(lid);
  if(it == lock_map.end())
  {
    lock_map.insert(std::pair<lock_protocol::lockid_t, int>(lid, 0));
  }
  else
  {
    int locked = it->second;
    while(locked == 0)
    {
       pthread_cond_wait(&cond, &mutex);
       locked = lock_map.find(lid)->second;
    }
    lock_map.find(lid)->second = 0;
  }
  pthread_mutex_unlock(&mutex);
  return ret;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("release request from clt %d\n",clt);
  r = nacquire;

  pthread_mutex_lock(&mutex);
  std::map<lock_protocol::lockid_t, int>::iterator it;
  it = lock_map.find(lid);
  if(it != lock_map.end() && it->second==0)
  {
     it->second = 1;
     pthread_cond_signal(&cond); 
  }
  pthread_mutex_unlock(&mutex);
  return ret;
}

