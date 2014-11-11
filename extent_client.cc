// RPC stubs for clients to talk to extent_server

#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <time.h>
#include "tprintf.h"

// The calls assume that the caller holds a lock on the extent

extent_client::extent_client(std::string dst)
{
  VERIFY(pthread_mutex_init(&mutex, NULL) == 0);
  sockaddr_in dstsock;
  make_sockaddr(dst.c_str(), &dstsock);
  cl = new rpcc(dstsock);
  if (cl->bind() != 0) {
    printf("extent_client: bind failed\n");
  }
}

extent_protocol::status
extent_client::get(extent_protocol::extentid_t eid, std::string &buf)
{
  /*extent_protocol::status ret = extent_protocol::OK;
  ret = cl->call(extent_protocol::get, eid, buf);
  return ret;*/
  pthread_mutex_lock(&mutex);
  tprintf("[msx-log] Extent_client::get, id = %016llx.\n", eid);
  std::map<extent_protocol::extentid_t, std::string>::iterator dir_it;
  std::map<extent_protocol::extentid_t, extent_protocol::attr>::iterator attr_it;
  std::map<extent_protocol::extentid_t, int>::iterator stat_it;
  dir_it = dir_map.find(eid);
  attr_it = attr_map.find(eid);
  stat_it = dir_stat.find(eid);

  std::vector<extent_protocol::extentid_t>::iterator removed_it = removed_list.begin();
  
  for(; removed_it<removed_list.end(); removed_it++)
  {
     //tprintf("[msx-log] removedid = %016llx eid = %016llx.\n",*removed_it, eid);
     if(*removed_it == eid)
        break;
  }
  if(removed_it != removed_list.end())
  {
    tprintf("[msx-log] The eid has already  been removed locally , eid = %016llx.\n", eid);
    pthread_mutex_unlock(&mutex);
    return extent_protocol::IOERR;
  }
 
  if(dir_it != dir_map.end())
  {
    tprintf("[msx-log] The dir is found locally, no need to get from server.\n");
    buf = dir_it->second;
    attr_it->second.atime = time(NULL);
    pthread_mutex_unlock(&mutex);
    return extent_protocol::OK;
  }
  else
  {
    tprintf("[msx-log] Cannot find the dir locally, so get from server.\n");
    extent_protocol::status ret;
    ret = cl->call(extent_protocol::get, eid, buf);
    dir_map.insert(std::pair<extent_protocol::extentid_t, std::string>(eid, buf));
    dir_stat.insert(std::pair<extent_protocol::extentid_t, int>(eid, 0));
    extent_protocol::attr attr;
    ret = cl->call(extent_protocol::getattr, eid, attr);
    attr_map.insert(std::pair<extent_protocol::extentid_t, extent_protocol::attr>(eid, attr));
    pthread_mutex_unlock(&mutex);
    return extent_protocol::OK;
  }
}

extent_protocol::status
extent_client::getattr(extent_protocol::extentid_t eid, 
		       extent_protocol::attr &attr)
{
  /*extent_protocol::status ret = extent_protocol::OK;
  ret = cl->call(extent_protocol::getattr, eid, attr);
  return ret;*/
  pthread_mutex_lock(&mutex);
  tprintf("[msx-log] Extent_client::getattr, id = %016llx.\n", eid);
  std::map<extent_protocol::extentid_t, extent_protocol::attr>::iterator attr_it;
  std::map<extent_protocol::extentid_t, int>::iterator stat_it;
  attr_it = attr_map.find(eid);
  stat_it = dir_stat.find(eid);
  extent_protocol::status ret;
  if(attr_it == attr_map.end())
  {
    tprintf("[msx-log] GetAttr fron server.\n");
    ret = cl->call(extent_protocol::getattr, eid, attr);
    tprintf("[msx-log] Finished getattr from server.\n");
    attr_map.insert(std::pair<extent_protocol::extentid_t, extent_protocol::attr>(eid, attr));
  }
  else if(attr_it!= attr_map.end())
  {
    tprintf("[msx-log] GetAttr locally.\n");
    attr = attr_it->second;
  }
  pthread_mutex_unlock(&mutex);
  return extent_protocol::OK;
}

extent_protocol::status
extent_client::put(extent_protocol::extentid_t eid, std::string buf)
{
  /*extent_protocol::status ret = extent_protocol::OK;
  int r;
  ret = cl->call(extent_protocol::put, eid, buf, r);
  return ret;*/
  pthread_mutex_lock(&mutex);
  tprintf("[msx-log] Extent_client::put, id = %016llx.\n", eid);
  std::map<extent_protocol::extentid_t, std::string>::iterator dir_it;
  std::map<extent_protocol::extentid_t, extent_protocol::attr>::iterator attr_it;
  std::map<extent_protocol::extentid_t, int>::iterator stat_it;
  dir_it = dir_map.find(eid);
  attr_it = attr_map.find(eid);
  stat_it = dir_stat.find(eid);
  if(dir_it == dir_map.end())
  {
    tprintf("[msx-log] Creating operatoin locally.\n");
    dir_map.insert(std::pair<extent_protocol::extentid_t, std::string>(eid, buf));
    dir_stat.insert(std::pair<extent_protocol::extentid_t, int>(eid, 1));
    extent_protocol::attr a;
    time_t now = time(NULL);
    a.ctime = now;
    a.mtime = now;
    tprintf("[msx-log] buf =@%s@.\n", buf.c_str());
    a.size = 0;
    attr_map.insert(std::pair<extent_protocol::extentid_t, extent_protocol::attr>(eid, a));
  }
  else
  {
    tprintf("[msx-log] Modification operation locally.\n");
    dir_it->second = buf;
    //tprintf("[msx-log] buf = = = %s.\n", buf.c_str());
    stat_it->second = 1;
    time_t now = time(NULL);
    attr_it->second.mtime = now;
    attr_it->second.ctime = now;
    attr_it->second.size = buf.size();
    tprintf("[msx-log] attr size = %d.\n", attr_it->second.size);
  }
  extent_protocol::status ret;
  int r;
  //ret = cl->call(extent_protocol::put, eid, buf, r);
  pthread_mutex_unlock(&mutex);
  return extent_protocol::OK;
}

extent_protocol::status
extent_client::remove(extent_protocol::extentid_t eid)
{
  /*extent_protocol::status ret = extent_protocol::OK;
  int r;
  ret = cl->call(extent_protocol::remove, eid, r);
  return ret;*/
  pthread_mutex_lock(&mutex);
  tprintf("[msx-log] Extent_client::remove, id = %016llx.\n", eid);
  extent_protocol::status ret;
  int r;
  std::vector<extent_protocol::extentid_t>::iterator removed_it = removed_list.begin();
  
  for(; removed_it<removed_list.end(); removed_it++)
  {
     //tprintf("[msx-log] removedid = %016llx eid = %016llx.\n",*removed_it, eid);
     if(*removed_it == eid)
        break;
  }
  if(removed_it == removed_list.end())
  {
    tprintf("[msx-log] Insert in removed list , eid = %016llx.\n", eid);
    removed_list.push_back(eid);
    dir_map.erase(eid);
    attr_map.erase(eid);
    dir_stat.erase(eid);
  }
  //ret = cl->call(extent_protocol::remove, eid, r);
  pthread_mutex_unlock(&mutex);
  return extent_protocol::OK;
}

extent_protocol::status
extent_client::flush(extent_protocol::extentid_t eid)
{
  pthread_mutex_lock(&mutex);
  tprintf("[msx-log] Extent_client::flush.\n");
  extent_protocol::status ret;
  int r;
  std::string buf;
  std::map<extent_protocol::extentid_t, int>::iterator stat_it;
  std::map<extent_protocol::extentid_t, std::string>::iterator dir_it;
  std::map<extent_protocol::extentid_t, extent_protocol::attr>::iterator attr_it;
  stat_it = dir_stat.find(eid);
   
  int state;
  dir_it = dir_map.find(eid);
  attr_it = attr_map.find(eid);
  if(dir_it != dir_map.end())
  {
     buf = dir_it->second;
     state = stat_it->second;
     tprintf("[msx-log] Delete the lcoal extent.\n");
     dir_map.erase(eid);
     attr_map.erase(eid);  
     dir_stat.erase(eid);    
 
     if(state == 0)
     {
       tprintf("[msx-log] No change.\n");
     }
     else if(state == 1)
     {
       tprintf("[msx-log] Modified, so do extent_server put.\n");
       ret = cl->call(extent_protocol::put, eid, buf, r);
       tprintf("[msx-log] Finished RPC Call.\n");
     }
  }

  std::vector<extent_protocol::extentid_t>::iterator removed_it = removed_list.begin();  
  for(; removed_it<removed_list.end(); removed_it++)
  {
     //tprintf("[msx-log] removedid = %016llx eid = %016llx.\n",*removed_it, eid);
     if(*removed_it == eid)
        break;
  }
  if(removed_it != removed_list.end())
  {
    tprintf("[msx-log] find the eid in removed list , eid = %016llx.\n", eid);
    tprintf("[msx-log] The eid need to be removed in server.\n"); 
    ret = cl->call(extent_protocol::remove, eid, r);
    removed_list.erase(removed_it);   
  }
  tprintf("[msx-log] Finished flush.\n");
  pthread_mutex_unlock(&mutex);
  return extent_protocol::OK;
}  
