// the extent server implementation

#include "extent_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

extent_server::extent_server()
{
   dir_map.insert(std::pair<extent_protocol::extentid_t, std::string>(0x00000001, ""));
   extent_protocol::attr a;
   a.atime = time(NULL);
   a.ctime = time(NULL);
   a.mtime = time(NULL);
   attr_map.insert(std::pair<extent_protocol::extentid_t, extent_protocol::attr>(0x00000001, a));
}


int extent_server::put(extent_protocol::extentid_t id, std::string buf, int &)
{
  printf("[msx-log] now in extent_server put, id = %016llx\n", id);
  std::map<extent_protocol::extentid_t, std::string>::iterator dir_it;
  std::map<extent_protocol::extentid_t, extent_protocol::attr>::iterator attr_it;
  dir_it = dir_map.find(id);
  attr_it = attr_map.find(id);
  if(dir_it == dir_map.end())
  {
     printf("[msx-log] put of creating operation in extent_server\n");
     dir_map.insert(std::pair<extent_protocol::extentid_t, std::string>(id, buf));
     extent_protocol::attr a;
     time_t now = time(NULL);
     a.ctime = now;
     a.mtime = now;
     a.size = buf.size();
     attr_map.insert(std::pair<extent_protocol::extentid_t, extent_protocol::attr>(id, a));
  }
  else
  {
     printf("[msx-log] put of modification operation in extent_server\n");
     dir_it->second = buf;
     time_t now = time(NULL);
     attr_it->second.mtime = now;
     attr_it->second.ctime = now;
     printf("[msx-log] buf = %s.\n", buf.c_str());
     printf("[msx-log] buf size = %d.\n", buf.size());
     attr_it->second.size = buf.size();
  }
  printf("[msx-log] executed put operation successfully\n");
  return extent_protocol::OK;
}

int extent_server::get(extent_protocol::extentid_t id, std::string &buf)
{
  printf("[msx-log] now in extent_server get, id = %016llx\n", id);
  std::map<extent_protocol::extentid_t, std::string>::iterator dir_it;
  dir_it = dir_map.find(id);
  std::map<extent_protocol::extentid_t, extent_protocol::attr>::iterator attr_it;
  attr_it = attr_map.find(id);
  if(dir_it == dir_map.end())
  {
     printf("[msx-log] cannot find this id extent_server::get , id = %016llx\n",id);
     return extent_protocol::IOERR;
  }
  else
  {
     printf("[msx-log] succeed to find this id in extent_server::get, id = %016llx\n", id);
     buf = dir_it->second;
     attr_it->second.atime = time(NULL);
     return extent_protocol::OK;
  }
}

int extent_server::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a)
{
  printf("[msx-log] getattr in server.\n");
  std::map<extent_protocol::extentid_t, extent_protocol::attr>::iterator it;
  it = attr_map.find(id);
 // printf("[msx-log] ");
  if(it == attr_map.end())
     return extent_protocol::IOERR;
  else
  {
     printf("[msx-log] find the attr.\n");
     a = it->second;
     printf("[msx-log] attr size = %d.\n", a.size);
     return extent_protocol::OK;
  }
}

int extent_server::remove(extent_protocol::extentid_t id, int &)
{
  printf("[msx_log] now in extent_server remove, id = %016llx\n", id);
  dir_map.erase(id);
  attr_map.erase(id);
  return extent_protocol::OK;
}

