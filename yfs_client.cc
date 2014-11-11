// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include "lock_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "lock_client_cache.h"

yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
  ec = new extent_client(extent_dst);
  //lc = new lock_client(lock_dst);
  lu = new lock_release_user1(ec);
  lc = new lock_client_cache(lock_dst, lu);
}

yfs_client::inum
yfs_client::n2i(std::string n)
{
  std::istringstream ist(n);
  unsigned long long finum;
  ist >> finum;
  return finum;
}

std::string
yfs_client::filename(inum inum)
{
  std::ostringstream ost;
  ost << inum;
  return ost.str();
}

bool
yfs_client::isfile(inum inum)
{
  if(inum & 0x80000000)
    return true;
  return false;
}

bool
yfs_client::isdir(inum inum)
{
  return ! isfile(inum);
}

int
yfs_client::getfile(inum inum, fileinfo &fin)
{
  int r = OK;
  // You modify this function for Lab 3
  // - hold and release the file lock
  lc->acquire(inum);
  printf("getfile %016llx\n", inum);
  extent_protocol::attr a;
  if (ec->getattr(inum, a) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }

  fin.atime = a.atime;
  fin.mtime = a.mtime;
  fin.ctime = a.ctime;
  fin.size = a.size;
  printf("getfile %016llx -> sz %llu\n", inum, fin.size);

 release:
  lc->release(inum);
  return r;
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
  int r = OK;
  // You modify this function for Lab 3
  // - hold and release the directory lock
  lc->acquire(inum);
  printf("getdir %016llx\n", inum);
  extent_protocol::attr a;
  if (ec->getattr(inum, a) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }
  din.atime = a.atime;
  din.mtime = a.mtime;
  din.ctime = a.ctime;

 release:
  lc->release(inum);
  return r;
}

bool
yfs_client::create(inum parent, inum ino, const char *name)
{
   printf("[msx-log] now in yfs_client create, parent = %016llx, ino = %016llx, name = %s\n", parent, ino, name);
   printf("[msx-log] try to acquire a lock before createing\n"); 
   lc->acquire(parent);
   lc->acquire(ino);
   std::string buf;
   if(ec->get(parent, buf) == extent_protocol::IOERR || isfile(parent))
   { 
      printf("[msx-log] cannot find the parent\n");
      lc->release(ino);
      lc->release(parent);
      return false;
   }
   std::vector<std::string> name_list;
   std::vector<inum> ino_list;
   dispatch(buf, name_list, ino_list);
   std::string name_str = name;
   std::vector<std::string>::iterator it = name_list.begin();
   for(; it<name_list.end(); it++)
   {
      if(*it == name_str)
      {
          printf("[msx-log] the file_name already existed\n");
          break;
      }
   }
   if(it == name_list.end())
   {
      patch(buf, ino, name_str);
      ec->put(parent, buf);
      ec->put(ino, "");
      printf("[msx-log] before return true release the lock\n");
      lc->release(ino);
      lc->release(parent);
      return true;
   }
   else
   {
      printf("[msx-log] before return false release the lock\n");
      lc->release(ino);
      lc->release(parent);
      return false;
   }
   lc->release(ino);
   lc->release(parent);
   return false;
}

bool 
yfs_client::mkdir(inum parent, inum ino, const char* name)
{
   printf("[msx_log] now in yfs_client mkdir, parent = %016llx, ino = %016llx, name = %s\n", parent, ino, name);
   printf("[msx-log] before mkdir acquire a lock\n");
   lc->acquire(parent);
   lc->acquire(ino);
   std::string buf;
   if(ec->get(parent, buf) == extent_protocol::IOERR || isfile(parent))
   {
      printf("[msx-log] cannot find the parent\n");
      lc->release(ino);
      lc->release(parent);
      return false;
   }
   std::vector<std::string> name_list;
   std::vector<inum> ino_list;
   dispatch(buf, name_list, ino_list);
   std::string name_str = name;
   std::vector<std::string>::iterator it = name_list.begin();
   for(; it<name_list.end(); it++)
   {
      if(*it == name_str)
         break;
   }
   if(it == name_list.end())
   {
      patch(buf, ino, name_str);
      ec->put(parent, buf);
      ec->put(ino, "");
      printf("[msx-log] before return true release the lock\n");
      lc->release(ino);
      lc->release(parent);
      return true;
   }
   else
   {
      printf("[msx-log] before return false release the lock\n"); 
      lc->release(ino);
      lc->release(parent);
      return false;
   }
}

void
yfs_client::dispatch(std::string buf, std::vector<std::string> &name_list, std::vector<inum> &ino_list)
{
   int n = 0;
   int nn = 0;
   do
   {
      std::string tmp_str_name = "";
      std::string tmp_str_ino = "";
      std::string tmp_str;
      n = buf.find("&");
      if(-1 == n)
         return;
      tmp_str = buf.substr(0, n);
      nn = tmp_str.find("%");
      tmp_str_name = tmp_str.substr(nn+1, n-nn-1);
      tmp_str_ino = tmp_str.substr(0, nn);
      buf.erase(0, n+1);
      name_list.push_back(tmp_str_name);
      ino_list.push_back(n2i(tmp_str_ino));
   }
   while(true);
}

void
yfs_client::patch(std::string &buf, inum ino, std::string name)
{
   std::string ino_str = filename(ino);
   buf = buf + ino_str + "%" + name + "&";
}

std::string
yfs_client::patchAll(std::vector<inum> ino_list, std::vector<std::string> name_list)
{
   std::string buf;
   for(int i=0; i<name_list.size(); i++)
   {
      std::string ino_str = filename(ino_list[i]);
      buf = buf + ino_str + "%" + name_list[i] + "&";
   }
   return buf;
}

bool
yfs_client::readdir(inum id, std::vector<std::string> &name_list, std::vector<inum> &ino_list)
{
   printf("[msx-log] now in yfs_client readdir, id = %016llx\n", id);
   lc->acquire(id);
   std::string buf;
   if(ec->get(id, buf) == extent_protocol::IOERR)
   {
      lc->release(id);
      return false;
   }
   //printf("[msx-log] readdir buf is %s\n", buf.c_str());
   dispatch(buf, name_list, ino_list);
   lc->release(id);
   return true;
}

void
yfs_client::unlink(inum parent, inum fileid)
{
   printf("[msx_log] now in yfs_client unlink, parent = %016llx, fileid = %016llx\n", parent, fileid);
   printf("[msx-log] before unlink a file acquire a lock\n");
   lc->acquire(parent);
   lc->acquire(fileid);
   ec->remove(fileid);
   std::vector<std::string> name_list;
   std::vector<inum> ino_list;
   //readdir(parent, name_list, ino_list);
   bool flag;
   std::string buf;
   if(ec->get(parent, buf) == extent_protocol::IOERR)
     flag = false;
   else
   {
     dispatch(buf, name_list, ino_list);
     flag = true;
   }
   std::string dir_buf;
   for(int i=0; i<ino_list.size(); i++)
   {
      if(fileid == ino_list[i])
      {
         ino_list.erase(ino_list.begin()+i);
         name_list.erase(name_list.begin()+i);
      }
   }
   std::string dir_buf_unlinked;
   dir_buf_unlinked = patchAll(ino_list, name_list);
   //printf("[msx-log] after patchAll the buf is %s\n", dir_buf_unlinked.c_str());
   if(ec->put(parent, dir_buf_unlinked) == extent_protocol::OK)
   {
      printf("[msx_log] finished unlinking\n");
      printf("[msx-log] before return release the lock\n");
      lc->release(fileid);
      lc->release(parent);
      return;
   }
   printf("[msx-log] before return release the lock\n"); 
   lc->release(fileid);
   lc->release(parent);
}
 
unsigned long
yfs_client::lookup(inum parent, const char *name)
{
   printf("[msx-log] now in yfs_client lookup , parent = %016llx, name = %s\n", parent, name);
   lc->acquire(parent);
   std::string name_str = name;
   std::vector<std::string> name_list;
   std::vector<inum> ino_list;

   bool flag;
   std::string buf;
   if(ec->get(parent, buf) == extent_protocol::IOERR)
     flag = false;
   else
   {
     dispatch(buf, name_list, ino_list);
     flag = true;
   }
   
   if(flag == false)
   {
      printf("[msx-log] readdir() == false, we cannot find the parent in server\n");
      lc->release(parent);
      return 0;
   }
   printf("[msx-log] after readdir() , and we found the parent dir, the size of it = %d\n", name_list.size());
   for(int i=0; i<name_list.size(); i++)
   {
      printf("[msx-log] yfs_client name_list item = %s\n", name_list[i].c_str());
      if(name_list[i] == name_str)
      {
         printf("[msx-log] found name equals, name = %s\n", name_str.c_str());
         lc->release(parent);
         return ino_list[i];
      }
   }
   lc->release(parent);
   return 0;
}

bool
yfs_client::write(inum ino, const char* buf, size_t size, off_t off)
{
   printf("[msx-log] now in yfs_client write\n");
   lc->acquire(ino);
   std::string buf_str = std::string(buf, size);
   printf("[msx-log] size = %d, off = %lld, ino = %016llx\n",  size, off, ino);
   printf("[msx-log] before write acquire a lock\n");
   //lc->acquire(ino);
   std::string current_buf;
   size_t current_size;
   size_t written_size; 
   if(ec->get(ino, current_buf) == extent_protocol::IOERR || isdir(ino))
   {
      printf("[msx-log] before return false release the lock\n");
      lc->release(ino);
      return false;
   }
   current_size = current_buf.size();
   //printf("[msx-log] before written: current_buf = %s, buf_size = %d\n", current_buf.c_str(), current_size);
   written_size = off + size;
   if(written_size > current_size)
   {
      current_buf.resize(written_size);
      int j = 0;
      for(int i=off; i<written_size; i++)
         current_buf[i] = buf_str[j++];
   }
   else
   {
      int j = 0;
      for(int i=off; i<written_size; i++)
         current_buf[i] = buf_str[j++];
   }

   //printf("[msx-log] after written: current_size = %d, current_buf = %s\n", current_buf.size(), current_buf.c_str());
   if(ec->put(ino, current_buf) == extent_protocol::OK)
   { 
      printf("[msx-log] after writing to the server\n");
      printf("[msx-log] before return true release the lock\n");
      lc->release(ino);
      return true;
   }
   else
   {
      printf("[msx-log] before return false release the lock\n"); 
      lc->release(ino);
      return false;
   }
}

bool
yfs_client::read(inum ino, size_t size, off_t off, std::string &buf)
{
   printf("read in yfs_client, size = %d, off = %lld\n", size, off);
   printf("initially buf = %s\n", buf.c_str());
   lc->acquire(ino);
   std::string buf_server;
   if(ec->get(ino, buf_server) == extent_protocol::IOERR)
   {
      printf("failed to read buf from server\n");
      lc->release(ino);
      return false;
   }
   size_t buf_server_size = buf_server.size();
   printf("buf_server_size = %d\n", buf_server_size);
   if(off >= buf_server_size)
   {
      printf("off >= buf_server_size\n");
      buf = "";
      lc->release(ino);
      return true;
   }
   else
   {
      if(off + size > buf_server_size)
      {
         //printf("off + size > buf_server_size\n");
         //printf("off = %lld, buf_server_size = %d\n", off, buf_server_size);
         buf.resize(buf_server_size - off);
         //printf("the buf size after resizing in branch1= %d\n", buf.size());
         //printf("the beginning bits of buf = %c, %c, %c\n", buf_server[0], buf_server[1], buf_server[buf_server_size-1]);
         int j = 0;
         for(int i=off; i<buf_server_size; i++)
         {
            //printf("now we are in the loop of readinf\n"); 
            buf[j++] = buf_server[i];
            //printf("buf[%d] = %c, buf_server[%d] = %c\n", j-1,buf[j-1], i, buf_server[i]);
         }
      }
      else
      {
         printf("off + size <= buf_server_size\n");
         buf.resize(size);
         printf("the buf size after resizing in branch2 = %d\n", buf.size());
         int j = 0;
         for(int i=off; i<(off+size); i++)
           buf[j++] = buf_server[i];
      }
   }
   //printf("finally the buf read = %s\n", buf.c_str());
   lc->release(ino);
   return true;
}

bool
yfs_client::setattr(inum ino, unsigned long long size)
{
   printf("[msx-log] now in yfs_client setattr, ino = %016llx, size = %llx\n", ino, size);
   printf("[msx-log] before setattr acquire a lock\n");
   lc->acquire(ino);
   std::string buf_server;
   int buf_server_size;
   if(ec->get(ino, buf_server) == extent_protocol::IOERR)
   {
      printf("[msx-log] before return false, release the lock\n");
      lc->release(ino);
      return false;
   }
   buf_server_size = buf_server.size();
   if(size > buf_server_size)
   {
      printf("size > buf_server_size\n");
      buf_server.resize(size);  
      for(int i=buf_server_size; i<size; i++)
         buf_server[i] = '\0';
   }
   else
   {
      printf("size <= buf_server_size\n");
      buf_server.resize(size);
   }
   printf("buf_server_size = %d\n", buf_server.size());
   if(ec->put(ino, buf_server) == extent_protocol::OK)
   {
      printf("[msx-log] before return true , release the lock\n");
      lc->release(ino);
      return true; 
   }
   else
   {
      printf("[msx-log] before return false, release the lock\n");
      lc->release(ino);
      return false;
   }
}
