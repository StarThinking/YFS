#ifndef yfs_client_h
#define yfs_client_h

#include <string>
//#include "yfs_protocol.h"
#include "extent_client.h"
#include <vector>

#include "lock_protocol.h"
#include "lock_client_cache.h"

class yfs_client {
  extent_client *ec;
  lock_client *lc;
  lock_release_user1 *lu;
 public:

  typedef unsigned long long inum;
  enum xxstatus { OK, RPCERR, NOENT, IOERR, EXIST };
  typedef int status;

  struct fileinfo {
    unsigned long long size;
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirinfo {
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirent {
    std::string name;
    yfs_client::inum inum;
  };

 private:
  static std::string filename(inum);
  static inum n2i(std::string);
 public:

  yfs_client(std::string, std::string);

  bool isfile(inum);
  bool isdir(inum);

  int getfile(inum, fileinfo &);
  int getdir(inum, dirinfo &);
  bool create(inum, inum, const char*);
  void dispatch(std::string, std::vector<std::string> &, std::vector<inum> &);
  void patch(std::string &, inum, std::string);
  std::string patchAll(std::vector<inum> , std::vector<std::string>);
  bool readdir(inum, std::vector<std::string> &, std::vector<inum> &);
  unsigned long lookup(inum, const char*);
  bool write(inum, const char*, size_t, off_t);
  bool read(inum, size_t, off_t, std::string &);
  bool setattr(inum, unsigned long long);
  void unlink(inum, inum);
  bool mkdir(inum, inum, const char*);
};
  
#endif 
