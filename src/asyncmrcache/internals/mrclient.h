#pragma once

#include <Python.h>
#include <stdbool.h>
#include "mrprotocol.h"

typedef struct {
  MrProtocol **conns;
  int conns_sz;
  int next_conn;
  int num_conns;
  int num;
  void *client;
} MrServer;

typedef struct {
  PyObject_HEAD
  MrServer *server; 
  PyObject *q;
  MrProtocol *conn;
  PyObject *b;
  PyObject *pause;
  PyObject *resume;
  //MrServer **servers; 
  //int num_servers;
} MrClient;

extern PyTypeObject MrClientType;

PyObject *MrClient_new    (PyTypeObject* self, PyObject *args, PyObject *kwargs);
int       MrClient_init   (MrClient* self,    PyObject *args, PyObject *kwargs);
void      MrClient_dealloc(MrClient* self);

PyObject *MrClient_cinit(MrClient* self);
PyObject *MrClient_pause_writing(MrClient* self);
PyObject *MrClient_resume_writing(MrClient* self);
//void MrClient_setupConnMap( MrClient* self ) ;

void MrClient_connection_lost( MrClient* self, MrProtocol* conn );
PyObject *MrClient_add_connection(MrClient* self, MrProtocol *conn );
//void MrClient_connection_lost( MrClient* self, MrProtocol* conn, int server_num );
//PyObject *MrClient_add_connection(MrClient* self, MrProtocol *conn, int server);

PyObject *MrClient_get(MrClient* self, PyObject *key);
PyObject *MrClient_set(MrClient* self, PyObject *args);
PyObject *MrClient_stat(MrClient* self, PyObject *args);
//int MrClient_get(MrClient* self, char *key, void *fn, void *connection );
//PyObject *MrClient_set(MrClient* self, PyObject *args);

int MrServer_init( MrServer *self );
//int MrServer_get( MrServer *self, char *k, void *fn, void *connection);
//int MrServer_set( MrServer *self, char *k, int ksz, char* d, int dsz );
int MrServer_add_connection( MrServer *self, MrProtocol *conn) ;
void MrServer_connection_lost( MrServer* self, MrProtocol* conn );
