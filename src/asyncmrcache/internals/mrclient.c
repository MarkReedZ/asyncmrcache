
#include <Python.h>
#include <stdbool.h>

#include "mrclient.h"
#include "common.h"
#include "module.h"

static char buf[1025*1024]; // TODO dynamically size the buffer

static PyMethodDef MrClient_methods[] = {
  {"cinit", (PyCFunction)MrClient_cinit, METH_NOARGS,   ""},
  {"_get", (PyCFunction)MrClient_get, METH_O,   ""},
  {"_set", (PyCFunction)MrClient_set, METH_VARARGS,   ""},
  {"_stat", (PyCFunction)MrClient_stat, METH_VARARGS,   ""},
  {"pause_writing",  (PyCFunction)MrClient_pause_writing, METH_NOARGS,   ""},
  {"resume_writing", (PyCFunction)MrClient_resume_writing, METH_NOARGS,   ""},
  {NULL}
};

PyTypeObject MrClientType = {
  PyVarObject_HEAD_INIT(NULL, 0)
  "internals.MrClient",       /* tp_name */
  sizeof(MrClient),          /* tp_basicsize */
  0,                         /* tp_itemsize */
  (destructor)MrClient_dealloc, /* tp_dealloc */
  0,                         /* tp_print */
  0,                         /* tp_getattr */
  0,                         /* tp_setattr */
  0,                         /* tp_reserved */
  0,                         /* tp_repr */
  0,                         /* tp_as_number */
  0,                         /* tp_as_sequence */
  0,                         /* tp_as_mapping */
  0,                         /* tp_hash  */
  0,                         /* tp_call */
  0,                         /* tp_str */
  0,
  0,                         /* tp_setattro */
  0,                         /* tp_as_buffer */
  Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,        /* tp_flags */
  "MrClient",                /* tp_doc */
  0,                         /* tp_traverse */
  0,                         /* tp_clear */
  0,                         /* tp_richcompare */
  0,                         /* tp_weaklistoffset */
  0,                         /* tp_iter */
  0,                         /* tp_iternext */
  MrClient_methods,           /* tp_methods */
  0,                         /* tp_members */
  0,            /* tp_getset */
  0,                         /* tp_base */
  0,                         /* tp_dict */
  0,                         /* tp_descr_get */
  0,                         /* tp_descr_set */
  0,                         /* tp_dictoffset */
  (initproc)MrClient_init,    /* tp_init */
  0,                         /* tp_alloc */
  MrClient_new,              /* tp_new */
};

static int connmap[4096];

static char hexchar[256] = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
                             0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
                             0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
                             0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 0, 0, 0, 0, 0, 
                             0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
                             0,10,11,12,13,14,15, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
                             0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
                             0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
                             0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
                             0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
                             0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
                             0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
                             0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
                             0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
                             0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
                             0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };

PyObject *MrClient_new(PyTypeObject* type, PyObject *args, PyObject *kwargs) {
  MrClient* self = NULL;
  self = (MrClient*)type->tp_alloc(type, 0);
  return (PyObject*)self;
}

void MrClient_dealloc(MrClient* self) {
  free(self->server);
  Py_XDECREF(self->conn);
}

int MrClient_init(MrClient* self, PyObject *args, PyObject *kwargs) {

  self->server = malloc( sizeof(MrServer) );
  MrServer_init(self->server);

  if(!(self->pause      = PyObject_GetAttrString((PyObject*)self, "pause"))) return 1;
  if(!(self->resume     = PyObject_GetAttrString((PyObject*)self, "resume"))) return 1;

  //char tst[64] = "*2\r\n$3\r\nget\r\n$3\r\nkey\r\n";
  //self->b = PyBytes_FromStringAndSize( tst, 22 );
  char tst[64] = {0};
  tst[0] = 0;
  tst[1] = 1;
  tst[2] = 1;
  self->b = PyBytes_FromStringAndSize( tst, 10 );
  return 0;
}

static int paused = 0;
PyObject *MrClient_cinit(MrClient* self) {
  Py_RETURN_NONE;
}
PyObject *MrClient_pause_writing(MrClient* self) {
  paused = 1;
  PyObject_CallFunction(self->pause, NULL);
  Py_RETURN_NONE;
}
PyObject *MrClient_resume_writing(MrClient* self) {
  paused = 0;
  PyObject_CallFunction(self->resume, NULL);
  Py_RETURN_NONE;
}

PyObject *MrClient_add_connection(MrClient* self, MrProtocol *conn) { //, int server) { 
  Py_INCREF(conn); 
  DBG printf(" add conn server %p\n",self->server);
  MrServer_add_connection( self->server, conn );
  self->conn = conn;
  self->q = conn->respq;
  Py_RETURN_NONE;
}


// TODO https://docs.python.org/3/c-api/buffer.html  Use buffer to modify an existing bytes object instead of creating here

PyObject *MrClient_get(MrClient* self, PyObject *key) {

  PyObject *o;
  PyObject *bytes;

  // TODO Accept unicode or bytes and throw if not

  Py_ssize_t klen;
  char *kp;
  PyBytes_AsStringAndSize(key, &kp, &klen); 

  buf[0] = 0;
  buf[1] = 1;
  uint16_t *p = (uint16_t*)(buf+2);
  *p = klen;
  memcpy(buf+4, kp, klen);
  
  bytes = PyBytes_FromStringAndSize( buf, klen+4 );

  if(!(o = PyObject_CallFunctionObjArgs(self->conn->write, bytes, NULL))) return NULL;
  Py_DECREF(o);
  Py_DECREF(bytes);
  Py_INCREF(self->q);
  return self->q;

}

PyObject *MrClient_set(MrClient* self, PyObject *args) { //PyObject *key, PyObject *val) {

  if ( paused ) Py_RETURN_NONE;

  PyObject *bytes, *key, *val;

  buf[0] = 0;
  buf[1] = 2;
  uint16_t *klenp = (uint16_t*)(buf+2);
  uint32_t *vlenp = (uint32_t*)(buf+4);

  if (!PyArg_ParseTuple(args, "SS", &key, &val)) return NULL;

  Py_ssize_t klen;
  char *kp;
  PyBytes_AsStringAndSize(key, &kp, &klen);   // Can't copy directly in
  *klenp = (uint16_t)klen;

  Py_ssize_t vlen;
  char *vp;
  PyBytes_AsStringAndSize(val, &vp, &vlen);   // Can't copy directly in
  *vlenp = (uint32_t)vlen;

  memcpy(buf+8, kp, klen);
  memcpy(buf+8+klen, vp, vlen);

  bytes = PyBytes_FromStringAndSize( buf, 8+klen+vlen );

  PyObject *o;
  if(!(o = PyObject_CallFunctionObjArgs(self->conn->write, bytes, NULL))) return NULL;
  Py_DECREF(o);
  Py_DECREF(bytes);
  //Py_INCREF(self->q);
  //return self->q;
  Py_RETURN_NONE; // There is no response, why was I returning the Q
}

PyObject *MrClient_stat(MrClient* self, PyObject *args) { //PyObject *key, PyObject *val) {
  PyObject *o;
  PyObject *bytes;

  buf[0] = 0;
  buf[1] = 3;
  
  bytes = PyBytes_FromStringAndSize( buf, 2 );

  if(!(o = PyObject_CallFunctionObjArgs(self->conn->write, bytes, NULL))) return NULL;
  Py_DECREF(o);
  Py_DECREF(bytes);
  Py_RETURN_NONE; // There is no response, why was I returning the Q
}

PyObject *MrClient_set64(MrClient* self, PyObject *args) { //PyObject *key, PyObject *val) {

  PyObject *bytes, *val;

  buf[0] = 0;
  buf[1] = 2;
  unsigned long *lp = (unsigned long*)(buf+2);
  unsigned int *ip = (unsigned int*)(buf+10);
  if (!PyArg_ParseTuple(args, "kS", lp, &val)) return NULL;

  //PyObject_Print( val, stdout, 0 ); printf("\n");

  Py_ssize_t vlen;
  char *p;
  PyBytes_AsStringAndSize(val, &p, &vlen);   // Can't copy directly in
  *ip = (unsigned int)vlen;

  memcpy(buf+14, p, vlen);

  //printf("A3\n");
  //*lp = PyLong_AsUnsignedLong(key);
  //printf("A4\n");
  //*ip = (unsigned int)PyLong_AsLong(val);
  bytes = PyBytes_FromStringAndSize( buf, 14+vlen );
  //printf("Sent: "); PyObject_Print( bytes, stdout, 0 ); printf("\n");

  PyObject *o;
  if(!(o = PyObject_CallFunctionObjArgs(self->conn->write, bytes, NULL))) return NULL;
  Py_DECREF(o);
  Py_DECREF(bytes);
  Py_INCREF(self->q);
  return self->q;
}


/*
PyObject *MrClient_get(MrClient* self, PyObject *key) {

  PyObject *o;

  PyObject *bytes;

  buf[0] = 0;
  buf[1] = 1;
  unsigned long *lp = buf+2;
  *lp = PyLong_AsUnsignedLong(key);
  bytes = PyBytes_FromStringAndSize( buf, 10 );
  //printf("Sent: "); PyObject_Print( bytes, stdout, 0 ); printf("\n");

  if(!(o = PyObject_CallFunctionObjArgs(self->conn->write, bytes, NULL))) return NULL;
  //Py_DECREF(o);
  //Py_DECREF(bytes);
  Py_INCREF(self->q);
  return self->q;
}

PyObject *MrClient_set(MrClient* self, PyObject *args) { //PyObject *key, PyObject *val) {

  PyObject *bytes, *val;

  buf[0] = 0;
  buf[1] = 2;
  unsigned long *lp = (unsigned long*)(buf+2);
  unsigned int *ip = (unsigned int*)(buf+10);
  if (!PyArg_ParseTuple(args, "kS", lp, &val)) return NULL;

  //PyObject_Print( val, stdout, 0 ); printf("\n");

  Py_ssize_t vlen;
  char *p;
  PyBytes_AsStringAndSize(val, &p, &vlen);   // Can't copy directly in
  *ip = (unsigned int)vlen;

  memcpy(buf+14, p, vlen);

  //printf("A3\n");
  // *lp = PyLong_AsUnsignedLong(key);
  //printf("A4\n");
  // *ip = (unsigned int)PyLong_AsLong(val);
  bytes = PyBytes_FromStringAndSize( buf, 14+vlen );
  //printf("Sent: "); PyObject_Print( bytes, stdout, 0 ); printf("\n");

  PyObject *o;
  if(!(o = PyObject_CallFunctionObjArgs(self->conn->write, bytes, NULL))) return NULL;
  //Py_DECREF(o);
  //Py_DECREF(bytes);
  Py_INCREF(self->q);
  return self->q;
}
*/

/*


// Args Session key bytes, user bytes
int MrClient_get(MrClient* self, char *key, void *fn, void *connection ) {

  int ksz = 32;
  int hash = (hexchar[(uint8_t)key[ksz-3]]<<8) | (hexchar[(uint8_t)key[ksz-2]]<<4) | hexchar[(uint8_t)key[ksz-1]];
  int server = connmap[hash];
  DBG printf("  memcached get server %d\n",server); 
  if ( server == -1 ) return -1;

  int rc = MrServer_get( self->servers[server], key, fn, connection );
  return rc;
}

// Args Session key bytes, user bytes
PyObject *MrClient_set(MrClient* self, PyObject *args) {

  PyObject *pykey, *pydata;
  if(!PyArg_ParseTuple(args, "OO", &pykey, &pydata)) return NULL;

  Py_ssize_t ksz;
  char *k = PyUnicode_AsUTF8AndSize( pykey, &ksz ); 
  Py_ssize_t dsz;
  char *d = PyUnicode_AsUTF8AndSize( pydata, &dsz ); 

  int hash = (hexchar[(uint8_t)k[ksz-3]]<<8) | (hexchar[(uint8_t)k[ksz-2]]<<4) | hexchar[(uint8_t)k[ksz-1]];
  int server = connmap[hash];
  if ( server == -1 ) return NULL;

  DBG printf("  memcached set server %d\n",server); 
  int rc = MrServer_set( self->servers[server], k, ksz, d, dsz );
  // TODO We have to set an exception here
  if ( rc != 0 ) {
    return NULL;
  }
  Py_RETURN_NONE;
}
*/

void MrClient_connection_lost( MrClient* self, MrProtocol* conn ) {
  //DBG printf("conn %p lost server %d\n", conn, server);
  //MrServer_connection_lost( self->servers[server], conn );
  MrServer_connection_lost( self->server, conn );

  PyObject* func = PyObject_GetAttrString((PyObject*)self, "lost_connection");
  PyObject* tmp = PyObject_CallFunctionObjArgs(func, NULL);
  //PyObject* tmp = PyObject_CallFunctionObjArgs(func, PyLong_FromLong(server), NULL);
  Py_XDECREF(func);
  Py_XDECREF(tmp);

  // If we have no more connections to this server remove it 
  //if ( self->servers[server]->num_conns == 0 ) {
    //MrClient_setupConnMap(self);
  //}
}

int MrServer_dealloc( MrServer *self ) {
  free(self->conns);
  return 0;
}

int MrServer_init( MrServer *self ) { //, int server_num ) {
  self->num_conns = 0;
  self->next_conn = 0;
  //self->client = client;
  //self->num = server_num;
  self->conns = malloc( sizeof(MrProtocol*) * 160 );
  return 0;
}
int MrServer_add_connection( MrServer *self, MrProtocol *conn) {
  //printf("  MrqServer add conn %p\n", conn);
  self->conns[ self->num_conns++ ] = conn;
  return 0;
}
void MrServer_connection_lost( MrServer* self, MrProtocol* conn ) {
  //printf("conn %p lost\n", conn);
  self->num_conns--;
  self->next_conn = 0;
  if ( self->num_conns == 0 ) {
    DBG printf("  No more memcached connections\n");
    return;
  }

  // Remove the connection by shifting the rest left    
  MrProtocol **p = self->conns;
  for (int i = 0; i < self->num_conns+1; i++) {
    p[i] = self->conns[i];
    if ( self->conns[i] != conn ) p++;
  }
  
}
/*
int MrServer_set( MrServer *self, char *k, int ksz, char* d, int dsz ) {
  if ( self->num_conns == 0 ) return -1;
  int c = self->next_conn++;
  if ( self->next_conn == self->num_conns ) self->next_conn = 0;
  return MrProtocol_asyncSet( self->conns[c], k, d, dsz );
}
int MrServer_get( MrServer *self, char *k, void *fn, void *connection ) {
  if ( self->num_conns == 0 ) return -1;
  int c = self->next_conn++;
  if ( self->next_conn == self->num_conns ) self->next_conn = 0;
  MrProtocol_asyncGet( self->conns[c], k, fn, connection );
  return 0;
}
*/









