

#include <Python.h>
#include "structmember.h"
#include <stdbool.h>

#include "mrserver.h"
#include "common.h"
#include "module.h"
#include "mrclient.h"
//#include "plist.h"


static char buf[1025*1024]; // TODO dynamically size the buffer

static PyMethodDef MrServer_methods[] = {
  //{"_get", (PyCFunction)MrServer_get, METH_O,   ""},
  //{"_set", (PyCFunction)MrServer_set, METH_VARARGS,   ""},
  //{"_stat", (PyCFunction)MrServer_stat, METH_VARARGS,   ""},
  {"pause_writing",  (PyCFunction)MrServer_pause_writing, METH_NOARGS,   ""},
  {"resume_writing", (PyCFunction)MrServer_resume_writing, METH_NOARGS,   ""},
  {NULL}
};

static PyMemberDef MrServer_members[] = {
    {"reconnecting", T_BOOL, offsetof(MrServer, reconnecting), 0, "MrServer reconnecting"},
    {NULL}
};


PyTypeObject MrServerType = {
  PyVarObject_HEAD_INIT(NULL, 0)
  "internals.MrServer",       /* tp_name */
  sizeof(MrServer),          /* tp_basicsize */
  0,                         /* tp_itemsize */
  (destructor)MrServer_dealloc, /* tp_dealloc */
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
  "MrServer",                /* tp_doc */
  0,                         /* tp_traverse */
  0,                         /* tp_clear */
  0,                         /* tp_richcompare */
  0,                         /* tp_weaklistoffset */
  0,                         /* tp_iter */
  0,                         /* tp_iternext */
  MrServer_methods,          /* tp_methods */
  MrServer_members,          /* tp_members */
  0,            /* tp_getset */
  0,                         /* tp_base */
  0,                         /* tp_dict */
  0,                         /* tp_descr_get */
  0,                         /* tp_descr_set */
  0,                         /* tp_dictoffset */
  (initproc)MrServer_init,    /* tp_init */
  0,                         /* tp_alloc */
  MrServer_new,              /* tp_new */
};

static int connmap[4096];

PyObject *MrServer_new(PyTypeObject* type, PyObject *args, PyObject *kwargs) {
  MrServer* self = NULL;
  self = (MrServer*)type->tp_alloc(type, 0);
  return (PyObject*)self;
}

void MrServer_dealloc(MrServer* self) {
  DBG printf("dealloc called\n");
  //free(self->server);
  //Py_XDECREF(self->conn);
}

int MrServer_init(MrServer* self, PyObject *args, PyObject *kwargs) {

  DBG printf("MrServer init\n");
  //if(!(self->pause      = PyObject_GetAttrString((PyObject*)self, "pause"))) return 1;
  //if(!(self->resume     = PyObject_GetAttrString((PyObject*)self, "resume"))) return 1;
  //self->num_conns = 0;
  //self->conns = malloc( sizeof(MrProtocol*) * 160 );
  return 0;
}

void MrServer_cinit(MrServer* self, MrClient *client) {

  self->client = client;
  self->closed = false;
  self->num_conns = 0;
  self->next_conn = 0;

  self->py_conns = PyObject_GetAttrString((PyObject*)self, "conns");
  if (self->py_conns == NULL)  { printf("conns null\n"); exit(1); } // DELME

}
PyObject *MrServer_pause_writing(MrServer* self) {
  //PyObject_CallFunction(self->pause, NULL);
  Py_RETURN_NONE;
}
PyObject *MrServer_resume_writing(MrServer* self) {
  //PyObject_CallFunction(self->resume, NULL);
  Py_RETURN_NONE;
}

int MrServer_add_connection( MrServer *self, MrProtocol *conn) {
  DBG printf("MrServer add conn %p num %d\n", conn, self->num_conns);

  PyObject *qs = PyObject_GetAttrString((PyObject*)self, "queues");
  conn->respq = PyList_GET_ITEM( qs, self->num_conns++ );
  self->conn = conn;

  return 0;
}

void MrServer_connection_lost( MrServer* self, MrProtocol* conn ) {
  DBG printf("MrServer conn %p lost\n", conn);
  // This will kick off the reconnection in server.py
  PyObject *func = PyObject_GetAttrString((PyObject*)self, "lost_connection");
  PyObject_CallFunction(func, NULL);
  //PyObject_CallFunctionObjArgs(func, (PyObject*)conn, NULL);

  // Tell the client we're down
  self->num_conns -= 1;
  MrClient_server_lost( self->client );

/*j
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
 */ 
}

void MrServer_next_connection( MrServer *self ) {
  //if ( self->closed ) return 0;
  self->next_conn = (self->next_conn+1) % PyList_GET_SIZE(self->py_conns);
  self->conn = (MrProtocol*)PyList_GET_ITEM(self->py_conns, self->next_conn);
  //return 1;
}












