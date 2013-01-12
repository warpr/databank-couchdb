/*

This file is part of databank-couchdb, a CouchDB driver for databank.
Copyright 2013 Kuno Woudt <kuno@frob.nl>

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License.  You may
obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied.  See the License for the specific language governing
permissions and limitations under the License.

*/

var _ = require('underscore');
var step = require('step');
var couchdb = require('couchdb-api');
var databank = require('databank');

var CouchDBDatabank = function(params) {
    this.schema    = params.schema || {};
    this.location  = params.location || "http://localhost:5984";
    this.username  = params.username || null;
    this.password  = params.password || null;
    this.database  = params.database || null;
    this.clear_database = params._clear_database_for_test_run || false;
    this.connection = null;
};

var SEPARATOR = ':';
var document_key = function (type, id) { return type + SEPARATOR + id; };

/* When running tests, clear the database first, but only do so once. */
var TEST_DATABASE_CLEARED = false;

CouchDBDatabank.prototype = new databank.Databank();

CouchDBDatabank.prototype._error_messages = {
    "clear_test_database": function (location, dbname) {
        return "CouchDB cannot create the test database " + dbname + " at " + location;
    },
    "connect": function (location, dbname) {
        return "CouchDB cannot connect to database " + dbname + " at " + location;
    },
    "delete": function (type, id) {
        return "CouchDB error deleting document " + document_key (type, id);
    },
    "read": function (type, id) {
        return "CouchDB error while reading document " + document_key (type, id);
    },
    "read_document": function (document_id) {
        return "CouchDB error while reading document " + document_id;
    },
    "readAll": function (type, ids) {
        return "CouchDB error while reading all " + type + "documents";
    },
    "save_document": function (document_id) {
        return "CouchDB error while saving document " + document_id;
    },
    "search": function (type, criteria) {
        return "CouchDB error while searching for a " + type;
    }
};

CouchDBDatabank.prototype._error = function (err, verb) {
    if (err instanceof databank.DatabankError)
        return err;

    var self = this;
    var context = Array.prototype.slice.call (arguments, 2);
    var msg = self._error_messages[verb].apply (null, context) +
        ": " + JSON.stringify (err);

    return new databank.DatabankError (msg);
};

CouchDBDatabank.prototype._connected = function(onCompletion) {
    if (!this.connection) {
        onCompletion(new databank.NotConnectedError(), null);
        return false;
    }

    return true;
};

CouchDBDatabank.prototype._new_document = function (type, id) {
    return this.connection.doc (document_key (type, id));
};

CouchDBDatabank.prototype._read_document = function(type, id, onCompletion) {
    var self = this;
    if (!self._connected (onCompletion))
        return;

    var document = self._new_document (type, id);
    document.get (function (err, response) {
        if (err && err.error === 'not_found')
        {
            onCompletion (new databank.NoSuchThingError(type, id), null);
        }
        else if (err)
        {
            onCompletion (self._error (err, "read_document", document));
        }
        else
        {
            onCompletion (null, document);
        }
    });
};

CouchDBDatabank.prototype._save_document = function (document, value, onCompletion) {
    var self = this;

    document.body.data = value;
    document.save (function (err, response) {
        if (err && err.error === 'conflict')
        {
            onCompletion(new databank.AlreadyExistsError ());
        }
        else if (err)
        {
            onCompletion(self._error (err, "save_document", document.id))
        }
        else
        {
            onCompletion(null, value);
        }
    });
};

CouchDBDatabank.prototype._clear_test_database = function (params, onCompletion) {
    var self = this;
    if (self.clear_database && !TEST_DATABASE_CLEARED)
    {
        step(
            function () {
                self.connection.drop (this);
            },
            function (err) {
                TEST_DATABASE_CLEARED = true;
                if (!err || (err && err.error === 'not_found'))
                {
                    self.connection.create (this);
                }
                else
                {
                    this(err);
                }
            },
            function () {
                onCompletion ();
            }
        );
    }
    else
    {
        onCompletion ();
    }
};

CouchDBDatabank.prototype.connect = function(params, onCompletion) {
    var self = this;

    if (self.connection) {
        onCompletion(new databank.AlreadyConnectedError(), null);
        return;
    }

    var server = couchdb.srv(self.location);
    if (self.username || self.password)
    {
        server.auth = [ self.username, self.password ];
    }

    self.connection = server.db(self.database);

    var verify_connection = function () {
        self.connection.info (function (err, response) {
            err ? onCompletion(self._error (err, "connect", self.location, self.database))
                : onCompletion(null);
        });
    };

    self._clear_test_database (params, function (err) {
        err ? onCompletion(self._error (err, "clear_test_database", self.location, self.database))
            : verify_connection();
    });
};

CouchDBDatabank.prototype.disconnect = function(onCompletion) {
    var self = this;
    if (!self._connected (onCompletion))
        return;

    self.connection = null;
    onCompletion (null);
};

CouchDBDatabank.prototype.create = function(type, id, value, onCompletion) {
    var self = this;
    if (!self._connected (onCompletion))
        return;

    // FIXME: onCompletion(new AlreadyExistsError(type, id), null);
    var document = self._new_document (type, id);
    self._save_document (document, value, onCompletion);
};

CouchDBDatabank.prototype.read = function(type, id, onCompletion) {
    var self = this;
    self._read_document (type, id, function (err, document) {
        err ? onCompletion (self._error (err, "read", type, id))
            : onCompletion (null, document.body.data);
    });
};

CouchDBDatabank.prototype.update = function(type, id, value, onCompletion) {
    var self = this;
    if (!self._connected (onCompletion))
        return;

    self._read_document (type, id, function (err, document) {
        err ? onCompletion (err)
            : self._save_document (document, value, onCompletion);
    });
};

CouchDBDatabank.prototype.save = function(type, id, value, onCompletion) {
    var self = this;
    if (!self._connected (onCompletion))
        return;

    // FIXME: verify that err is a NoSuchThingError.
    self._read_document (type, id, function (err, document) {
        err ? self.create (type, id, value, onCompletion)
            : self.update (type, id, value, onCompletion);
    });
};

CouchDBDatabank.prototype.del = function(type, id, onCompletion) {
    var self = this;
    if (!self._connected (onCompletion))
        return;

    self._read_document (type, id, function (err, document) {
        if (!document) {
            onCompletion (err)
        }
        else
        {
            document.del (function (err, response) {
                err ? onCompletion (self._error (err, "delete", type, id))
                    : onCompletion(null);
            });
        }
    });
};

CouchDBDatabank.prototype.search = function(type, criteria, onResult, onCompletion) {
    var self = this;
    if (!self._connected (onCompletion))
        return;

    var keys = _(criteria).keys ();

    var map = [
        "function (doc) {",
        "    var isArray = function(o) { return (o instanceof Array || typeof o == \"array\"); };",
        "    var getDottedProperty = null;",
        "    getDottedProperty = function (doc, name) {",
        "        if (!isArray (name)) name = name.split (\".\");",
        "        var key = name.shift ();",
        "        if (!key) return doc;",
        "        if (!doc.hasOwnProperty (key)) return undefined;",
        "        return getDottedProperty (doc[key], name);",
        "    };",
        "    var type = " + JSON.stringify (type + SEPARATOR) + ";",
        "    if (doc._id.substr (0, type.length) !== type) return;",
        "    var criteria = " + JSON.stringify (keys) + ";",
        "    var results = [];",
        "    for (var i in criteria) {",
        "        var value = getDottedProperty(doc.data, criteria[i]);",
        "        if (value === undefined) return;",
        "        results.push (value);",
        "    }",
        "    emit (results, doc);",
        "}"
    ];

    var query = { "key": _(keys).map (function (key) { return criteria[key] }) };

    // FIXME: tempViews are slow.  automatically create permanent view
    // after N requests for the same view?

    self.connection.tempView (map.join ("\n"), null, query, function (err, response) {
        if (err)
        {
            onCompletion(self._error (err, "search", type, criteria));
        }

        response.rows.forEach (function (doc) { onResult(doc.value.data); });

        onCompletion(null);
    });
};

CouchDBDatabank.prototype.readAll = function(type, ids, onCompletion) {
    var self = this;
    if (!self._connected (onCompletion))
        return;

    var id_map = {};
    ids.forEach (function (id) {
        var docid = document_key (type, id);
        id_map[docid] = id;
    });

    var options = { include_docs: true };
    var docids = _(id_map).keys ();

    self.connection.allDocs (options, docids, function (err, response) {
        if (err) {
            onCompletion (self._error (err, "readAll", type, ids));
        }
        else
        {
            var results = {};
            response.rows.forEach (function (row) {
                results[ id_map[row.key] ] = row.doc ? row.doc.data : null;
            });
            onCompletion(null, results);
        }
    });
};

module.exports = CouchDBDatabank;
