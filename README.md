databank-couchdb
================

This is a couchdb driver for Databank.

This driver is not ready for production use.  So far it has only been
tested with the driver test included with databank.  Consider this an
alpha release.


Usage
-----

To create a couchdb databank, use the `Databank.get()` method:

    var Databank = require('databank').Databank;
    var db = Databank.get('couchdb', { database: "hello-world" });

The driver takes the following parameters:

* `location`: the server url. Default is 'http://localhost:5984'.
* `database`: the database you want to connect to.


License
-------

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

