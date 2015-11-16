ckanext-harmonisation
======================

A tool for harmonising metadata labels and values.

Tested with CKAN v2.2 (http://docs.ckan.org/en/ckan-2.2/).

General
--------
The ckanext-harmonisation plugin harmonises metadata labels and values that comply with the ODM metadata scheme.
The plugin uses the mongo DB as metadata repository and developed as part of the ODM project (www.opendatamonitor.eu).
In order to use it you need to have the metadata (ODM scheme) stored to a collection named odm_harmonised and the raw metadata in a collection named odm.

Implementation
---------------

The extension gives the ability to an ending user to harmonise specific metadata fields (Dates,Resources,Licenses,Categories) threw a web form. It also gives the ability to add new mappings. 

Building
---------

To build and use this plugin, simply:

    git clone https://github.com/opendatamonitor/ckanext-harmonisation.git
    cd ckanext-harmonisation
    pip install -r pip-requirements.txt
    python setup.py develop

Then you will need to update your CKAN configuration to include the new harvester.  This will mean adding the
ckanext-genericapiharvest plugin as a plugin.  E.g.

    ckan.plugins = harmonisation

Using
---------

After setting this up, you should be able to go to:
    http://localhost:5000/harmonisation

In order to run  harmonisation jobs automatically you need to add a cron job that will execute the command:

    python /var/local/ckan/default/pyenv/src/ckanext-harmonisation/ckanext/harmonisation/controllers/harmonisation_slave.py 


Licence
---------

This work is licensed under the GNU Affero General Public License (AGPL) v3.0 (http://www.fsf.org/licensing/licenses/agpl-3.0.html).

