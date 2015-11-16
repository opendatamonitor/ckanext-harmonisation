ckanext-harmonisation
======================

A tool for harmonising metadata labels and values.

Tested with CKAN v2.2 (http://docs.ckan.org/en/ckan-2.2/).

General
--------
The ckanext-harmonisation plugin harmonises metadata labels and values that comply with the ODM metadata scheme.
The plugin uses the mongo DB as metadata repository and developed as part of the ODM project (www.opendatamonitor.eu).
In order to use it, the collected metadata, raw and harmonised, need to be stored in collections with names as **odm_harmonised** and **odm** respectively.

Implementation
---------------

The extension gives the ability to an end user to harmonise specific metadata fields (Dates,Resources,Licenses,Categories) via a web form. It also provides the ability to add new mappings or update existing ones. 

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

In order to execute the scheduled harmonisation jobs you need to execute the harmonisation_slave.py. This script can be found in the ckanext-harmonisation plugin under **controllers** folder. For instance, if you have installed the CKAN under the /var/local/ckan/default/pyenv virtualenv folder, you can add the following cron job to run it automatically:

    30 * * * * *    /usr/bin/flock -n /tmp/slave.lockfile /var/local/ckan/default/pyenv/bin/python /var/local/ckan/default/pyenv/src/ckanext-harmonisation/ckanext/harmonisation/controllers/harmonisation_slave.py > /tmp/cronslave.log 2>&1


Licence
---------

This work is licensed under the GNU Affero General Public License (AGPL) v3.0 (http://www.fsf.org/licensing/licenses/agpl-3.0.html).

