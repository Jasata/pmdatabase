# pmdatabase
Scripts to create PATE Monitor's SQLite datafile. These are used only during installation or development time re-initialization.

*This script is cloned and executed by master installation script - aside from development use, this script has no other use than one-time database creation.*

Intended directory is `/srv/pmdatabase`. *Remember that the `/srv` directory itself has to be writable for accounts using the database file.*

    =============================================================================
    University of Turku, Department of Future Technologies
    ForeSail-1 / PATE Monitor database creation script
    Version 0.3.1, 2018 Jani Tammi <jasata@utu.fi>
    
    optional arguments:
      -h, --help                 show this help message and exit
      -l [LEVEL], --log [LEVEL]  Set logging level. Default: 'DEBUG'
      --force                    Delete existing database file and recreate.
      --dev                      Generate development content.
 
 
 NOTE: So much is still to be specified for PATE. This script will need tons of changes in the weeks to come.
