# pmdatabase
Scripts to create PATE Monitor's SQLite datafile. These are used only during installation or development time re-initialization.

*This script is cloned and executed by master installation script - aside from development use, this script has no other use than one-time database creation.*

**IMPORTANT!** Intended directory is `/srv/pmdatabase`. *Remember that the `/srv` directory itself has to be writable for accounts using the database file.*

    usage: setup.py [-h] [-l LEVEL] [--force] [-m MODE]
    
    =============================================================================
    University of Turku, Department of Future Technologies
    ForeSail-1 / PATE Monitor database creation script
    Version 0.4.1, 2019 Jani Tammi <jasata@utu.fi>
    
    optional arguments:
      -h, --help            show this help message and exit
      -l LEVEL, --log LEVEL
                            Set logging level. Default: 'DEBUG'
      --force               Delete existing database file and recreate.
      -m MODE, --mode MODE  Instance mode (DEV|UAT|PRD). Default: 'DEV'
  
 
## Write-Ahead Logging Mode
In WAL mode, SQLite3 creates writelogs into separate files. These will always be created under the ownership of the CRUD DML issuer. What permission mask is used, is unclear at this time.

For multi-user solution, this imposes a difficult issue where the main strengths of WAL mode are lost into journal file accessability issues. This is most acute in the planned architecture which was hoping to avoid occasional "long commits" (automatic checkpointing) by adopting the SQLite3 documentation proposed model of creating a maintenance daemon which keeps logs small and makes the checkpoint writes without performance impact issues.

Imagine `psud` writing updates to `psud` table twice a second, but leaving behind the write-ahead log files, diabling Flask based Web UI from writing `command` rows until the maintentnace daemon has written the logs into the main database file. *SQLite3 WAL mode allows it, but the filesystem will not.*

**The solution** might be found in a way to tell the maintenance process to leave the log files and not to remove them. This could allow us to set their ownership and permissions to support access by multiple users. **This needs to be studied!**
