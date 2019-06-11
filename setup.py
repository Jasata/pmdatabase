#! /usr/bin/env python3
# -*- coding: utf-8 -*-
#
# PATE Monitor / Development Utility 2018
# Post-git-clone script
# Create PATE Monitor SQLite database file.
#
# setup.py - Jani Tammi <jasata@utu.fi>
#   0.1.0   2018.10.23  Initial version.
#   0.2.0   2018.10.30  Corrected hitcount table columns.
#   0.2.1   2018.11.11  hitcount.rotation -> hitcount.timestamp
#   0.2.2   2018.11.11  housekeeping.timestamp datetime -> integer.
#   0.3.0   2018.11.26  Modified and renamed as 'setup.py'.
#   0.3.1   2018.11.27  Slight output/print changes.
#   0.4.0   2019.01.24  Column psu.state removed.
#
import os
import getpass
import sqlite3
import pathlib
import logging
import argparse
import subprocess

# PEP 396 -- Module Version Numbers https://www.python.org/dev/peps/pep-0396/
__version__ = "0.3.1"
__author__  = "Jani Tammi <jasata@utu.fi>"
VERSION = __version__
HEADER  = """
=============================================================================
University of Turku, Department of Future Technologies
ForeSail-1 / PATE Monitor database creation script
Version {}, 2018 {}
""".format(__version__, __author__)

class Config:
    dbfile          = "/srv/patemon.sqlite3"
    dbfile_owner    = "patemon.patemon"
    dbdir_owner     = "patemon.www-data"
    logging_level   = "DEBUG"


def do_or_die(cmd: list):
    prc = subprocess.run(cmd.split(" "))
    if prc.returncode:
        print("Command '{}' failed!".format(cmd))
        os._exit(-1)

def check_user_and_group(usr_grp : str):
    """Will fail unless the provided value has a dot separator"""
    import pwd
    import grp
    (user, group) = usr_grp.split(".")
    try:
        pwd.getpwnam(user)
    except KeyError as e:
        raise ValueError("User '{}' does not exist!".format(user)) from e
    try:
        grp.getgrnam(group)
    except KeyError:
        raise ValueError("Group '{}' does not exist!".format(group)) from e


if __name__ == '__main__':

    #
    # MUST be executed as 'patemon'!!
    # getpass.getuser() seems to get effective user - good!
    # if getpass.getuser() != "patemon":
    #     print("This script MUST be executed as 'patemon' user!")
    #     os._exit(-1)
    # CHANGED MY MIND - ROOT REQUIRED NOW :-)
    if os.geteuid() != 0:
        print("This script MUST be executed as 'root'!")
        os._exit(-1)


    #
    # Commandline arguments
    #
    parser = argparse.ArgumentParser(
        description     = HEADER,
        formatter_class = argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        '-l',
        '--log',
        help    = "Set logging level. Default: '{}'".format(Config.logging_level),
        choices = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        nargs   = '?',
        dest    = "logging_level",
        const   = "INFO",
        default = Config.logging_level,
        type    = str.upper,
        metavar = "LEVEL"
    )
    parser.add_argument(
        '--force',
        help    = 'Delete existing database file and recreate.',
        action  = 'store_true'
    )
    parser.add_argument(
        '--dev',
        help    = 'Generate development content.',
        action  = 'store_true'
    )
    args = parser.parse_args()
    Config.logging_level = getattr(logging, args.logging_level)


    #
    # Set up logging
    #
    logging.basicConfig(
        level       = Config.logging_level,
        filename    = "setup.log",
        format      = "%(asctime)s.%(msecs)03d %(levelname)s: %(message)s",
        datefmt     = "%H:%M:%S"
    )
    log = logging.getLogger()


    #
    # Check for pre-existing database file
    #
    print(
        "Checking existing database file '{}'...".format(Config.dbfile),
        end="",
        flush=True
    )
    if os.path.exists(Config.dbfile):
        if args.force:
            try:
                os.remove(Config.dbfile)
            except:
                print("Previous database file exists and could not be removed!")
                os._exit(-1)
        else:
            print("Database file already exists! (use '--force' to remove)")
            os._exit(-1)
    print("OK!")

    #
    # Create the file explicitly to test that the directory is writable
    #
    print("Creating new database file...", end="", flush=True)
    try:
        pathlib.Path(Config.dbfile).touch(mode=0o770, exist_ok=False)
    except FileExistsError as e:
        print("Old database file was not successfully removed!")
        os._exit(-1)
    print("OK!")


    #
    # Start actual database creation
    #
    print("Connecting...", end="", flush=True)
    connection = sqlite3.connect(Config.dbfile)
    connection.execute('PRAGMA journal_mode=wal')
    connection.execute("PRAGMA foreign_keys = 1")
    print("OK!")

    print("Creating new tables...")
    try:
        #
        #   pate
        #
        #       PATE instruments shall be identified via (specified) ADC channel
        #       that has a unique resistor, giving the unit a unique reading on
        #       that channel. Columns id_min and id_max define the range in
        #       which the value needs to be, in order for the unit to be
        #       identified as the one defined by the row.
        #
        sql = """
        CREATE TABLE pate
        (
            id          INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            id_min      INTEGER NOT NULL,
            id_max      INTEGER NOT NULL,
            label       TEXT NOT NULL
        )
        """
        connection.execute(sql)
        print("Table 'pate' created")


        #
        # testing_session
        #
        #       PATE firmware may change between sessions. It shall be queried
        #       from the instrument and recorded into the testing session.
        #
        sql = """
        CREATE TABLE testing_session
        (
            id              INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            started         DATETIME,
            pate_id         INTEGER NOT NULL,
            pate_firmware   TEXT NOT NULL,
            FOREIGN KEY (pate_id) REFERENCES pate (id)
        )
        """
        connection.execute(sql)
        print("Table 'testing_session' created")


        #
        # hitcount
        #
        #       Science data (energy-classified particle hits) is collected in
        #       units of "rotations", as the satellite rorates over its axis.
        #       Each rotation is divided into 10 degree (36) sectors and each
        #       has the same collection of hit counts (12 + 8). In addition,
        #       there is "37th sector", which is in fact, the sun-pointing
        #       telescope.
        #
        #       Each sector has;
        #           10  Primary Proton energy classes (channels)
        #            7  Primary Electron energy classes
        #            2  Secondary Proton energy classes
        #            1  Secondary Electron energy class
        #
        #       Sector naming; sc[00..36], where sector zero is sun-pointing.
        #
        #       Both telescopes also collect other hit counters;
        #
        #            2  AC classes
        #            4  D1 classes
        #            1  D2 class
        #            2  trash classes
        #
        #       Telescopes
        #           st = Sun-pointing Telescope
        #           rt = Rotating Telescope
        #
        #       Design decision has been made to lay all these in a flat table,
        #       even though this generates more than a thousand columns.
        #
        #       Each row is identified by datetime value (named 'rotation')
        #       which designates the beginning of the measurement rotation.
        #       The start of each sector measurement is calculated based on
        #       'rotation' timestamp and the rotation interval.
        #
        #       Sector zero (0) is the sun-pointing telescope, other indeces are
        #       naturally ordered with the rotational direction. (index 1 is
        #       measured first and index 36 last).
        #
        #       NOTE: Default limit for number of columns in SQLite is 2000
        #
        sql = """
        CREATE TABLE hitcount
        (
            timestamp       INTEGER NOT NULL DEFAULT CURRENT_TIME PRIMARY KEY,
            session_id      INTEGER NOT NULL,
        """
        cols = []
        # Sector specific counters
        for sector in range(0,37):
            for proton in range(1,13):
                cols.append("s{:02}p{:02} INTEGER NOT NULL, ".format(sector, proton))
            for electron in range(1,9):
                cols.append("s{:02}e{:02} INTEGER NOT NULL, ".format(sector, electron))
        # Telescope specfic counters
        for telescope in ('st', 'rt'):
            for ac in range(1, 3):
                cols.append("{}ac{} INTEGER NOT NULL, ".format(telescope, ac))
            # D1 hit patterns
            for d1 in range(1,5):
                cols.append("{}d1p{:01} INTEGER NOT NULL, ".format(telescope, d1))
            # D2 hit pattern
            cols.append("{}d2p1 INTEGER NOT NULL, ".format(telescope))
            for trash in range(1,3):
                cols.append("{}trash{:01} INTEGER NOT NULL, ".format(telescope, trash))
        sql += "".join(cols)
        sql += " FOREIGN KEY (session_id) REFERENCES testing_session (id) )"
        connection.execute(sql)
        print("Table 'hitcount' created")


        #
        # pulseheight
        #
        #       Calibration data is raw hit detection data from detector disks,
        #       containing ADC values that indicate the pulse heights.
        #
        #       Sample data contained an 8-bit hit mask. DOES THIS EXIST IN THE
        #       ACTUAL CALIBRATION DATA?
        #
        sql = """
        CREATE TABLE pulseheight
        (
            timestamp       INTEGER NOT NULL DEFAULT CURRENT_TIME PRIMARY KEY,
            session_id      INTEGER NOT NULL,
            ac1             INTEGER NOT NULL,
            d1a             INTEGER NOT NULL,
            d1b             INTEGER NOT NULL,
            d1c             INTEGER NOT NULL,
            d2a             INTEGER NOT NULL,
            d2b             INTEGER NOT NULL,
            d3              INTEGER NOT NULL,
            ac2             INTEGER NOT NULL,
            FOREIGN KEY (session_id) REFERENCES testing_session (id)
        )
        """
        connection.execute(sql)
        print("Table 'pulseheight' created")


        #
        # register
        #
        #       PATE Registers. Assumably, this table will get populated when
        #       a testing session begins, allowing UI to display these values
        #       without issuing (high-delay) commands to PATE for reading the
        #       values.
        #
        #       NOTE: Just a placeholder for now...
        #
        sql = """
        CREATE TABLE register
        (
            pate_id         INTEGER NOT NULL,
            retrieved       DATETIME NOT NULL,
            reg01           INTEGER NOT NULL,
            reg02           INTEGER NOT NULL,
            FOREIGN KEY (pate_id) REFERENCES pate (id)
        )
        """
        connection.execute(sql)
        print("Table 'register' created")


        #
        # note
        #
        #       Store operator issued notes during a testing session.
        #       (remove for mission-time EGSE)
        #
        sql = """
        CREATE TABLE note
        (
            id              INTEGER     NOT NULL PRIMARY KEY AUTOINCREMENT,
            session_id      INTEGER     NOT NULL,
            text            TEXT            NULL,
            created         INTEGER     NOT NULL DEFAULT (strftime('%s', 'now')),
            FOREIGN KEY (session_id) REFERENCES testing_session (id)
        )
        """
        connection.execute(sql)
        print("Table 'note' created")

        sql = """ -- make id into a timestamp with ms accuracy
        CREATE TABLE note2
        (
            id              INTEGER     NOT NULL PRIMARY KEY AUTOINCREMENT,
            session_id      INTEGER     NOT NULL,
            text            TEXT            NULL,
            created         INTEGER     NOT NULL DEFAULT (strftime('%s', 'now')),
            FOREIGN KEY (session_id) REFERENCES testing_session (id)
        )
        """



        #
        # command
        #
        sql = """
        CREATE TABLE command
        (
            id              INTEGER         NOT NULL PRIMARY KEY AUTOINCREMENT,
            session_id      INTEGER         NOT NULL,
            interface       TEXT            NOT NULL,
            command         TEXT            NOT NULL,
            value           TEXT            NOT NULL,
            created         TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
            handled         DATETIME            NULL,
            result          TEXT                NULL,
            FOREIGN KEY (session_id) REFERENCES testing_session (id)
        )
        """
        connection.execute(sql)
        print("Table 'command' created")


        #
        # PSU (this table is supposed to have only zero or one rows)
        #
        sql = """
        CREATE TABLE psu
        (
            id                  INTEGER         NOT NULL DEFAULT 0 PRIMARY KEY,
            power               TEXT            NOT NULL,
            voltage_setting     REAL            NOT NULL,
            current_limit       REAL            NOT NULL,
            measured_current    REAL            NOT NULL,
            measured_voltage    REAL            NOT NULL,
            modified            INTEGER         NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT          single_row_chk  CHECK (id = 0),
            CONSTRAINT          power_chk       CHECK (power IN ('ON', 'OFF'))
        )
        """
        connection.execute(sql)
        print("Table 'psu' created")
        # SQLite doesn't have "CREATE OR REPLACE"
        trg = """
        CREATE TRIGGER psu_ari
        AFTER UPDATE ON psu
        FOR EACH ROW
        BEGIN
            UPDATE psu
            SET    modified = CURRENT_TIMESTAMP
            WHERE  id = old.id;
        END;
        """
        connection.execute(trg)
        print("Trigger 'psu_ari' created")


        #
        # Housekeeping
        #
        sql = """
        CREATE TABLE housekeeping
        (
            timestamp       INTEGER NOT NULL DEFAULT CURRENT_TIME PRIMARY KEY,
            session_id      INTEGER NOT NULL,
        """
        cols = []
        # dummy columns
        for c in range(0,37):
            cols.append("s_c{:02} INTEGER NOT NULL, ".format(c))    # S: Sun-pointing
            cols.append("r_c{:02} INTEGER NOT NULL, ".format(c))    # R: Rotating
        sql += "".join(cols)
        sql += " FOREIGN KEY (session_id) REFERENCES testing_session (id) )"
        connection.execute(sql)
        print("Table 'housekeeping' created")


    except:
        print("Database creation failed!")
        print(sql)
        os._exit(-1)
    else:
        print("Database creation successful!")
    finally:
        connection.commit()




    #
    # Check that specified 'user.group' 
    #
    check_user_and_group(Config.dbdir_owner)
    check_user_and_group(Config.dbfile_owner)

    #
    # Post-create steps - ownerships and permissions
    #
    print("Setting ownerships and permissions...", end="", flush=True)
    dbdir = os.path.dirname(Config.dbfile)
    do_or_die("chown {} {}".format(Config.dbfile_owner, dbdir))
    do_or_die("chmod 775 " + dbdir)
    do_or_die("chown {} {}".format(Config.dbdir_owner, Config.dbfile))
    do_or_die("chmod 775 " + Config.dbfile)
    print("OK!")


    #
    # Leave, if development content is not requested
    #
    if not args.dev:
        connection.close()
        print("Module 'pmdatabase' setup completed!\n")
        os._exit(0)
    else:
        print("Creating development and testing content...")


    ###########################################################################
    #
    # Development Content Creation
    #
    ###########################################################################
    import csv
    import time
    import random

    # Configurations
    HITCOUNT_ROTATIONS      = 5760      # 5760 equals one day of data
    HITCOUNT_INTERVAL       = 15        # one rotation per 15 seconds
    HITCOUNT_MAXHITS        = 2**21     # Full 21-bit register
    PULSEHEIGHT_CSVFILE     = "sample.csv"
    PULSEHEIGHT_INTERVAL    = 15        # data every 15 seconds
    HOUSEKEEPING_INTERVAL   = 60
    HOUSEKEEPING_SAMPLES    = 1000
    HOUSEKEEPING_MAXVAL     = 255


    def get_session(cursor):
        """Get or create session. Also creates PATE, if needed."""
        #
        # Get session, if available
        #
        row = cursor.execute(
            "SELECT id FROM testing_session LIMIT 1"
        ).fetchone()
        if row:
            return row[0]
        #
        # No testing_session rows, create one
        #
        row = cursor.execute("SELECT id FROM pate LIMIT 1").fetchone()
        if not row:
            # create pate
            cursor.execute("INSERT INTO pate (id_min, id_max, label) VALUES (0, 1000, 'Created to insert sample pulseheight data')")
            pate_id = cursor.lastrowid
        else:
            pate_id = row[0]
        # create testing_session
        cursor.execute(
            """
            INSERT INTO testing_session (started, pate_id, pate_firmware)
            VALUES (?, ?, 'Created to insert sample pulseheight data')
            """,
            (time.strftime('%Y-%m-%d %H:%M:%S'), pate_id)
        )
        return cursor.lastrowid


    def get_column_list(cursor, table, exclude = []):
        """Method that compiles a list of data columns from a table"""
        if not exclude:
            exclude = []
        sql = "SELECT * FROM {} LIMIT 1".format(table)
        cursor.execute(sql)
        # Ignore query result and use cursor.description instead
        return [key[0] for key in cursor.description if key[0] not in exclude]

    def generate_hitcount_insert_sql(cursor):
        sql1 = "INSERT INTO hitcount (timestamp, session_id, "
        sql2 = ") VALUES (?, ?, "
        cols = get_column_list(cursor, "hitcount", ['timestamp', 'session_id'])
        sql_binds = ""
        for col in cols:
            sql_binds += "?, "
        return sql1 + ",".join(cols) + sql2 + sql_binds[:-2] + ")"

    def generate_hitcount_packet(session_id, cursor, interval=15):
        """Generate tuple"""
        # Timestamp
        try:
            (generate_hitcount_packet.timestamp)
        except:
            generate_hitcount_packet.timestamp = int(time.time())
        else:
            generate_hitcount_packet.timestamp += interval
        # Number of columns to provide data for
        try:
            (generate_hitcount_packet.nvars)
        except:
            generate_hitcount_packet.nvars = len(get_column_list(
                cursor,
                "hitcount",
                ['timestamp', 'session_id']
            ))
        nvars = generate_hitcount_packet.nvars
        lst = [random.randint(0, HITCOUNT_MAXHITS) for x in range(0, nvars)]
        lst.insert(0, session_id)
        lst.insert(0, generate_hitcount_packet.timestamp)
        return tuple(lst)

    def generate_housekeeping_sql(cursor):
        sql1 = "INSERT INTO housekeeping (timestamp, session_id, "
        sql2 = ") VALUES (?, ?, "
        cols = get_column_list(
            cursor,
            "housekeeping",
            ['timestamp', 'session_id']
        )
        sql_binds = ""
        for col in cols:
            sql_binds += "?, "
        return sql1 + ",".join(cols) + sql2 + sql_binds[:-2] + ")"

    def generate_housekeeping_packet(session_id, cursor, interval=60):
        """Generate tuple"""
        # Timestamp
        try:
            (generate_housekeeping_packet.timestamp)
        except:
            generate_housekeeping_packet.timestamp = time.time()
        else:
            generate_housekeeping_packet.timestamp += interval
        # Number of columns to provide data for
        try:
            (generate_housekeeping_packet.nvars)
        except:
            generate_housekeeping_packet.nvars = len(get_column_list(
                cursor,
                "housekeeping",
                ['timestamp', 'session_id']
            ))
        nvars = generate_housekeeping_packet.nvars
        lst = [random.randint(0, HOUSEKEEPING_MAXVAL) for x in range(0, nvars)]
        lst.insert(0, session_id)
        lst.insert(0, int(generate_housekeeping_packet.timestamp))
        return tuple(lst)





    cursor = connection.cursor()
    #
    # table 'hitcount' content
    #
    session_id = get_session(cursor)

    # # Clear table - NOT NEEDED, we always work with new database files
    # cursor.execute("DELETE FROM {}".format(Config.table_name))
    # connection.commit()

    # SQL
    sql = generate_hitcount_insert_sql(cursor)
    # Generate sci data rotations
    print("Creating {} rotations of hitcount data...".format(HITCOUNT_ROTATIONS))
    try:
        for i in range(0, HITCOUNT_ROTATIONS):
            print("\r{:>6.2f} % ...".format((100*i)/HITCOUNT_ROTATIONS), end='')
            cursor.execute(
                sql,
                generate_hitcount_packet(session_id, cursor, HITCOUNT_INTERVAL)
            )
    except:
        print("hitcount table content generation failed!")
        print(sql)
        os._exit(-1)
    print("\r100.00 %      ")
    connection.commit()



    #
    # Table 'pulseheight' content
    #
    class excel_finnish(csv.Dialect):
        """Describe the properties of Finnish locale Excel-generated CSV files."""
        delimiter = ';'
        quotechar = '"'
        doublequote = True
        skipinitialspace = False
        lineterminator = '\r\n'
        quoting = csv.QUOTE_MINIMAL

    csv.register_dialect("excel-finnish", excel_finnish)

    # # First clear out existing data - not needed. Always new datafile
    # try:
    #     cursor.execute("DELETE FROM pulseheight")
    # except sqlite3.Error as e:
    #     print(str(e))
    #     os._exit(-1)

    session_id = get_session(cursor)

    sql = """
        INSERT INTO pulseheight (
            timestamp,
            session_id,
            ac1,
            d1a,
            d1b,
            d1c,
            d2a,
            d2b,
            d3,
            ac2
        )
        VALUES (
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?
        )
    """

    print("Importing sample pulseheight data...", end="", flush=True)
    with open(PULSEHEIGHT_CSVFILE, 'r') as csvfile:
        reader = csv.reader(csvfile, dialect='excel-finnish')

        # Skip two first rows
        header = next(reader, None)
        header = next(reader, None)

        # Selected columns 20 - 28, range() is [a, b[ 
        included_cols = [x for x in range(20,29)]

        ts_first = time.time()
        for index, row in enumerate(reader):
            # import pdb; pdb.set_trace()
            content = list(int(row[i], 2) if i == 20 else int(row[i]) for i in included_cols)

            cursor.execute(
                sql,
                (
                    int(ts_first + index * PULSEHEIGHT_INTERVAL),
                    session_id,
                    content[1],
                    content[2],
                    content[3],
                    content[4],
                    content[5],
                    content[6],
                    content[7],
                    content[8]
                )
            )
    connection.commit()
    print("done!")



    #
    # Table 'housekeeping' content
    #

    session_id = get_session(cursor)

    # # Clear table - not needed. always empty database file
    # cursor.execute("DELETE FROM {}".format(Config.table_name))
    # connection.commit()

    # SQL
    sql = generate_housekeeping_sql(cursor)
    # Generate sci data samples
    print(
        "Creating {} samples of housekeeping data...".format(
            HOUSEKEEPING_SAMPLES
        )
    )
    try:
        for i in range(0, HOUSEKEEPING_SAMPLES):
            print(
                "\r{:>6.2f} % ...".format((100*i)/HOUSEKEEPING_SAMPLES),
                end='',
                flush=True
            )
            cursor.execute(
                sql,
                generate_housekeeping_packet(
                    session_id,
                    cursor,
                    HOUSEKEEPING_INTERVAL
                )
            )
    except:
        print("Housekeeping dev content generation failed!")
        print(sql)
        os._exit(-1)
    else:
        print("\r100.00 %      ")
    finally:
        connection.commit()



    connection.commit()
    connection.close()
    print("Module 'pmdatabase' setup completed!\n")


# EOF