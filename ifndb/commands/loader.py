import os
import json
from cliff.command import Command
from . import register
from ..utils import from_iso_time,write_content,read_json
from ..db import DbQuery, connection
from ..importer import Importer, CSVDataSource, Profile
from collections import Counter
import random
import string

def get_random_string(length):
    letters = string.ascii_letters + string.digits
    return ''.join(random.choice(letters) for i in range(length))


class ImportCommand(Command):
    """
    import data from a csv file, using a transformation profile (describing transformation)
    """

    name = 'import'

    def get_parser(self, prog_name):
        parser = super(ImportCommand, self).get_parser(prog_name)
        parser.add_argument("profile", help="profile")
        parser.add_argument("table", help="Table")
        parser.add_argument("file", help="csv file to import")
        parser.add_argument("--dry-run", action="store_true")
        parser.add_argument("--show-batch", type=int, default=0)
        parser.add_argument("--show-batch-row", type=int, default=0)
        
        return parser

    def take_action(self, args):

        file = args.file
        path = os.path.dirname(file)

        debug = self.app.options.debug

        opts = {
            'debug': debug,
            'dry_run': args.dry_run,
            'show_batch': args.show_batch,
            'show_batch_row': args.show_batch_row,
        }

        source = CSVDataSource(file, 'submitted')

        importer = Importer(path, opts)
        importer.load_profile(args.profile)
        importer.import_table(args.table, source)

class ImportCatalogCommand(Command):
    """
    import data from a catalog file (several files), using a transformation profile (describing transformation)
    """

    name = 'import-catalog'

    def get_parser(self, prog_name):
        parser = super(ImportCatalogCommand, self).get_parser(prog_name)
        parser.add_argument("--profile", help="profile yaml file describing the import process", required=True)
        parser.add_argument("--table", help="Table name (entry name in profile)", required=True)
        parser.add_argument("--catalog", help="csv of catalog files", required=True)
        parser.add_argument("--dry-run", action="store_true") 
        return parser

    def take_action(self, args):

        catalog_file = args.catalog
        path = os.path.dirname(catalog_file)

        debug = self.app.options.debug

        opts = {
            'debug': debug,
            'dry_run': args.dry_run,
        }
        
        data_path = os.path.dirname(catalog_file)

        mark_ext = '.done'

        catalog = read_json(catalog_file)

        counter = Counter()
        
        for catalog_entry in catalog['files']:
            file = data_path + '/' + catalog_entry['file']
            mark_file = file + mark_ext
            if os.path.exists(mark_file):
                skip = True
                if os.path.getmtime(mark_file) < os.path.getmtime(file):
                    print("File %s to be updated" % file)
                    skip = False
                if skip:
                    counter['skipped'] += 1
                    continue
            min_time = from_iso_time(catalog_entry['start'])
            max_time = from_iso_time(catalog_entry['end'])
            print("Processing %s [%s, %s]" % (file, min_time, max_time))
            source = CSVDataSource(file, 'submitted')
            source.set_time_range(min_time, max_time)
            importer = Importer(path, opts)
            importer.load_profile(args.profile)
            importer.import_table(args.table, source)
            write_content(mark_file, '') # Mark file as done
            counter['processed'] += 1
        print("%d processed, %d skipped (already done)" % (counter['processed'], counter['skipped']))


class QuerySet:

    def __init__(self):
        self.querys = []

    def add(self, label, query):
        self.querys.append( (label, query))
    
    def run(self, db: DbQuery):
        for q in self.querys:
            label, query = q
            n = db.execute(query)
            print("%s : %d" % (label, n))
    
    def show(self):
        for q in self.querys:
            label, query = q
            print("-- %s" % (label))
            print("%s;" % (query) )
    
class UpdateParticipantsCommand(Command):
    """
    Update participant table 
    """

    name = 'update-participants'

    def get_parser(self, prog_name):
        parser = super(UpdateParticipantsCommand, self).get_parser(prog_name)
        parser.add_argument("--profile", help="profile yaml file describing the import process", required=True)
        parser.add_argument("--dry-run", action="store_true") 
        return parser
    
    def take_action(self, args):
        
        profile = Profile.from_yaml(args.profile, {'skip_prepare': True})

        dry_run = args.dry_run

        tables = []

        for name, table_conf in profile.tables.items():
            table = table_conf.get_table_name()
            tables.append(table)

        connection.connect()

        part_table = "participant_ids_" + get_random_string(5)

        qq = map(lambda t: "select global_id from %s " % (t), tables )

        qq = " union ".join(qq)

        querys = QuerySet()

        querys.add("create_table", "CREATE TEMPORARY TABLE %s (id VARCHAR(45) NOT NULL)" % (part_table))

        query = "INSERT INTO %s (id) SELECT DISTINCT global_id FROM(%s) t" % (part_table, qq)

        querys.add("insert ids", query)

        querys.add("remove existing", "DELETE FROM %s WHERE id IN(select global_id from survey_surveyuser)" % (part_table))

        querys.add("insert remains", "INSERT INTO survey_surveyuser (global_id, participant_id) SELECT id, id from %s" % (part_table))

        if dry_run:
            querys.show()
        else:
            db = DbQuery()
            querys.run(db)

register(ImportCommand)
register(ImportCatalogCommand)
register(UpdateParticipantsCommand)