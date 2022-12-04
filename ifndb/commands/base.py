import os
from cliff.command import Command
from . import register

from ..importer import Importer, CSVDataSource
class ImportCommand(Command):
    """
    import data
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

register(ImportCommand)