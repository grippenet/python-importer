from . import register
from cliff.command import Command
from ..config import settings
from ..exporter import ExportProfile, ExporterManager
from pathlib import Path

class ExportCommand(Command):
    """
    Create table and view for new season
    """

    name = 'export'

    def get_parser(self, prog_name):
        parser = super(ExportCommand, self).get_parser(prog_name)
        parser.add_argument("name", help="Profile name")
        parser.add_argument("--apply", help="apply query", action="store_true", default=False)
        return parser

    def take_action(self, args):
        profile_path = settings.get('export_profile_path', None)
        if profile_path is None:
            print("Define 'export_profile_path' in settings.json with path to export profiles")
        profile_name = args.name
        file = Path(profile_path,profile_name + '.yaml')
        profile = ExportProfile.from_yaml(file, {})
        #print(r)

        exporter = ExporterManager(profile)
        results = exporter.build()
        for name, res in results:
            print(name)
            if(len(res.errors) > 0):
                print("Errors:")
                for err in res.errors:
                    print(err)
            else:
                query = res.query
                print(query.query())

register(ExportCommand)