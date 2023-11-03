from . import register
from cliff.command import Command
from ..config import settings
from ..exporter import ExportProfile, ExporterManager, get_export_table
from ..db import DbQuery
from ..common import TABLES
from pathlib import Path
from rich.console import Console
from rich.table import Table
class ExportCommand(Command):
    """
    Create table and view for new season
    """

    name = 'export'

    def get_parser(self, prog_name):
        parser = super(ExportCommand, self).get_parser(prog_name)
        parser.add_argument("profile", help="Profile name (name of the yaml file in export_profile_path)")
        parser.add_argument("survey", help="Profile survey name, all if None", default=None, nargs="?")
        g = parser.add_mutually_exclusive_group()
        g.add_argument("--config", help="Show Config and exit", action="store_true", default=False)
        g.add_argument("--select", help="Test select query", action="store_true", default=False)
        g.add_argument("--apply", help="apply query", action="store_true", default=False)
        g.add_argument("--show", help="Show query (default)", action="store_true", default=False)
        return parser

    def take_action(self, args):
        profile_path = settings.get('export_profile_path', None)
        if profile_path is None:
            print("Define 'export_profile_path' in settings.json with path to export profiles")
        profile_name = args.profile
        survey_name = args.survey
        file = Path(profile_path, profile_name + '.yaml')
        profile = ExportProfile.from_yaml(file, {})
        if args.config:
            print(repr(profile))
            return
        exporter = ExporterManager(profile)
        results = exporter.build(survey_name)
        has_error = False
        for name, res in results.items():
            if(len(res.errors) > 0):
                has_error = True
                print("Errors:")
                for err in res.errors:
                    print(err)
        if has_error:
            print("Export profile has errors, cannot continue")
            return
        
        if args.show:
            dry_run = True
        else:
            dry_run = False
        
        for name, res in results.items():
            print("Export profile ", name)
            update = res.update
            src_range = update.source_range()
            if args.select:
                select_query = update.select()
                print(select_query)
                try:
                    q = DbQuery()
                    count = q.execute(select_query)
                    print("%d rows" % (count))
                except Exception as e:
                    print("Error running select query")
                    print(e)
            if args.apply or args.show:
                q = DbQuery()
                try:
                    cleanup_query = "DELETE FROM %s where timestamp >= %%s and timestamp <= %%s" % (update.target_table)
                    if dry_run:
                        print(cleanup_query.replace('%%s','%s') % (src_range[0], src_range[1]))
                    else:
                        q.execute(cleanup_query, (src_range[0], src_range[1]))
                    update_query = update.query()
                    if dry_run:
                        print(update_query)
                    else:
                        q.execute(update_query)
                except Exception as e:
                    print("Error executing query")
                    print(e)

class ExportShowCommand(Command):
    """
    Create table and view for new season
    """

    name = 'export:show'

    def get_parser(self, prog_name):
        parser = super(ExportShowCommand, self).get_parser(prog_name)
        parser.add_argument("survey", help="Profile survey name, all if None", default=None, nargs="?")
        parser.add_argument("season", help="Profile survey name, all if None", default=None, nargs="?")
        return parser

    def take_action(self, args):
        surveys = TABLES
        tb = Table()
        tb.add_column("Survey")
        tb.add_column("Season")
        tb.add_column("Count")
        tb.add_column("Min")
        tb.add_column("Max")
        
        console = Console()
        season = True
        for survey in surveys:
            tb.add_section()
            q = DbQuery()

            if season:
                season_col = "date_part('year', \"timestamp\")::int - case when date_part('month', \"timestamp\") < 9 then 1 else 0 end";
            else:
                season_col = "'all'"    
            query = "select %s \"season\", count(*), min(timestamp), max(timestamp) from %s group by \"season\" order by \"season\" " % (season_col, get_export_table(survey))
            rr = q.fetch(query)
            for r in rr:
                tb.add_row(survey, str(r[0]), str(r[1]), str(r[2]), str(r[3]))
        console.print(tb)
    

register(ExportCommand)
register(ExportShowCommand)