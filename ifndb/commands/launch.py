import os
import json
from cliff.command import Command
from . import register
from ..db import DbQuery, connection
from ..sql import load_template

class LaunchViewCommand(Command):
    """
    Create table and view for new season
    """

    name = 'launch:view'

    def get_parser(self, prog_name):
        parser = super(LaunchViewCommand, self).get_parser(prog_name)
        parser.add_argument("season", help="Season number")
        parser.add_argument("--apply", help="apply query", action="store_true", default=False)
        parser.add_argument("--current", help="set as default view", action="store_true", default=False)
        return parser

    def take_action(self, args):
        season = args.season
        query = load_template('launchview', {'year':season})

        if args.current:
            default_query = load_template('defaultview', {'year':season})
            query += "\n\n" + default_query
            
        if args.apply:
            connection.connect()
            q = DbQuery()
            q.execute(query)
            print("Launch view applied")
        else:
            print(query)
               

       
register(LaunchViewCommand)