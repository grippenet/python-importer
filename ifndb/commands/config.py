from cliff.command import Command
from . import register
from ..config import settings
class ShowConfigCommand(Command):
    """
    Show config
    """

    name = 'config'

    def get_parser(self, prog_name):
        parser = super(ShowConfigCommand, self).get_parser(prog_name)
        return parser

    def take_action(self, args):
       print(settings)
       
register(ShowConfigCommand)