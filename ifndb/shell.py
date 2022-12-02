import sys
import os
from cliff.app import App
from cliff.commandmanager import CommandManager

from .commands import get_commands
from .config import load_config
class MyApp(App):
    
    def build_option_parser(self, description, version):
        parser = super(MyApp, self).build_option_parser(
            description,
            version,
        )
        return parser

    def initialize_app(self, argv):
        load_config()
        commands = get_commands()
        for command in commands:
            if hasattr(command, 'name'):
                name = command.name
            else:
                name = command.__name__
            self.command_manager.add_command(name.lower(), command)

def main(argv=sys.argv[1:]):
    if len(argv) == 0:
        argv = ['help']
    app = MyApp(
            description="IfnDb CLI",
            version="0.0.1",
            command_manager=CommandManager('ifndb'),
            deferred_help=True,
        )
    return app.run(argv)


if __name__ == '__main__':
    sys.exit(main())