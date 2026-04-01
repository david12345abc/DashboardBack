from django.core.management.base import BaseCommand

from User.models import User


class Command(BaseCommand):
    help = 'Create the initial User1 (admin) account'

    def add_arguments(self, parser):
        parser.add_argument('--nickname', required=True)
        parser.add_argument('--password', required=True)

    def handle(self, *args, **options):
        nickname = options['nickname']
        password = options['password']

        if User.objects.filter(nickname=nickname).exists():
            self.stderr.write(f'User "{nickname}" already exists.')
            return

        user = User(nickname=nickname, role=User.Role.USER1)
        user.set_password(password)
        user.save()

        self.stdout.write(self.style.SUCCESS(
            f'Admin "{nickname}" (User1) created successfully.'
        ))
