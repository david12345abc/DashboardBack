"""manage.py list_enterprise_positions — см. tools.scripts.list_enterprise_positions."""
from django.core.management.base import BaseCommand

from tools.scripts.list_enterprise_positions import run


class Command(BaseCommand):
    help = "Должности и ФИО сотрудников из 1С (OData)"

    def add_arguments(self, parser):
        parser.add_argument(
            "root",
            nargs="?",
            default=None,
            help="Корень в структуре предприятия",
        )
        parser.add_argument("--root", dest="root_flag", default=None)
        parser.add_argument("--position", default=None)
        parser.add_argument("--dept-path", default=None)
        parser.add_argument("--employees-only", action="store_true")
        parser.add_argument("--force", action="store_true")
        parser.add_argument("--output", "-o", default=None)
        parser.add_argument("--json", action="store_true")

    def handle(self, *args, **options):
        argv: list[str] = []
        root = options.get("root_flag") or options.get("root")
        if root:
            argv.append(str(root))
        if options.get("position"):
            argv.extend(["--position", options["position"]])
        if options.get("dept_path"):
            argv.extend(["--dept-path", options["dept_path"]])
        if options.get("employees_only"):
            argv.append("--employees-only")
        if options.get("force"):
            argv.append("--force")
        if options.get("output"):
            argv.extend(["--output", options["output"]])
        if options.get("json"):
            argv.append("--json")

        code = run(argv)
        if code:
            self.stderr.write(self.style.ERROR(f"Завершено с кодом {code}"))
