from django.core.management.base import BaseCommand

from getkpi.restore_scheduler import run_restore_once


class Command(BaseCommand):
    help = (
        "Native restore of the newest Z:\\erp_pm*.bak into local SQL Server, "
        "then refresh commercial director caches on success."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--force",
            action="store_true",
            help="Force full cycle even if local bak already matches Z:\\",
        )
        parser.add_argument(
            "--no-commercial",
            action="store_true",
            help="Only restore; skip commercial cache refresh (uses sql_restore directly).",
        )

    def handle(self, *args, **options):
        force = bool(options["force"])
        if options["no_commercial"]:
            from sql_restore.restore_native import run_restore

            result = run_restore(force=force)
        else:
            result = run_restore_once(force=force)
        self.stdout.write(self.style.NOTICE(repr(result)))
        status = str(result.get("status") or "")
        if status in {"error", "disabled", "already_running"}:
            raise SystemExit(1 if status == "error" else 2)
