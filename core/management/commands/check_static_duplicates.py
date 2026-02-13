from collections import defaultdict
from pathlib import Path

from django.contrib.staticfiles import finders
from django.core.management.base import BaseCommand, CommandError


class Command(BaseCommand):
    help = (
        "Verifica conflitos de arquivos estaticos com o mesmo caminho logico "
        "(duplicidade entre static sources)."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--show-all",
            action="store_true",
            help="Mostra todos os arquivos mesmo quando nao ha conflito.",
        )

    def handle(self, *args, **options):
        path_map = defaultdict(list)

        for finder in finders.get_finders():
            for rel_path, storage in finder.list([]):
                abs_path = self._resolve_absolute_path(storage, rel_path)
                path_map[rel_path].append(abs_path)

        duplicates = {
            rel_path: sorted({p for p in locations if p})
            for rel_path, locations in path_map.items()
            if len({p for p in locations if p}) > 1
        }

        if options["show_all"]:
            self.stdout.write(self.style.NOTICE("Arquivos estaticos mapeados:"))
            for rel_path in sorted(path_map):
                unique_locations = sorted({p for p in path_map[rel_path] if p})
                self.stdout.write(f"- {rel_path}")
                for location in unique_locations:
                    self.stdout.write(f"    {location}")

        if duplicates:
            self.stderr.write(self.style.ERROR("Conflitos de static encontrados:"))
            for rel_path in sorted(duplicates):
                self.stderr.write(f"- {rel_path}")
                for location in duplicates[rel_path]:
                    self.stderr.write(f"    {location}")
            raise CommandError(
                f"{len(duplicates)} conflito(s) de static detectado(s). Resolva antes de deploy/CI."
            )

        self.stdout.write(self.style.SUCCESS("Sem conflitos de static paths."))

    def _resolve_absolute_path(self, storage, rel_path):
        try:
            candidate = storage.path(rel_path)
        except Exception:
            candidate = ""

        if not candidate:
            return ""

        try:
            return str(Path(candidate).resolve())
        except Exception:
            return str(candidate)
