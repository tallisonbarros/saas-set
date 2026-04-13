import logging

from django.core.management.base import BaseCommand, CommandError

from core.models import IOImportJob
from core.views import _reprocess_io_import_job


logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Processa um job de importacao de IO em segundo plano."

    def add_arguments(self, parser):
        parser.add_argument("job_id", type=int)

    def handle(self, *args, **options):
        job_id = options["job_id"]
        try:
            job = IOImportJob.objects.get(pk=job_id)
        except IOImportJob.DoesNotExist as exc:
            raise CommandError(f"Job de importacao IO {job_id} nao encontrado.") from exc

        self.stdout.write(f"[io-import] processando job {job.pk} ({job.original_filename})")
        try:
            _reprocess_io_import_job(job)
        except Exception as exc:
            logger.exception("IO import background job failed", extra={"job_id": job.pk})
            job.status = IOImportJob.Status.FAILED
            warnings = list(job.warnings or [])
            warnings.append(f"Falha interna durante o processamento em segundo plano: {exc}")
            job.warnings = warnings
            job.save(update_fields=["status", "warnings", "updated_at"])
            raise CommandError(f"Falha ao processar job {job.pk}: {exc}") from exc

        job.refresh_from_db(fields=["status", "ai_status", "rows_parsed"])
        self.stdout.write(
            self.style.SUCCESS(
                f"[io-import] job {job.pk} finalizado com status={job.status} ai_status={job.ai_status} rows={job.rows_parsed}"
            )
        )
