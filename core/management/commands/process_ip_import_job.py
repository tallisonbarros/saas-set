import logging

from django.core.management.base import BaseCommand, CommandError

from core.models import IPImportJob
from core.views import _failed_ip_import_progress_payload, _reprocess_ip_import_job


logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Processa um job de importacao de IP em segundo plano."

    def add_arguments(self, parser):
        parser.add_argument("job_id", type=int)

    def handle(self, *args, **options):
        job_id = options["job_id"]
        try:
            job = IPImportJob.objects.get(pk=job_id)
        except IPImportJob.DoesNotExist as exc:
            raise CommandError(f"Job de importacao IP {job_id} nao encontrado.") from exc

        self.stdout.write(f"[ip-import] processando job {job.pk} ({job.original_filename})")
        try:
            _reprocess_ip_import_job(job)
        except Exception as exc:
            logger.exception("IP import background job failed", extra={"job_id": job.pk})
            job.status = IPImportJob.Status.FAILED
            job.ai_status = IPImportJob.AIStatus.FAILED
            job.ai_error = str(exc)
            warnings = list(job.warnings or [])
            warnings.append(f"Falha interna durante o processamento em segundo plano: {exc}")
            job.warnings = warnings
            job.progress_payload = _failed_ip_import_progress_payload(str(exc), job.progress_payload)
            job.save(update_fields=["status", "ai_status", "ai_error", "warnings", "progress_payload", "updated_at"])
            raise CommandError(f"Falha ao processar job {job.pk}: {exc}") from exc

        job.refresh_from_db(fields=["status", "ai_status", "rows_parsed"])
        self.stdout.write(
            self.style.SUCCESS(
                f"[ip-import] job {job.pk} finalizado com status={job.status} ai_status={job.ai_status} rows={job.rows_parsed}"
            )
        )
