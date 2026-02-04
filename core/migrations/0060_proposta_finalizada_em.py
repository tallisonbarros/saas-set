from django.db import migrations, models
from django.utils import timezone


def set_finalizada_em(apps, schema_editor):
    Proposta = apps.get_model("core", "Proposta")
    for proposta in Proposta.objects.filter(finalizada=True, finalizada_em__isnull=True):
        fallback = proposta.decidido_em or proposta.criado_em or timezone.now()
        proposta.finalizada_em = fallback
        proposta.save(update_fields=["finalizada_em"])


class Migration(migrations.Migration):
    dependencies = [
        ("core", "0059_ingest_error_log_resolved"),
    ]

    operations = [
        migrations.AddField(
            model_name="proposta",
            name="finalizada_em",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.RunPython(set_finalizada_em, migrations.RunPython.noop),
    ]
