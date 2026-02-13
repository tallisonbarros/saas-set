from django.conf import settings
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0064_proposta_origem_trabalho"),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.AddField(
            model_name="radartrabalho",
            name="criado_por",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=models.SET_NULL,
                related_name="radar_trabalhos_criados",
                to=settings.AUTH_USER_MODEL,
            ),
        ),
    ]
