from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("core", "0037_canalrackio_links"),
    ]

    operations = [
        migrations.AddField(
            model_name="rackio",
            name="local",
            field=models.CharField(blank=True, max_length=120),
        ),
    ]
