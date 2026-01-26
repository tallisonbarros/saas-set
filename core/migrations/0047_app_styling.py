from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("core", "0046_apps"),
    ]

    operations = [
        migrations.AddField(
            model_name="app",
            name="icon",
            field=models.CharField(blank=True, max_length=80),
        ),
        migrations.AddField(
            model_name="app",
            name="theme_color",
            field=models.CharField(blank=True, max_length=30),
        ),
    ]
