from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("core", "0048_merge_0046_ingestrecord_0047_app_styling"),
    ]

    operations = [
        migrations.AddField(
            model_name="app",
            name="logo",
            field=models.ImageField(blank=True, null=True, upload_to="apps/logos/"),
        ),
    ]
