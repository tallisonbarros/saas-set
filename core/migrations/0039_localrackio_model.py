from django.db import migrations, models


def migrate_local_racks(apps, schema_editor):
    LocalRackIO = apps.get_model("core", "LocalRackIO")
    RackIO = apps.get_model("core", "RackIO")
    for rack in RackIO.objects.exclude(local__isnull=True).exclude(local="").iterator():
        local_name = (rack.local or "").strip()
        if not local_name:
            continue
        local_obj, _ = LocalRackIO.objects.get_or_create(nome=local_name)
        rack.local_ref = local_obj
        rack.save(update_fields=["local_ref"])


def reverse_local_racks(apps, schema_editor):
    RackIO = apps.get_model("core", "RackIO")
    for rack in RackIO.objects.exclude(local_ref__isnull=True).iterator():
        rack.local = rack.local_ref.nome
        rack.save(update_fields=["local"])


class Migration(migrations.Migration):
    dependencies = [
        ("core", "0038_rackio_local"),
    ]

    operations = [
        migrations.CreateModel(
            name="LocalRackIO",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("nome", models.CharField(max_length=120, unique=True)),
            ],
            options={"ordering": ["nome"]},
        ),
        migrations.AddField(
            model_name="rackio",
            name="local_ref",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=models.SET_NULL,
                related_name="racks",
                to="core.localrackio",
            ),
        ),
        migrations.RunPython(migrate_local_racks, reverse_local_racks),
        migrations.RemoveField(
            model_name="rackio",
            name="local",
        ),
        migrations.RenameField(
            model_name="rackio",
            old_name="local_ref",
            new_name="local",
        ),
    ]
