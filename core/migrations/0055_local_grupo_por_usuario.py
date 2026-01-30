from django.db import migrations, models
import django.db.models.deletion


def split_locais_grupos(apps, schema_editor):
    RackIO = apps.get_model("core", "RackIO")
    LocalRackIO = apps.get_model("core", "LocalRackIO")
    GrupoRackIO = apps.get_model("core", "GrupoRackIO")
    db_alias = schema_editor.connection.alias

    local_map = {}
    racks_with_local = (
        RackIO.objects.using(db_alias)
        .select_related("local", "cliente")
        .exclude(local_id__isnull=True)
    )
    for rack in racks_with_local:
        local = rack.local
        key = (local.id, rack.cliente_id)
        if local.cliente_id == rack.cliente_id:
            local_map.setdefault(key, local.id)
            continue
        new_id = local_map.get(key)
        if not new_id:
            new_local = LocalRackIO.objects.using(db_alias).create(
                nome=local.nome,
                cliente_id=rack.cliente_id,
            )
            new_id = new_local.id
            local_map[key] = new_id
        rack.local_id = new_id
        rack.save(update_fields=["local"])

    grupo_map = {}
    racks_with_grupo = (
        RackIO.objects.using(db_alias)
        .select_related("grupo", "cliente")
        .exclude(grupo_id__isnull=True)
    )
    for rack in racks_with_grupo:
        grupo = rack.grupo
        key = (grupo.id, rack.cliente_id)
        if grupo.cliente_id == rack.cliente_id:
            grupo_map.setdefault(key, grupo.id)
            continue
        new_id = grupo_map.get(key)
        if not new_id:
            new_grupo = GrupoRackIO.objects.using(db_alias).create(
                nome=grupo.nome,
                cliente_id=rack.cliente_id,
            )
            new_id = new_grupo.id
            grupo_map[key] = new_id
        rack.grupo_id = new_id
        rack.save(update_fields=["grupo"])


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0054_admin_access_log_simple"),
    ]

    operations = [
        migrations.AddField(
            model_name="localrackio",
            name="cliente",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.CASCADE,
                related_name="locais_rack",
                to="core.perfilusuario",
            ),
        ),
        migrations.AddField(
            model_name="gruporackio",
            name="cliente",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.CASCADE,
                related_name="grupos_rack",
                to="core.perfilusuario",
            ),
        ),
        migrations.AlterField(
            model_name="localrackio",
            name="nome",
            field=models.CharField(max_length=120),
        ),
        migrations.AlterField(
            model_name="gruporackio",
            name="nome",
            field=models.CharField(max_length=120),
        ),
        migrations.RunPython(split_locais_grupos, migrations.RunPython.noop),
        migrations.AddConstraint(
            model_name="localrackio",
            constraint=models.UniqueConstraint(
                fields=("cliente", "nome"),
                name="unique_localrackio_cliente_nome",
            ),
        ),
        migrations.AddConstraint(
            model_name="gruporackio",
            constraint=models.UniqueConstraint(
                fields=("cliente", "nome"),
                name="unique_gruporackio_cliente_nome",
            ),
        ),
    ]
