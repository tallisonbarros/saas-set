from django.db import migrations, models
import django.db.models.deletion


def _normalize_code(value):
    value = "".join(ch for ch in (value or "").upper() if ch.isalnum())
    return value[:3] if value else "ATV"


def forwards(apps, schema_editor):
    TipoAtivo = apps.get_model("core", "TipoAtivo")
    Ativo = apps.get_model("core", "Ativo")
    AtivoItem = apps.get_model("core", "AtivoItem")

    defaults = {
        "MOTOR": ("Motor", "MOT"),
        "VALVULA": ("Valvula", "VAL"),
        "EQUIPAMENTO": ("Equipamento", "EQU"),
        "CONJUNTO": ("Conjunto", "CON"),
        "TRANSMISSOR_ANALOGICO": ("Transmissor analogico", "TMA"),
        "TRANSMISSOR_DIGITAL": ("Transmissor digital", "TMD"),
        "SONORO": ("Sonoro", "SON"),
        "VISUAL": ("Visual", "VIS"),
        "OUTRO": ("Outro", "OUT"),
    }
    used_codes = set()
    tipo_cache = {}

    def get_or_create_tipo(raw_value):
        if raw_value in tipo_cache:
            return tipo_cache[raw_value]
        nome, codigo = defaults.get(raw_value, (raw_value.title() if raw_value else "Outro", None))
        code = codigo or _normalize_code(raw_value or nome)
        base = code or "ATV"
        suffix = 1
        while base in used_codes or TipoAtivo.objects.filter(codigo=base).exists():
            suffix += 1
            base = f"{code}{suffix}"
        tipo = TipoAtivo.objects.create(nome=nome, codigo=base, ativo=True)
        used_codes.add(base)
        tipo_cache[raw_value] = tipo
        return tipo

    ativos = list(Ativo.objects.exclude(tipo__isnull=True).exclude(tipo="").values_list("id", "tipo"))
    for ativo_id, tipo_raw in ativos:
        tipo = get_or_create_tipo(tipo_raw)
        Ativo.objects.filter(id=ativo_id).update(tipo_ref=tipo)

    itens = list(AtivoItem.objects.exclude(tipo__isnull=True).exclude(tipo="").values_list("id", "tipo"))
    for item_id, tipo_raw in itens:
        tipo = get_or_create_tipo(tipo_raw)
        AtivoItem.objects.filter(id=item_id).update(tipo_ref=tipo)


def backwards(apps, schema_editor):
    pass


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0034_inventario_tagset_pattern"),
    ]

    operations = [
        migrations.CreateModel(
            name="TipoAtivo",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("nome", models.CharField(max_length=80, unique=True)),
                ("codigo", models.CharField(max_length=10, unique=True)),
                ("ativo", models.BooleanField(default=True)),
                ("criado_em", models.DateTimeField(auto_now_add=True)),
            ],
            options={
                "ordering": ["nome"],
            },
        ),
        migrations.AddField(
            model_name="ativo",
            name="tipo_ref",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.PROTECT,
                related_name="ativos",
                to="core.tipoativo",
            ),
        ),
        migrations.AddField(
            model_name="ativoitem",
            name="tipo_ref",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.PROTECT,
                related_name="itens",
                to="core.tipoativo",
            ),
        ),
        migrations.RunPython(forwards, backwards),
        migrations.RemoveField(
            model_name="ativo",
            name="tipo",
        ),
        migrations.RemoveField(
            model_name="ativoitem",
            name="tipo",
        ),
        migrations.RenameField(
            model_name="ativo",
            old_name="tipo_ref",
            new_name="tipo",
        ),
        migrations.RenameField(
            model_name="ativoitem",
            old_name="tipo_ref",
            new_name="tipo",
        ),
    ]
