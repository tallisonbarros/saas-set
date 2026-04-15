from django.db import migrations, models


def migrate_payment_provider_to_stripe(apps, schema_editor):
    ConfiguracaoPagamento = apps.get_model("core", "ConfiguracaoPagamento")
    for config in ConfiguracaoPagamento.objects.all():
        config.provider = "STRIPE"
        config.save(update_fields=["provider"])


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0091_consumoimportacaodiaria_and_more"),
    ]

    operations = [
        migrations.AddField(
            model_name="configuracaopagamento",
            name="stripe_publishable_key",
            field=models.CharField(blank=True, default="", max_length=255),
        ),
        migrations.AddField(
            model_name="configuracaopagamento",
            name="stripe_secret_key",
            field=models.CharField(blank=True, default="", max_length=255),
        ),
        migrations.AddField(
            model_name="configuracaopagamento",
            name="stripe_webhook_secret",
            field=models.CharField(blank=True, default="", max_length=255),
        ),
        migrations.RemoveField(
            model_name="configuracaopagamento",
            name="mercado_pago_access_token",
        ),
        migrations.RemoveField(
            model_name="configuracaopagamento",
            name="mercado_pago_public_key",
        ),
        migrations.RemoveField(
            model_name="configuracaopagamento",
            name="mercado_pago_webhook_secret",
        ),
        migrations.AlterField(
            model_name="assinaturausuario",
            name="provider",
            field=models.CharField(
                choices=[("INTERNAL", "Interno"), ("STRIPE", "Stripe")],
                default="INTERNAL",
                max_length=20,
            ),
        ),
        migrations.AlterField(
            model_name="configuracaopagamento",
            name="provider",
            field=models.CharField(choices=[("STRIPE", "Stripe")], default="STRIPE", max_length=20),
        ),
        migrations.AlterField(
            model_name="eventopagamentowebhook",
            name="provider",
            field=models.CharField(choices=[("STRIPE", "Stripe")], max_length=20),
        ),
        migrations.RunPython(migrate_payment_provider_to_stripe, migrations.RunPython.noop),
    ]
