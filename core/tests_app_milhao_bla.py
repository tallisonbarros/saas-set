from django.contrib.auth.models import User
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from core.models import App, IngestRecord, PerfilUsuario


class AppMilhaoBlaIngestConfigTests(TestCase):
    def setUp(self):
        self.app = App.objects.create(
            slug="appmilhaobla",
            nome="App Milhao Bla",
            ativo=True,
            ingest_client_id="UBS3-UN1",
            ingest_agent_id="VMSCADA",
            ingest_source="balanca_acumulado_hora",
        )
        self.user = User.objects.create_user(username="bla_user", password="123456", email="bla@example.com")
        self.perfil = PerfilUsuario.objects.create(
            nome="Bla User",
            email="bla@example.com",
            usuario=self.user,
            ativo=True,
        )
        self.perfil.apps.add(self.app)

    def test_dashboard_uses_ingest_config_from_app(self):
        now_iso = timezone.now().isoformat()
        IngestRecord.objects.create(
            source_id="bla-1",
            client_id="UBS3-UN1",
            agent_id="VMSCADA",
            source="balanca_acumulado_hora",
            payload={"TagName": "LIMBL01", "Hora": now_iso, "ProducaoHora": "10"},
        )
        IngestRecord.objects.create(
            source_id="bla-2",
            client_id="clienteA",
            agent_id="agente01",
            source="balanca_acumulado_hora",
            payload={"TagName": "LIMBL01", "Hora": now_iso, "ProducaoHora": "99"},
        )

        self.client.force_login(self.user)
        response = self.client.get(reverse("app_milhao_bla_dashboard"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Cliente: UBS3-UN1")
        entries = response.context["entries"]
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["value"], 10.0)

