from io import BytesIO

from django.contrib.auth.models import User
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone
from openpyxl import load_workbook

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

    def test_cards_data_endpoint_returns_live_payload(self):
        now_iso = timezone.now().isoformat()
        IngestRecord.objects.create(
            source_id="bla-3",
            client_id="UBS3-UN1",
            agent_id="VMSCADA",
            source="balanca_acumulado_hora",
            payload={"TagName": "LIMBL01", "Hora": now_iso, "ProducaoHora": "10"},
        )
        IngestRecord.objects.create(
            source_id="bla-4",
            client_id="UBS3-UN1",
            agent_id="VMSCADA",
            source="balanca_acumulado_hora",
            payload={"TagName": "SECBL01", "Hora": now_iso, "ProducaoHora": "5"},
        )

        self.client.force_login(self.user)
        response = self.client.get(reverse("app_milhao_bla_cards_data"))
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["total_value_display"], "10")
        total_card = next(item for item in payload["totals_by_balance"] if item["balance"] == "TOTAL")
        self.assertEqual(total_card["total_display"], "5")
        self.assertGreaterEqual(len(payload["composition"]), 1)

    def test_export_excel_endpoint_returns_workbook_with_expected_structure(self):
        IngestRecord.objects.create(
            source_id="bla-export-1",
            client_id="UBS3-UN1",
            agent_id="VMSCADA",
            source="balanca_acumulado_hora",
            payload={"TagName": "LIMBL01", "Hora": "2026-03-01T08:00:00", "ProducaoHora": "10"},
        )
        IngestRecord.objects.create(
            source_id="bla-export-2",
            client_id="UBS3-UN1",
            agent_id="VMSCADA",
            source="balanca_acumulado_hora",
            payload={"TagName": "SECBL01", "Hora": "2026-03-01T09:00:00", "ProducaoHora": "5"},
        )
        IngestRecord.objects.create(
            source_id="bla-export-3",
            client_id="UBS3-UN1",
            agent_id="VMSCADA",
            source="balanca_acumulado_hora",
            payload={"TagName": "CLABL01", "Hora": "2026-03-02T10:00:00", "ProducaoHora": "7"},
        )

        self.client.force_login(self.user)
        response = self.client.post(
            reverse("app_milhao_bla_export_excel"),
            {"start_date": "2026-03-01", "end_date": "2026-03-02"},
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn(
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            response["Content-Type"],
        )
        self.assertIn(
            "milhao_bla_20260301_a_20260302.xlsx",
            response["Content-Disposition"],
        )

        workbook = load_workbook(filename=BytesIO(response.content))
        self.assertListEqual(
            workbook.sheetnames,
            ["Resumo", "Totais por balanca", "Leituras por hora", "Totais por dia"],
        )
        resumo = workbook["Resumo"]
        self.assertEqual(resumo["A2"].value, "Arquivo: milhao_bla_20260301_a_20260302.xlsx")
        self.assertNotIn("Cliente", {str(resumo[f"A{line}"].value) for line in range(1, 9)})

        for sheet_name in workbook.sheetnames:
            ws = workbook[sheet_name]
            footer_text = str(ws.cell(row=ws.max_row, column=1).value or "").lower()
            self.assertIn("setbrasil.club", footer_text)

    def test_export_excel_rejects_interval_over_limit(self):
        self.client.force_login(self.user)
        response = self.client.post(
            reverse("app_milhao_bla_export_excel"),
            {"start_date": "2026-01-01", "end_date": "2026-05-01"},
        )
        self.assertEqual(response.status_code, 400)
        payload = response.json()
        self.assertFalse(payload["ok"])
        self.assertIn("Intervalo maximo", payload["error"])

    def test_export_excel_requires_app_permission(self):
        outsider = User.objects.create_user(username="outsider", password="123456", email="out@example.com")
        PerfilUsuario.objects.create(
            nome="Out",
            email="out@example.com",
            usuario=outsider,
            ativo=True,
        )
        self.client.force_login(outsider)
        response = self.client.post(
            reverse("app_milhao_bla_export_excel"),
            {"start_date": "2026-03-01", "end_date": "2026-03-02"},
        )
        self.assertEqual(response.status_code, 403)
