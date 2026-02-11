from django.contrib.auth.models import User
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from core.models import App, AppRotasMap, IngestRecord, PerfilUsuario


class AppRotasTests(TestCase):
    def setUp(self):
        self.app = App.objects.create(
            slug="approtas",
            nome="Rotas",
            ativo=True,
            ingest_client_id="UBS3-UN1",
            ingest_agent_id="VMSCADA",
            ingest_source="ROTA",
        )
        self.user = User.objects.create_user(username="operador", password="123456", email="operador@example.com")
        self.perfil = PerfilUsuario.objects.create(
            nome="Operador",
            email="operador@example.com",
            usuario=self.user,
            ativo=True,
        )

    def test_dashboard_forbidden_without_app_permission(self):
        self.client.force_login(self.user)
        response = self.client.get(reverse("app_rotas_dashboard"))
        self.assertEqual(response.status_code, 403)

    def test_dashboard_renders_dynamic_route_with_destin_typo(self):
        self.perfil.apps.add(self.app)
        AppRotasMap.objects.create(app=self.app, tipo="ORIGEM", codigo=1, nome="Silo 1", ativo=True)
        AppRotasMap.objects.create(app=self.app, tipo="DESTINO", codigo=5, nome="Linha 5", ativo=True)
        now_iso = timezone.now().isoformat()
        IngestRecord.objects.create(
            source_id="rotas-1",
            client_id="UBS3-UN1",
            agent_id="VMSCADA",
            source="ROTA",
            payload={"Name": "ENS01_ORIGEM", "TimestampUtc": now_iso, "Value": "1"},
        )
        IngestRecord.objects.create(
            source_id="rotas-2",
            client_id="UBS3-UN1",
            agent_id="VMSCADA",
            source="ROTA",
            payload={"Name": "ENS01_DESTIN", "TimestampUtc": now_iso, "Value": "5"},
        )
        IngestRecord.objects.create(
            source_id="rotas-3",
            client_id="UBS3-UN1",
            agent_id="VMSCADA",
            source="ROTA",
            payload={"Name": "ENS01_LIGAR", "TimestampUtc": now_iso, "Value": "1"},
        )
        IngestRecord.objects.create(
            source_id="rotas-4",
            client_id="UBS3-UN1",
            agent_id="VMSCADA",
            source="ROTA",
            payload={"Name": "ENS01_LIGADA", "TimestampUtc": now_iso, "Value": "1"},
        )

        self.client.force_login(self.user)
        response = self.client.get(reverse("app_rotas_dashboard"))
        self.assertEqual(response.status_code, 200)
        cards = response.context["cards"]
        self.assertEqual(len(cards), 1)
        self.assertEqual(cards[0]["prefixo"], "ENS01")
        self.assertEqual(cards[0]["origem_display"], "Silo 1")
        self.assertEqual(cards[0]["destino_display"], "Linha 5")
        self.assertTrue(cards[0]["play_on"])

    def test_apps_gerenciar_requires_ingest_for_approtas(self):
        staff = User.objects.create_user(username="admin", password="123456", is_staff=True)
        self.client.force_login(staff)
        response = self.client.post(
            reverse("apps_gerenciar"),
            data={
                "action": "create_app",
                "nome": "Rotas 2",
                "slug": "approtas",
                "descricao": "",
                "icon": "",
                "theme_color": "",
                "ingest_client_id": "",
                "ingest_agent_id": "",
                "ingest_source": "ROTA",
            },
        )
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "informe client_id e agent_id", status_code=200)

    def test_dashboard_search_accepts_comma_separated_terms(self):
        self.perfil.apps.add(self.app)
        now_iso = timezone.now().isoformat()
        IngestRecord.objects.create(
            source_id="rotas-s-1",
            client_id="UBS3-UN1",
            agent_id="VMSCADA",
            source="ROTA",
            payload={"Name": "BEN_ORIGEM", "TimestampUtc": now_iso, "Value": "1"},
        )
        IngestRecord.objects.create(
            source_id="rotas-s-2",
            client_id="UBS3-UN1",
            agent_id="VMSCADA",
            source="ROTA",
            payload={"Name": "BEN_DESTINO", "TimestampUtc": now_iso, "Value": "2"},
        )

        self.client.force_login(self.user)
        response = self.client.get(reverse("app_rotas_dashboard"), {"busca": "XPT01, BEN, SILO A", "mostrar_inativas": "1"})
        self.assertEqual(response.status_code, 200)
        cards = response.context["cards"]
        self.assertEqual(len(cards), 1)
        self.assertEqual(cards[0]["prefixo"], "BEN")

    def test_dashboard_recent_events_is_paginated_with_10_items(self):
        self.perfil.apps.add(self.app)
        for idx in range(15):
            IngestRecord.objects.create(
                source_id=f"rotas-p-{idx}",
                client_id="UBS3-UN1",
                agent_id="VMSCADA",
                source="ROTA",
                payload={
                    "Name": f"SEC01_ORIGEM",
                    "TimestampUtc": timezone.now().isoformat(),
                    "Value": str(idx),
                },
            )
        self.client.force_login(self.user)
        response_page1 = self.client.get(reverse("app_rotas_dashboard"))
        self.assertEqual(response_page1.status_code, 200)
        self.assertEqual(len(response_page1.context["eventos_recentes"]), 10)
        self.assertEqual(response_page1.context["recent_events_page"].number, 1)

        response_page2 = self.client.get(reverse("app_rotas_dashboard"), {"events_page": "2"})
        self.assertEqual(response_page2.status_code, 200)
        self.assertEqual(len(response_page2.context["eventos_recentes"]), 5)
        self.assertEqual(response_page2.context["recent_events_page"].number, 2)
