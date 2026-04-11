import json
from io import BytesIO
from unittest.mock import patch
from datetime import datetime, timedelta
from decimal import Decimal

from django.contrib.auth.models import User
from django.test import Client, SimpleTestCase, TestCase
from django.utils import timezone

from core.apps.app_rotas.views import _global_point_visual_flags, _route_point_visual_flags
from core.access_control import has_tipo_code, normalize_access_code
from core.models import (
    App,
    PerfilUsuario,
    IngestRecord,
    ModuloAcesso,
    Proposta,
    Radar,
    RadarAtividade,
    RadarAtividadeDiaExecucao,
    RadarColaborador,
    RadarClassificacao,
    RadarContrato,
    RadarID,
    RadarTrabalho,
    RadarTrabalhoColaborador,
    RadarTrabalhoObservacao,
    SystemConfiguration,
    TipoPerfil,
)
from core.views import _build_proposta_pdf_context, _build_radar_relatorio_pdf_context, _sanitize_proposta_descricao, _user_role


class AccessControlFoundationTests(TestCase):
    def test_system_types_are_seeded_with_stable_codes(self):
        self.assertTrue(TipoPerfil.objects.filter(codigo="DEV", sistema=True, ativo=True).exists())
        self.assertTrue(TipoPerfil.objects.filter(codigo="MASTER", sistema=True, ativo=True).exists())

    def test_access_modules_are_seeded_with_expected_base_data(self):
        modulo = ModuloAcesso.objects.get(codigo="APP_MILHAO_BLA")
        self.assertEqual(modulo.tipo, ModuloAcesso.Tipo.APP)
        self.assertTrue(modulo.ativo)

    def test_tipo_perfil_generates_code_from_name_when_missing(self):
        tipo = TipoPerfil.objects.create(nome="Radar Operacional")
        self.assertEqual(tipo.codigo, "RADAR_OPERACIONAL")

    def test_modulo_acesso_normalizes_code_on_save(self):
        modulo = ModuloAcesso.objects.create(codigo="", nome="Modulo Piloto")
        self.assertEqual(modulo.codigo, "MODULO_PILOTO")

    def test_tipo_code_helper_uses_stable_codes(self):
        user = User.objects.create_user(username="acl@set.local", email="acl@set.local", password="123456")
        perfil = PerfilUsuario.objects.create(nome="ACL", email="acl@set.local", usuario=user)
        tipo = TipoPerfil.objects.create(nome="Teste ACL", codigo="ACL_TESTE")
        perfil.tipos.add(tipo)
        self.assertEqual(normalize_access_code("acl teste"), "ACL_TESTE")
        self.assertTrue(has_tipo_code(user, "acl teste"))


class AccessControlAdminAndVisibilityTests(TestCase):
    def setUp(self):
        self.client_http = Client()
        self.tipo_dev = TipoPerfil.objects.get(codigo="DEV")
        self.tipo_financeiro = TipoPerfil.objects.get(codigo="FINANCEIRO")
        self.dev_user = User.objects.create_user(username="shadow-dev@set.local", email="shadow-dev@set.local", password="123456")
        self.dev_perfil = PerfilUsuario.objects.create(
            nome="Shadow Dev",
            email="shadow-dev@set.local",
            usuario=self.dev_user,
        )
        self.dev_perfil.tipos.add(self.tipo_dev)

        self.user = User.objects.create_user(username="cliente-shadow@set.local", email="cliente-shadow@set.local", password="123456")
        self.perfil = PerfilUsuario.objects.create(
            nome="Cliente Shadow",
            email="cliente-shadow@set.local",
            usuario=self.user,
        )

    def test_dev_can_open_module_access_management(self):
        self.client_http.force_login(self.dev_user)
        response = self.client_http.get("/modulos-acesso/")
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Modulos de acesso")
        self.assertNotContains(response, "Novo modulo")
        self.assertNotContains(response, "App vinculado")
        self.assertContains(response, "APP ID em `perfil.apps`", html=False)
        self.assertContains(response, "Os IDs atuais continuam apenas como escopo e compartilhamento interno entre usuarios.", html=False)
        self.assertNotContains(response, "Modos de autorizacao")

    def test_dev_can_update_module_access_management(self):
        modulo = ModuloAcesso.objects.get(codigo="FINANCEIRO")
        self.client_http.force_login(self.dev_user)
        response = self.client_http.post(
            "/modulos-acesso/",
            {
                "action": "update_module",
                "module_id": modulo.id,
                "tipos": [self.tipo_financeiro.id],
                "ativo": "on",
            },
        )
        self.assertEqual(response.status_code, 302)
        modulo.refresh_from_db()
        self.assertSetEqual(set(modulo.tipos.values_list("codigo", flat=True)), {"FINANCEIRO"})

    def test_internal_module_route_is_blocked_without_matching_type(self):
        modulo = ModuloAcesso.objects.get(codigo="FINANCEIRO")
        modulo.tipos.clear()

        self.client_http.force_login(self.user)
        response = self.client_http.get("/financeiro/")
        self.assertEqual(response.status_code, 403)

    def test_internal_module_card_stays_hidden_without_matching_type(self):
        modulo = ModuloAcesso.objects.get(codigo="FINANCEIRO")
        modulo.tipos.set([self.tipo_financeiro])
        self.client_http.force_login(self.user)
        response = self.client_http.get("/painel/")
        self.assertEqual(response.status_code, 200)
        self.assertNotContains(response, 'href="/financeiro/"')

    def test_internal_module_card_and_route_are_enabled_by_tipo(self):
        modulo = ModuloAcesso.objects.get(codigo="FINANCEIRO")
        modulo.tipos.set([self.tipo_financeiro])
        self.perfil.tipos.add(self.tipo_financeiro)
        self.client_http.force_login(self.user)
        painel_response = self.client_http.get("/painel/")
        self.assertEqual(painel_response.status_code, 200)
        self.assertContains(painel_response, 'href="/financeiro/"')
        route_response = self.client_http.get("/financeiro/")
        self.assertEqual(route_response.status_code, 200)

    def test_dedicated_apps_stay_reference_only_in_module_access_screen(self):
        self.client_http.force_login(self.dev_user)
        response = self.client_http.get("/modulos-acesso/")
        self.assertContains(response, "Apps dedicados")
        self.assertContains(response, "o acesso real continua sendo decidido exclusivamente pelo APP ID do usuario", html=False)

    def test_dedicated_apps_remain_visible_by_app_id_only(self):
        app = App.objects.create(nome="BLA", slug="bla", ativo=True)
        self.perfil.apps.add(app)
        self.client_http.force_login(self.user)
        response = self.client_http.get("/painel/")
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "BLA")
        self.assertNotContains(response, 'href="/financeiro/"')

    def test_dev_keeps_inactive_dedicated_app_visible_in_painel(self):
        app = App.objects.create(nome="BLA Oculto", slug="bla_oculto", ativo=False)
        self.dev_perfil.apps.add(app)
        self.client_http.force_login(self.dev_user)
        response = self.client_http.get("/painel/")
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "BLA Oculto")

    def test_dev_can_open_inactive_dedicated_app_home_when_assigned(self):
        app = App.objects.create(nome="BLA Oculto", slug="bla_oculto", ativo=False)
        self.dev_perfil.apps.add(app)
        self.client_http.force_login(self.dev_user)
        response = self.client_http.get("/apps/bla_oculto/")
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "BLA Oculto")


class DevAdminPrivilegesTests(TestCase):
    def setUp(self):
        self.client_http = Client()
        self.tipo_dev, _ = TipoPerfil.objects.get_or_create(nome="Dev")
        self.tipo_cliente, _ = TipoPerfil.objects.get_or_create(nome="Cliente")

        self.dev_user = User.objects.create_user(username="dev@set.local", email="dev@set.local", password="123456")
        self.dev_perfil = PerfilUsuario.objects.create(
            nome="Dev User",
            email="dev@set.local",
            usuario=self.dev_user,
        )
        self.dev_perfil.tipos.add(self.tipo_dev)

        self.user = User.objects.create_user(username="user@set.local", email="user@set.local", password="123456")
        self.perfil = PerfilUsuario.objects.create(
            nome="Regular User",
            email="user@set.local",
            usuario=self.user,
        )
        self.perfil.tipos.add(self.tipo_cliente)

    def test_dev_role_is_admin(self):
        self.assertEqual(_user_role(self.dev_user), "ADMIN")

    def test_dev_can_access_admin_logs(self):
        self.client_http.force_login(self.dev_user)
        response = self.client_http.get("/admin-logs/")
        self.assertEqual(response.status_code, 200)

    def test_dev_is_promoted_to_staff_by_middleware(self):
        self.assertFalse(User.objects.get(pk=self.dev_user.pk).is_staff)
        self.client_http.force_login(self.dev_user)
        response = self.client_http.get("/painel/")
        self.assertEqual(response.status_code, 200)
        self.dev_user.refresh_from_db()
        self.assertTrue(self.dev_user.is_staff)

    def test_non_dev_cannot_access_admin_logs(self):
        self.client_http.force_login(self.user)
        response = self.client_http.get("/admin-logs/")
        self.assertEqual(response.status_code, 403)


class MaintenanceModeTests(TestCase):
    def setUp(self):
        self.client_http = Client()
        self.tipo_dev = TipoPerfil.objects.get(codigo="DEV")
        self.tipo_cliente = TipoPerfil.objects.get(codigo="CLIENTE")

        self.dev_user = User.objects.create_user(username="maintenance-dev@set.local", email="maintenance-dev@set.local", password="123456")
        self.dev_perfil = PerfilUsuario.objects.create(
            nome="Maintenance Dev",
            email="maintenance-dev@set.local",
            usuario=self.dev_user,
        )
        self.dev_perfil.tipos.add(self.tipo_dev)

        self.user = User.objects.create_user(username="maintenance-user@set.local", email="maintenance-user@set.local", password="123456")
        self.perfil = PerfilUsuario.objects.create(
            nome="Maintenance User",
            email="maintenance-user@set.local",
            usuario=self.user,
        )
        self.perfil.tipos.add(self.tipo_cliente)

        self.config = SystemConfiguration.load()
        self.config.maintenance_mode_enabled = True
        self.config.maintenance_message = "Sistema temporariamente indisponivel."
        self.config.save()

    def test_non_dev_user_is_redirected_to_maintenance(self):
        self.client_http.force_login(self.user)
        response = self.client_http.get("/painel/")
        self.assertRedirects(response, "/manutencao/", fetch_redirect_response=False)

    def test_dev_user_bypasses_maintenance_mode(self):
        self.client_http.force_login(self.dev_user)
        response = self.client_http.get("/painel/")
        self.assertEqual(response.status_code, 200)

    def test_maintenance_page_renders_configured_message(self):
        self.client_http.force_login(self.user)
        response = self.client_http.get("/manutencao/")
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Sistema temporariamente indisponivel.")

    def test_logout_remains_accessible_during_maintenance(self):
        self.client_http.force_login(self.user)
        response = self.client_http.post("/logout/")
        self.assertEqual(response.status_code, 302)


class RouteTimelineStateTests(SimpleTestCase):
    def _timeline_points(self, start, count, step_minutes=5):
        points = []
        for idx in range(count):
            ts = start + timedelta(minutes=step_minutes * idx)
            points.append({"idx": idx, "timestamp": ts, "iso": ts.isoformat(), "label": ts.isoformat()})
        return points

    def test_route_visual_flags_follow_same_rule_as_status(self):
        tz = timezone.get_current_timezone()
        start = timezone.make_aware(datetime(2026, 1, 10, 8, 0, 0), tz)
        timeline = self._timeline_points(start, 4)
        events = [
            # Linha ainda nao deve ficar verde sem LIGADA.
            {"timestamp": timeline[1]["timestamp"], "atributo": "LIGAR", "valor": 1},
            # Linha ligada: deve ficar verde.
            {"timestamp": timeline[1]["timestamp"], "atributo": "LIGADA", "valor": 1},
            # Linha desligando: deve sair do verde mesmo que LIGADA ainda esteja 1.
            {"timestamp": timeline[2]["timestamp"], "atributo": "DESLIGAR", "valor": 1},
        ]
        flags = _route_point_visual_flags(
            day_events=events,
            timeline=timeline,
            available_until=timeline[-1]["timestamp"],
            baseline_attrs=None,
        )
        self.assertEqual(flags, [False, True, False, False])

    def test_global_visual_flags_use_same_play_on_semantics(self):
        tz = timezone.get_current_timezone()
        start = timezone.make_aware(datetime(2026, 1, 10, 9, 0, 0), tz)
        timeline = self._timeline_points(start, 3)
        seed_states = {
            "ENS01": {
                "attrs": {"LIGAR": 1, "DESLIGAR": 0, "LIGADA": 1, "ORIGEM": None, "DESTINO": None},
            }
        }
        events = [
            # Mesmo com LIGADA ainda 1, DESLIGAR deve apagar o verde global.
            {"prefixo": "ENS01", "timestamp": timeline[1]["timestamp"], "atributo": "DESLIGAR", "valor": 1},
        ]
        flags = _global_point_visual_flags(
            day_events=events,
            timeline=timeline,
            available_until=timeline[-1]["timestamp"],
            seed_states=seed_states,
        )
        self.assertEqual(flags, [True, False, False])


class IngestCleanupByDateTests(TestCase):
    def setUp(self):
        self.client_http = Client()
        self.tipo_dev, _ = TipoPerfil.objects.get_or_create(nome="Dev")
        self.dev_user = User.objects.create_user(username="ingest-dev@set.local", email="ingest-dev@set.local", password="123456")
        self.dev_perfil = PerfilUsuario.objects.create(
            nome="Ingest Dev",
            email="ingest-dev@set.local",
            usuario=self.dev_user,
        )
        self.dev_perfil.tipos.add(self.tipo_dev)

    def _create_ingest_record(self, source_id, created_at, client_id="CLIENTE-A", agent_id="AGENTE-A", source="SOURCE-A"):
        record = IngestRecord.objects.create(
            source_id=source_id,
            client_id=client_id,
            agent_id=agent_id,
            source=source,
            payload={"source_id": source_id},
        )
        IngestRecord.objects.filter(pk=record.pk).update(created_at=created_at, updated_at=created_at)
        record.refresh_from_db()
        return record

    def test_admin_can_delete_only_records_within_selected_created_at_range(self):
        tz = timezone.get_current_timezone()
        keep_before = self._create_ingest_record(
            "before-range",
            timezone.make_aware(datetime(2026, 3, 10, 9, 0, 0), tz),
        )
        remove_first = self._create_ingest_record(
            "inside-range-1",
            timezone.make_aware(datetime(2026, 3, 11, 10, 0, 0), tz),
        )
        remove_second = self._create_ingest_record(
            "inside-range-2",
            timezone.make_aware(datetime(2026, 3, 12, 11, 0, 0), tz),
        )
        keep_other_source = self._create_ingest_record(
            "other-source",
            timezone.make_aware(datetime(2026, 3, 11, 12, 0, 0), tz),
            source="SOURCE-B",
        )

        self.client_http.force_login(self.dev_user)
        response = self.client_http.post(
            "/ingest-gerenciar/limpar/",
            {
                "action": "delete_filtered_ingest",
                "client_id": "CLIENTE-A",
                "agent_id": "AGENTE-A",
                "source": "SOURCE-A",
                "data_inicial": "2026-03-11",
                "data_final": "2026-03-12",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "2 registro(s) removido(s)")
        self.assertTrue(IngestRecord.objects.filter(pk=keep_before.pk).exists())
        self.assertFalse(IngestRecord.objects.filter(pk=remove_first.pk).exists())
        self.assertFalse(IngestRecord.objects.filter(pk=remove_second.pk).exists())
        self.assertTrue(IngestRecord.objects.filter(pk=keep_other_source.pk).exists())

    def test_preview_can_show_only_records_that_match_selected_filters(self):
        tz = timezone.get_current_timezone()
        matching = self._create_ingest_record(
            "preview-match",
            timezone.make_aware(datetime(2026, 3, 11, 9, 0, 0), tz),
        )
        self._create_ingest_record(
            "preview-outside-date",
            timezone.make_aware(datetime(2026, 3, 15, 9, 0, 0), tz),
        )
        self._create_ingest_record(
            "preview-other-source",
            timezone.make_aware(datetime(2026, 3, 11, 11, 0, 0), tz),
            source="SOURCE-B",
        )

        self.client_http.force_login(self.dev_user)
        response = self.client_http.get(
            "/ingest-gerenciar/limpar/",
            {
                "client_id": "CLIENTE-A",
                "agent_id": "AGENTE-A",
                "source": "SOURCE-A",
                "data_inicial": "2026-03-11",
                "data_final": "2026-03-12",
                "preview": "1",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Pre-visualizacao da limpeza")
        self.assertContains(response, matching.source_id)
        self.assertNotContains(response, "preview-outside-date")
        self.assertNotContains(response, "preview-other-source")


class PropostaTrabalhoVinculoTests(TestCase):
    def setUp(self):
        self.client_http = Client()
        self.vendedor = User.objects.create_user(username="vend", email="vend@set.local", password="123456")
        self.destinatario_user = User.objects.create_user(
            username="cliente",
            email="cliente@set.local",
            password="123456",
        )
        self.destinatario = PerfilUsuario.objects.create(
            nome="Cliente",
            email="cliente@set.local",
            usuario=self.destinatario_user,
        )
        self.radar = Radar.objects.create(cliente=self.destinatario, nome="Radar Norte")
        self.classificacao = RadarClassificacao.objects.create(nome="Critica")
        self.contrato = RadarContrato.objects.create(nome="Contrato A")
        self.trabalho = RadarTrabalho.objects.create(
            radar=self.radar,
            classificacao=self.classificacao,
            contrato=self.contrato,
            nome="Trabalho TEC",
            descricao="Descricao tecnica inicial",
            setor="Manutencao",
            solicitante="Engenharia",
            responsavel="Paulo",
            criado_por=self.vendedor,
        )
        RadarTrabalhoColaborador.objects.create(trabalho=self.trabalho, nome="Ana")
        RadarTrabalhoColaborador.objects.create(trabalho=self.trabalho, nome="Bruno")
        RadarAtividade.objects.create(trabalho=self.trabalho, nome="Inspecao", descricao="Linha 1")
        self.proposta = Proposta.objects.create(
            cliente=self.destinatario,
            criada_por=self.vendedor,
            nome="Proposta 1",
            descricao="Escopo comercial.",
            trabalho=self.trabalho,
        )

    def test_proposta_vinculada_reflete_alteracoes_do_trabalho(self):
        self.trabalho.descricao = "Descricao tecnica atualizada"
        self.trabalho.save(update_fields=["descricao"])
        RadarAtividade.objects.create(trabalho=self.trabalho, nome="Comissionamento", descricao="Linha 2")
        proposta = Proposta.objects.select_related("trabalho", "trabalho__radar").prefetch_related("trabalho__atividades").get(
            pk=self.proposta.pk
        )
        context = _build_proposta_pdf_context(proposta, "Pendente", include_origem=True, trabalho=proposta.trabalho)
        labels = {row["label"]: row["value"] for row in context["origem_rows"]}
        self.assertEqual(labels.get("Descricao do trabalho"), "Descricao tecnica atualizada")
        atividades_nomes = [atividade["nome"] for atividade in context["atividades"]]
        self.assertIn("Comissionamento", atividades_nomes)

    def test_descricao_comercial_remove_bloco_tecnico_duplicado(self):
        raw = (
            "Origem tecnica\n"
            "Radar: Radar Norte\n"
            "Trabalho: Trabalho TEC\n"
            "Descricao do trabalho: xpto\n"
            "Resumo das atividades\n"
            "- item legado\n\n"
            "Somente condicoes comerciais."
        )
        clean = _sanitize_proposta_descricao(raw)
        self.assertNotIn("Origem tecnica", clean)
        self.assertNotIn("Radar:", clean)
        self.assertIn("Somente condicoes comerciais.", clean)

    def test_pdf_context_mostra_origem_tecnica_e_atividades_corretas(self):
        proposta = Proposta.objects.select_related(
            "trabalho",
            "trabalho__radar",
            "trabalho__classificacao",
            "trabalho__contrato",
        ).prefetch_related("trabalho__atividades").get(pk=self.proposta.pk)
        context = _build_proposta_pdf_context(proposta, "Pendente", include_origem=True, trabalho=proposta.trabalho)
        self.assertTrue(context["has_trabalho_vinculado"])
        self.assertFalse(context["trabalho_indisponivel"])
        labels = {row["label"]: row["value"] for row in context["origem_rows"]}
        self.assertEqual(labels.get("Radar"), "Radar Norte")
        self.assertEqual(labels.get("Trabalho"), "Trabalho TEC")
        self.assertEqual(labels.get("Colaboradores"), "Ana, Bruno")
        self.assertEqual(context["atividades"][0]["nome"], "Inspecao")

    def test_proposta_sem_vinculo_permanece_funcional(self):
        proposta = Proposta.objects.create(
            cliente=self.destinatario,
            criada_por=self.vendedor,
            nome="Proposta Avulsa",
            descricao="Conteudo comercial avulso.",
        )
        context = _build_proposta_pdf_context(proposta, "Pendente", include_origem=True)
        self.assertFalse(context["has_trabalho_vinculado"])
        self.assertEqual(context["origem_rows"], [])
        self.assertEqual(context["descricao_blocks"][0]["text"], "Conteudo comercial avulso.")

    def test_nao_autorizado_nao_cria_vinculo(self):
        outro = User.objects.create_user(username="outro", email="outro@set.local", password="123456")
        self.client_http.force_login(outro)
        response = self.client_http.post(
            "/propostas/nova/",
            {
                "email": self.destinatario.email,
                "nome": "Proposta sem permissao",
                "descricao": "Comercial",
                "trabalho_id": str(self.trabalho.id),
            },
        )
        self.assertEqual(response.status_code, 200)
        proposta = Proposta.objects.filter(nome="Proposta sem permissao").first()
        self.assertIsNone(proposta)


class RadarCreatorPermissionTests(TestCase):
    def setUp(self):
        self.client_http = Client()
        self.owner_user = User.objects.create_user(username="owner", email="owner@set.local", password="123456")
        self.viewer_user = User.objects.create_user(username="viewer", email="viewer@set.local", password="123456")
        self.owner = PerfilUsuario.objects.create(nome="Owner", email="owner@set.local", usuario=self.owner_user)
        self.viewer = PerfilUsuario.objects.create(nome="Viewer", email="viewer@set.local", usuario=self.viewer_user)
        self.radar_id = RadarID.objects.create(codigo="R-001")
        self.viewer.radares.add(self.radar_id)
        self.radar = Radar.objects.create(
            cliente=self.owner,
            id_radar=self.radar_id,
            nome="Radar Permissao",
            criador=self.owner_user,
        )
        self.trabalho = RadarTrabalho.objects.create(
            radar=self.radar,
            nome="Trabalho Permissao",
            criado_por=self.owner_user,
        )

    def test_usuario_com_id_radar_nao_altera_radar_trabalho_atividade(self):
        self.client_http.force_login(self.viewer_user)
        resp_radar = self.client_http.post(
            f"/radar-atividades/{self.radar.id}/",
            {"action": "create_trabalho", "nome": "Nao deve criar"},
        )
        self.assertEqual(resp_radar.status_code, 403)
        resp_trabalho = self.client_http.post(
            f"/radar-atividades/{self.radar.id}/trabalhos/{self.trabalho.id}/",
            {"action": "create_atividade", "nome": "Nao deve criar"},
        )
        self.assertEqual(resp_trabalho.status_code, 403)

    def test_criador_do_radar_pode_alterar(self):
        self.client_http.force_login(self.owner_user)
        resp = self.client_http.post(
            f"/radar-atividades/{self.radar.id}/",
            {"action": "create_trabalho", "nome": "Pode criar"},
        )
        self.assertEqual(resp.status_code, 302)
        self.assertTrue(RadarTrabalho.objects.filter(radar=self.radar, nome="Pode criar").exists())

    def test_create_trabalho_ajax_persiste_colaboradores(self):
        self.client_http.force_login(self.owner_user)
        response = self.client_http.post(
            f"/radar-atividades/{self.radar.id}/",
            {
                "action": "create_trabalho",
                "nome": "Trabalho Equipe",
                "colaboradores": "Ana, Bruno, ana",
            },
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["row"]["total_colaboradores"], 2)
        self.assertEqual(payload["row"]["total_horas"], "0.00")
        trabalho = RadarTrabalho.objects.get(radar=self.radar, nome="Trabalho Equipe")
        nomes = list(trabalho.colaboradores.order_by("nome").values_list("nome", flat=True))
        self.assertEqual(nomes, ["Ana", "Bruno"])

    def test_radar_detail_exibe_total_horas_por_trabalho(self):
        self.client_http.force_login(self.owner_user)
        atividade = RadarAtividade.objects.create(trabalho=self.trabalho, nome="Ativ Horas Lista")
        self.client_http.post(
            f"/radar-atividades/{self.radar.id}/trabalhos/{self.trabalho.id}/",
            {
                "action": "set_agenda_atividade",
                "atividade_id": str(atividade.id),
                "dias_execucao": json.dumps(["2026-03-01", "2026-03-02"]),
            },
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
        )

        response = self.client_http.get(f"/radar-atividades/{self.radar.id}/")
        self.assertEqual(response.status_code, 200)
        rows = response.context["trabalhos_table_data"]
        row = next(item for item in rows if item["id"] == self.trabalho.id)
        self.assertEqual(row["total_horas"], "16.00")

    def test_agenda_exibe_observacoes_do_dia_e_marcador_no_calendario(self):
        self.client_http.force_login(self.owner_user)
        atividade = RadarAtividade.objects.create(trabalho=self.trabalho, nome="Ativ Agenda Obs")
        RadarAtividadeDiaExecucao.objects.create(atividade=atividade, data_execucao="2026-03-10")
        RadarTrabalhoObservacao.objects.create(
            trabalho=self.trabalho,
            texto="Observacao no mesmo dia",
            data_observacao=datetime(2026, 3, 10).date(),
        )
        RadarTrabalhoObservacao.objects.create(
            trabalho=self.trabalho,
            texto="Observacao em outro dia",
            data_observacao=datetime(2026, 3, 11).date(),
        )

        response = self.client_http.get(f"/radar-atividades/{self.radar.id}/agenda/?dia=2026-03-10")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["daily_total_atividades"], 1)
        self.assertEqual(response.context["daily_total_observacoes"], 1)
        self.assertEqual(len(response.context["daily_groups"]), 1)
        group = response.context["daily_groups"][0]
        self.assertEqual(group["trabalho_id"], self.trabalho.id)
        self.assertEqual(group["total_atividades"], 1)
        self.assertEqual(group["total_observacoes"], 1)
        self.assertEqual(group["observacoes"][0]["texto"], "Observacao no mesmo dia")
        calendar_cells = [
            cell
            for week in response.context["calendar_weeks"]
            for cell in week
            if cell["iso"] == "2026-03-10"
        ]
        self.assertEqual(len(calendar_cells), 1)
        self.assertTrue(calendar_cells[0]["has_observation"])
        self.assertEqual(calendar_cells[0]["observation_count"], 1)

    def test_agenda_exibe_trabalho_com_apenas_observacao_no_dia(self):
        self.client_http.force_login(self.owner_user)
        RadarTrabalhoObservacao.objects.create(
            trabalho=self.trabalho,
            texto="Apenas observacao",
            data_observacao=datetime(2026, 3, 12).date(),
        )

        response = self.client_http.get(f"/radar-atividades/{self.radar.id}/agenda/?dia=2026-03-12")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["daily_total_atividades"], 0)
        self.assertEqual(response.context["daily_total_observacoes"], 1)
        self.assertEqual(len(response.context["daily_groups"]), 1)
        group = response.context["daily_groups"][0]
        self.assertEqual(group["trabalho_id"], self.trabalho.id)
        self.assertEqual(group["total_atividades"], 0)
        self.assertEqual(group["total_observacoes"], 1)
        self.assertEqual(group["observacoes"][0]["texto"], "Apenas observacao")

    def test_update_trabalho_sincroniza_colaboradores(self):
        self.client_http.force_login(self.owner_user)
        response = self.client_http.post(
            f"/radar-atividades/{self.radar.id}/trabalhos/{self.trabalho.id}/",
            {
                "action": "update_trabalho",
                "nome": self.trabalho.nome,
                "descricao": self.trabalho.descricao or "",
                "setor": self.trabalho.setor or "",
                "solicitante": self.trabalho.solicitante or "",
                "responsavel": self.trabalho.responsavel or "",
                "colaboradores": "Carlos, Diana, Carlos",
            },
        )
        self.assertEqual(response.status_code, 302)
        nomes = list(self.trabalho.colaboradores.order_by("nome").values_list("nome", flat=True))
        self.assertEqual(nomes, ["Carlos", "Diana"])

    def test_agenda_define_datas_e_status_permanece_manual(self):
        self.client_http.force_login(self.owner_user)
        create_resp = self.client_http.post(
            f"/radar-atividades/{self.radar.id}/trabalhos/{self.trabalho.id}/",
            {"action": "create_atividade", "nome": "Ativ X", "status": "EXECUTANDO"},
        )
        self.assertEqual(create_resp.status_code, 302)
        atividade = RadarAtividade.objects.get(trabalho=self.trabalho, nome="Ativ X")
        self.assertIsNone(atividade.inicio_execucao_em)
        self.assertIsNone(atividade.finalizada_em)

        agenda_resp = self.client_http.post(
            f"/radar-atividades/{self.radar.id}/trabalhos/{self.trabalho.id}/",
            {
                "action": "set_agenda_atividade",
                "atividade_id": str(atividade.id),
                "dias_execucao": json.dumps(["2026-03-05", "2026-03-07", "2026-03-06"]),
            },
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
        )
        self.assertEqual(agenda_resp.status_code, 200)
        self.assertEqual(agenda_resp.json()["agenda_total_dias"], 3)
        atividade.refresh_from_db()
        self.assertIsNotNone(atividade.inicio_execucao_em)
        self.assertIsNotNone(atividade.finalizada_em)
        self.assertEqual(atividade.inicio_execucao_em.date().isoformat(), "2026-03-05")
        self.assertEqual(atividade.finalizada_em.date().isoformat(), "2026-03-07")

        status_resp = self.client_http.post(
            f"/radar-atividades/{self.radar.id}/trabalhos/{self.trabalho.id}/",
            {
                "action": "quick_status_atividade",
                "atividade_id": str(atividade.id),
                "status": "FINALIZADA",
            },
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
        )
        self.assertEqual(status_resp.status_code, 200)
        atividade.refresh_from_db()
        self.assertEqual(atividade.inicio_execucao_em.date().isoformat(), "2026-03-05")
        self.assertEqual(atividade.finalizada_em.date().isoformat(), "2026-03-07")

        clear_resp = self.client_http.post(
            f"/radar-atividades/{self.radar.id}/trabalhos/{self.trabalho.id}/",
            {
                "action": "set_agenda_atividade",
                "atividade_id": str(atividade.id),
                "dias_execucao": "[]",
            },
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
        )
        self.assertEqual(clear_resp.status_code, 200)
        atividade.refresh_from_db()
        self.assertIsNone(atividade.inicio_execucao_em)
        self.assertIsNone(atividade.finalizada_em)
        self.assertFalse(RadarAtividadeDiaExecucao.objects.filter(atividade=atividade).exists())

    def test_set_agenda_atividade_retorna_erro_para_data_invalida(self):
        self.client_http.force_login(self.owner_user)
        atividade = RadarAtividade.objects.create(trabalho=self.trabalho, nome="Ativ Y")
        response = self.client_http.post(
            f"/radar-atividades/{self.radar.id}/trabalhos/{self.trabalho.id}/",
            {
                "action": "set_agenda_atividade",
                "atividade_id": str(atividade.id),
                "dias_execucao": json.dumps(["2026-02-30"]),
            },
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
        )
        self.assertEqual(response.status_code, 400)
        payload = response.json()
        self.assertFalse(payload["ok"])

    def test_horas_trabalho_e_calculada_por_agenda_x_horas_dia(self):
        self.client_http.force_login(self.owner_user)
        atividade = RadarAtividade.objects.create(trabalho=self.trabalho, nome="Ativ Horas")

        response = self.client_http.post(
            f"/radar-atividades/{self.radar.id}/trabalhos/{self.trabalho.id}/",
            {
                "action": "set_agenda_atividade",
                "atividade_id": str(atividade.id),
                "dias_execucao": json.dumps(["2026-03-01", "2026-03-02", "2026-03-03"]),
            },
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["horas_trabalho"], "24.00")

        atividade.refresh_from_db()
        self.assertEqual(atividade.horas_trabalho, Decimal("24.00"))

    def test_horas_trabalho_multiplica_quantidade_colaboradores(self):
        self.client_http.force_login(self.owner_user)
        RadarTrabalhoColaborador.objects.create(trabalho=self.trabalho, nome="Ana")
        RadarTrabalhoColaborador.objects.create(trabalho=self.trabalho, nome="Bruno")
        atividade = RadarAtividade.objects.create(trabalho=self.trabalho, nome="Ativ Equipe")

        response = self.client_http.post(
            f"/radar-atividades/{self.radar.id}/trabalhos/{self.trabalho.id}/",
            {
                "action": "set_agenda_atividade",
                "atividade_id": str(atividade.id),
                "dias_execucao": json.dumps(["2026-03-01", "2026-03-02", "2026-03-03"]),
            },
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["horas_trabalho"], "48.00")

        atividade.refresh_from_db()
        self.assertEqual(atividade.horas_trabalho, Decimal("48.00"))

    def test_create_atividade_herda_colaboradores_do_trabalho(self):
        self.client_http.force_login(self.owner_user)
        RadarTrabalhoColaborador.objects.create(trabalho=self.trabalho, nome="Ana")
        RadarTrabalhoColaborador.objects.create(trabalho=self.trabalho, nome="Bruno")

        response = self.client_http.post(
            f"/radar-atividades/{self.radar.id}/trabalhos/{self.trabalho.id}/",
            {"action": "create_atividade", "nome": "Ativ Herdada"},
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
        )
        self.assertEqual(response.status_code, 200)
        atividade = RadarAtividade.objects.get(trabalho=self.trabalho, nome="Ativ Herdada")
        nomes = list(atividade.colaboradores.order_by("nome").values_list("nome", flat=True))
        self.assertEqual(nomes, ["Ana", "Bruno"])

    def test_atualizar_horas_dia_recalcula_horas_de_todas_atividades(self):
        self.client_http.force_login(self.owner_user)
        atividade = RadarAtividade.objects.create(trabalho=self.trabalho, nome="Ativ Recalc")

        agenda_resp = self.client_http.post(
            f"/radar-atividades/{self.radar.id}/trabalhos/{self.trabalho.id}/",
            {
                "action": "set_agenda_atividade",
                "atividade_id": str(atividade.id),
                "dias_execucao": json.dumps(["2026-03-01", "2026-03-02"]),
            },
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
        )
        self.assertEqual(agenda_resp.status_code, 200)
        atividade.refresh_from_db()
        self.assertEqual(atividade.horas_trabalho, Decimal("16.00"))

        update_resp = self.client_http.post(
            f"/radar-atividades/{self.radar.id}/trabalhos/{self.trabalho.id}/",
            {
                "action": "update_trabalho",
                "nome": self.trabalho.nome,
                "descricao": self.trabalho.descricao or "",
                "setor": self.trabalho.setor or "",
                "solicitante": self.trabalho.solicitante or "",
                "responsavel": self.trabalho.responsavel or "",
                "horas_dia": "6",
                "colaboradores": "",
            },
        )
        self.assertEqual(update_resp.status_code, 302)
        atividade.refresh_from_db()
        self.assertEqual(atividade.horas_trabalho, Decimal("12.00"))

    def test_atualizar_colaboradores_do_trabalho_nao_muda_equipe_ja_herdada(self):
        self.client_http.force_login(self.owner_user)
        atividade = RadarAtividade.objects.create(trabalho=self.trabalho, nome="Ativ Recalc Equipe")

        agenda_resp = self.client_http.post(
            f"/radar-atividades/{self.radar.id}/trabalhos/{self.trabalho.id}/",
            {
                "action": "set_agenda_atividade",
                "atividade_id": str(atividade.id),
                "dias_execucao": json.dumps(["2026-03-01", "2026-03-02"]),
            },
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
        )
        self.assertEqual(agenda_resp.status_code, 200)
        atividade.refresh_from_db()
        self.assertEqual(atividade.horas_trabalho, Decimal("16.00"))

        update_resp = self.client_http.post(
            f"/radar-atividades/{self.radar.id}/trabalhos/{self.trabalho.id}/",
            {
                "action": "update_trabalho",
                "nome": self.trabalho.nome,
                "descricao": self.trabalho.descricao or "",
                "setor": self.trabalho.setor or "",
                "solicitante": self.trabalho.solicitante or "",
                "responsavel": self.trabalho.responsavel or "",
                "horas_dia": "8",
                "colaboradores": "Ana, Bruno",
            },
        )
        self.assertEqual(update_resp.status_code, 302)
        atividade.refresh_from_db()
        self.assertEqual(atividade.horas_trabalho, Decimal("16.00"))

    def test_update_atividade_recalcula_horas_com_colaboradores_da_atividade(self):
        self.client_http.force_login(self.owner_user)
        ana = RadarColaborador.objects.create(perfil=self.owner, nome="Ana")
        bruno = RadarColaborador.objects.create(perfil=self.owner, nome="Bruno")
        RadarTrabalhoColaborador.objects.create(trabalho=self.trabalho, nome="Ana", colaborador=ana)
        RadarTrabalhoColaborador.objects.create(trabalho=self.trabalho, nome="Bruno", colaborador=bruno)
        atividade = RadarAtividade.objects.create(trabalho=self.trabalho, nome="Ativ Eq Editavel")

        agenda_resp = self.client_http.post(
            f"/radar-atividades/{self.radar.id}/trabalhos/{self.trabalho.id}/",
            {
                "action": "set_agenda_atividade",
                "atividade_id": str(atividade.id),
                "dias_execucao": json.dumps(["2026-03-01", "2026-03-02"]),
            },
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
        )
        self.assertEqual(agenda_resp.status_code, 200)

        update_resp = self.client_http.post(
            f"/radar-atividades/{self.radar.id}/trabalhos/{self.trabalho.id}/",
            {
                "action": "update_atividade",
                "atividade_id": str(atividade.id),
                "nome": atividade.nome,
                "descricao": "",
                "status": "PENDENTE",
                "colaborador_ids": [str(ana.id), str(bruno.id)],
            },
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
        )
        self.assertEqual(update_resp.status_code, 200)
        atividade.refresh_from_db()
        self.assertEqual(atividade.horas_trabalho, Decimal("32.00"))
        self.assertEqual(
            list(atividade.colaboradores.order_by("nome").values_list("nome", flat=True)),
            ["Ana", "Bruno"],
        )

    def test_update_atividade_mantem_colaborador_ja_vinculado_mesmo_apos_remocao_do_trabalho(self):
        self.client_http.force_login(self.owner_user)
        ana = RadarColaborador.objects.create(perfil=self.owner, nome="Ana")
        RadarTrabalhoColaborador.objects.create(trabalho=self.trabalho, nome="Ana", colaborador=ana)
        atividade = RadarAtividade.objects.create(trabalho=self.trabalho, nome="Ativ Preserva Equipe")

        remove_resp = self.client_http.post(
            f"/radar-atividades/{self.radar.id}/trabalhos/{self.trabalho.id}/",
            {
                "action": "update_trabalho",
                "nome": self.trabalho.nome,
                "descricao": self.trabalho.descricao or "",
                "setor": self.trabalho.setor or "",
                "solicitante": self.trabalho.solicitante or "",
                "responsavel": self.trabalho.responsavel or "",
                "horas_dia": "8",
                "colaborador_ids": [],
                "colaboradores": "",
            },
        )
        self.assertEqual(remove_resp.status_code, 302)

        update_resp = self.client_http.post(
            f"/radar-atividades/{self.radar.id}/trabalhos/{self.trabalho.id}/",
            {
                "action": "update_atividade",
                "atividade_id": str(atividade.id),
                "nome": atividade.nome,
                "descricao": "",
                "status": "PENDENTE",
                "colaborador_ids": [str(ana.id)],
            },
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
        )
        self.assertEqual(update_resp.status_code, 200)
        self.assertEqual(
            list(atividade.colaboradores.order_by("nome").values_list("nome", flat=True)),
            ["Ana"],
        )

    def test_status_trabalho_nao_aceita_update_manual(self):
        self.client_http.force_login(self.owner_user)
        response = self.client_http.post(
            f"/radar-atividades/{self.radar.id}/",
            {
                "action": "quick_status_trabalho",
                "trabalho_id": str(self.trabalho.id),
                "status": "FINALIZADA",
            },
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
        )
        self.assertEqual(response.status_code, 400)
        payload = response.json()
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["status"], RadarTrabalho.Status.PENDENTE)
        self.assertIn("automatico", payload["message"].lower())

    def test_radar_trabalho_detail_recalcula_horas_stale_ao_carregar(self):
        self.client_http.force_login(self.owner_user)
        RadarTrabalhoColaborador.objects.create(trabalho=self.trabalho, nome="Ana")
        RadarTrabalhoColaborador.objects.create(trabalho=self.trabalho, nome="Bruno")
        atividade = RadarAtividade.objects.create(
            trabalho=self.trabalho,
            nome="Ativ stale",
            horas_trabalho=Decimal("0.00"),
        )
        RadarAtividadeDiaExecucao.objects.create(atividade=atividade, data_execucao="2026-03-01")
        RadarAtividadeDiaExecucao.objects.create(atividade=atividade, data_execucao="2026-03-02")

        response = self.client_http.get(
            f"/radar-atividades/{self.radar.id}/trabalhos/{self.trabalho.id}/"
        )
        self.assertEqual(response.status_code, 200)
        atividade.refresh_from_db()
        self.assertEqual(atividade.horas_trabalho, Decimal("32.00"))

    def test_export_relatorio_pdf_permite_apenas_criador(self):
        self.client_http.force_login(self.viewer_user)
        response = self.client_http.get(
            f"/radar-atividades/{self.radar.id}/relatorio/pdf/?mes=2026-03",
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
        )
        self.assertEqual(response.status_code, 403)

    def test_export_relatorio_pdf_bloqueia_sem_dados_no_mes(self):
        self.client_http.force_login(self.owner_user)
        response = self.client_http.get(
            f"/radar-atividades/{self.radar.id}/relatorio/pdf/?mes=2026-03",
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
        )
        self.assertEqual(response.status_code, 400)
        payload = response.json()
        self.assertFalse(payload["ok"])
        self.assertIn("Sem atividades executadas", payload["message"])

    def test_export_relatorio_pdf_gera_arquivo_com_nome_padrao(self):
        self.client_http.force_login(self.owner_user)
        atividade = RadarAtividade.objects.create(trabalho=self.trabalho, nome="Ativ Export")
        RadarAtividadeDiaExecucao.objects.create(atividade=atividade, data_execucao="2026-03-10")

        with patch("core.views._render_radar_relatorio_pdf", return_value=BytesIO(b"%PDF-1.4\nfake")):
            response = self.client_http.get(
                f"/radar-atividades/{self.radar.id}/relatorio/pdf/?mes=2026-03",
                HTTP_X_REQUESTED_WITH="XMLHttpRequest",
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response["Content-Type"], "application/pdf")
        self.assertIn('attachment; filename="relatorio_Radar_Permissao_2026-03.pdf"', response["Content-Disposition"])

    def test_criador_pode_criar_observacao_com_data_padrao(self):
        self.client_http.force_login(self.owner_user)
        response = self.client_http.post(
            f"/radar-atividades/{self.radar.id}/trabalhos/{self.trabalho.id}/",
            {
                "action": "create_observacao",
                "observacao_texto": "Primeira observacao",
            },
        )
        self.assertEqual(response.status_code, 302)
        observacao = RadarTrabalhoObservacao.objects.get(trabalho=self.trabalho)
        self.assertEqual(observacao.texto, "Primeira observacao")
        self.assertEqual(observacao.data_observacao, timezone.localdate())

    def test_viewer_nao_cria_observacao(self):
        self.client_http.force_login(self.viewer_user)
        response = self.client_http.post(
            f"/radar-atividades/{self.radar.id}/trabalhos/{self.trabalho.id}/",
            {
                "action": "create_observacao",
                "observacao_texto": "Nao pode",
            },
        )
        self.assertEqual(response.status_code, 403)
        self.assertFalse(RadarTrabalhoObservacao.objects.filter(trabalho=self.trabalho).exists())

    def test_criador_pode_editar_e_excluir_observacao(self):
        self.client_http.force_login(self.owner_user)
        observacao = RadarTrabalhoObservacao.objects.create(
            trabalho=self.trabalho,
            texto="Obs inicial",
            data_observacao=datetime(2026, 3, 1).date(),
        )
        update_response = self.client_http.post(
            f"/radar-atividades/{self.radar.id}/trabalhos/{self.trabalho.id}/",
            {
                "action": "update_observacao",
                "observacao_id": str(observacao.id),
                "observacao_texto": "Obs atualizada",
                "observacao_data": "2026-03-05",
            },
        )
        self.assertEqual(update_response.status_code, 302)
        observacao.refresh_from_db()
        self.assertEqual(observacao.texto, "Obs atualizada")
        self.assertEqual(observacao.data_observacao.isoformat(), "2026-03-05")

        delete_response = self.client_http.post(
            f"/radar-atividades/{self.radar.id}/trabalhos/{self.trabalho.id}/",
            {
                "action": "delete_observacao",
                "observacao_id": str(observacao.id),
            },
        )
        self.assertEqual(delete_response.status_code, 302)
        self.assertFalse(RadarTrabalhoObservacao.objects.filter(pk=observacao.id).exists())

    def test_contexto_pdf_do_radar_inclui_todas_observacoes_do_trabalho(self):
        self.client_http.force_login(self.owner_user)
        atividade = RadarAtividade.objects.create(trabalho=self.trabalho, nome="Ativ PDF Obs")
        RadarAtividadeDiaExecucao.objects.create(atividade=atividade, data_execucao="2026-03-10")
        RadarTrabalhoObservacao.objects.create(
            trabalho=self.trabalho,
            texto="Observacao antiga",
            data_observacao=datetime(2026, 1, 15).date(),
        )
        RadarTrabalhoObservacao.objects.create(
            trabalho=self.trabalho,
            texto="Observacao mais recente",
            data_observacao=datetime(2026, 4, 2).date(),
        )

        context = _build_radar_relatorio_pdf_context(
            self.radar,
            datetime(2026, 3, 1).date(),
            datetime(2026, 3, 31).date(),
        )
        trabalho_ctx = next(item for item in context["trabalho_pages"] if item["id"] == self.trabalho.id)
        self.assertEqual(len(trabalho_ctx["observacoes_resumo"]), 2)
        self.assertEqual(trabalho_ctx["observacoes_resumo"][0]["texto"], "Observacao mais recente")
        self.assertEqual(trabalho_ctx["observacoes_resumo"][1]["texto"], "Observacao antiga")
