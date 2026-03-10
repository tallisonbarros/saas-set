import json
from datetime import datetime, timedelta
from decimal import Decimal

from django.contrib.auth.models import User
from django.test import Client, SimpleTestCase, TestCase
from django.utils import timezone

from core.apps.app_rotas.views import _global_point_visual_flags, _route_point_visual_flags
from core.models import (
    PerfilUsuario,
    Proposta,
    Radar,
    RadarAtividade,
    RadarAtividadeDiaExecucao,
    RadarClassificacao,
    RadarContrato,
    RadarID,
    RadarTrabalho,
    RadarTrabalhoColaborador,
)
from core.views import _build_proposta_pdf_context, _sanitize_proposta_descricao


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

    def test_atualizar_colaboradores_recalcula_horas_de_todas_atividades(self):
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
        self.assertEqual(atividade.horas_trabalho, Decimal("32.00"))

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
