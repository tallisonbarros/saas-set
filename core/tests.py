from datetime import datetime, timedelta

from django.contrib.auth.models import User
from django.test import Client, SimpleTestCase, TestCase
from django.utils import timezone

from core.apps.app_rotas.views import _global_point_visual_flags, _route_point_visual_flags
from core.models import PerfilUsuario, Proposta, Radar, RadarAtividade, RadarClassificacao, RadarContrato, RadarTrabalho
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
