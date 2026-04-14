import re
from decimal import Decimal

from django.conf import settings
from django.contrib.auth.models import User
from django.core.validators import MaxValueValidator, MinValueValidator, RegexValidator
from django.db import models
from django.db.models import Max
from django.utils import timezone


def _normalize_access_code(value):
    cleaned = re.sub(r"[^0-9A-Za-z]+", "_", (value or "").strip().upper()).strip("_")
    return cleaned[:60]

class PerfilUsuario(models.Model):
    nome = models.CharField(max_length=120)
    email = models.EmailField()
    empresa = models.CharField(max_length=120, blank=True)
    sigla_cidade = models.CharField(max_length=3, blank=True)
    logo = models.ImageField(upload_to="clientes/logos/", blank=True, null=True)
    usuario = models.OneToOneField(User, on_delete=models.CASCADE)  # login do cliente
    ativo = models.BooleanField(default=True)
    tipos = models.ManyToManyField("TipoPerfil", blank=True, related_name="clientes")
    plantas = models.ManyToManyField("PlantaIO", blank=True, related_name="usuarios")
    financeiros = models.ManyToManyField("FinanceiroID", blank=True, related_name="usuarios")
    inventarios = models.ManyToManyField("InventarioID", blank=True, related_name="usuarios")
    listas_ip = models.ManyToManyField("ListaIPID", blank=True, related_name="usuarios")
    radares = models.ManyToManyField("RadarID", blank=True, related_name="usuarios")
    apps = models.ManyToManyField("App", blank=True, related_name="usuarios")

    def __str__(self):
        return self.nome


class TipoPerfil(models.Model):
    nome = models.CharField(max_length=50, unique=True)
    codigo = models.CharField(max_length=60, unique=True, blank=True)
    sistema = models.BooleanField(default=False)
    ativo = models.BooleanField(default=True)

    class Meta:
        ordering = ["nome"]

    def save(self, *args, **kwargs):
        if not self.codigo:
            self.codigo = _normalize_access_code(self.nome)
        super().save(*args, **kwargs)

    def __str__(self):
        return self.nome


class App(models.Model):
    slug = models.SlugField(max_length=60, unique=True)
    nome = models.CharField(max_length=120)
    descricao = models.TextField(blank=True)
    icon = models.CharField(max_length=80, blank=True)
    logo = models.ImageField(upload_to="apps/logos/", blank=True, null=True)
    theme_color = models.CharField(max_length=30, blank=True)
    ingest_client_id = models.CharField(max_length=120, blank=True, default="")
    ingest_agent_id = models.CharField(max_length=120, blank=True, default="")
    ingest_source = models.CharField(max_length=120, blank=True, default="")
    ativo = models.BooleanField(default=True)
    criado_em = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["nome"]

    def __str__(self):
        return self.nome


class ModuloAcesso(models.Model):
    class Tipo(models.TextChoices):
        CORE = "CORE", "Modulo interno"
        APP = "APP", "App dedicado"

    codigo = models.CharField(max_length=60, unique=True)
    nome = models.CharField(max_length=120)
    tipo = models.CharField(max_length=12, choices=Tipo.choices, default=Tipo.CORE)
    ativo = models.BooleanField(default=True)
    tipos = models.ManyToManyField("TipoPerfil", blank=True, related_name="modulos_acesso")
    criado_em = models.DateTimeField(auto_now_add=True)
    atualizado_em = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["nome"]

    def save(self, *args, **kwargs):
        self.codigo = _normalize_access_code(self.codigo or self.nome)
        super().save(*args, **kwargs)

    def __str__(self):
        return self.nome


class ProdutoPlataforma(models.Model):
    codigo = models.CharField(max_length=60, unique=True)
    nome = models.CharField(max_length=120)
    descricao = models.TextField(blank=True)
    ativo = models.BooleanField(default=True)
    criado_em = models.DateTimeField(auto_now_add=True)
    atualizado_em = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["nome"]

    def save(self, *args, **kwargs):
        self.codigo = _normalize_access_code(self.codigo or self.nome)
        super().save(*args, **kwargs)

    def __str__(self):
        return self.nome


class AcessoProdutoUsuario(models.Model):
    class Origem(models.TextChoices):
        TRIAL = "TRIAL", "Trial"
        MANUAL = "MANUAL", "Manual"
        INTERNO = "INTERNO", "Interno"

    class Status(models.TextChoices):
        TRIAL_ATIVO = "TRIAL_ATIVO", "Trial ativo"
        ATIVO = "ATIVO", "Ativo"
        EXPIRADO = "EXPIRADO", "Expirado"
        BLOQUEADO = "BLOQUEADO", "Bloqueado"

    usuario = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name="acessos_produto")
    produto = models.ForeignKey(ProdutoPlataforma, on_delete=models.CASCADE, related_name="acessos_usuario")
    origem = models.CharField(max_length=16, choices=Origem.choices, default=Origem.TRIAL)
    status = models.CharField(max_length=20, choices=Status.choices, default=Status.TRIAL_ATIVO)
    trial_inicio = models.DateTimeField(null=True, blank=True)
    trial_fim = models.DateTimeField(null=True, blank=True)
    acesso_inicio = models.DateTimeField(default=timezone.now)
    acesso_fim = models.DateTimeField(null=True, blank=True)
    observacao = models.TextField(blank=True)
    criado_em = models.DateTimeField(auto_now_add=True)
    atualizado_em = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["produto__nome", "usuario__username"]
        constraints = [
            models.UniqueConstraint(fields=["usuario", "produto"], name="unique_acesso_produto_usuario"),
        ]

    def __str__(self):
        return f"{self.usuario} - {self.produto} ({self.status})"


class PlanoComercial(models.Model):
    class Codigo(models.TextChoices):
        STARTER = "STARTER", "Starter"
        PROFESSIONAL = "PROFESSIONAL", "Professional"

    produto = models.ForeignKey(
        ProdutoPlataforma,
        on_delete=models.CASCADE,
        related_name="planos",
    )
    codigo = models.CharField(max_length=40, choices=Codigo.choices)
    nome = models.CharField(max_length=120)
    descricao = models.TextField(blank=True)
    ativo = models.BooleanField(default=True)
    is_free = models.BooleanField(default=False)
    ordem = models.PositiveSmallIntegerField(default=0)
    rack_limit_simultaneous = models.PositiveIntegerField(null=True, blank=True)
    daily_io_import_limit = models.PositiveIntegerField(null=True, blank=True)
    daily_ip_import_limit = models.PositiveIntegerField(null=True, blank=True)
    preco_mensal = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    preco_anual = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    provider_plan_code_mensal = models.CharField(max_length=120, blank=True, default="")
    provider_plan_code_anual = models.CharField(max_length=120, blank=True, default="")
    criado_em = models.DateTimeField(auto_now_add=True)
    atualizado_em = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["produto__nome", "ordem", "nome"]
        constraints = [
            models.UniqueConstraint(fields=["produto", "codigo"], name="unique_plano_comercial_produto_codigo"),
        ]

    def __str__(self):
        return f"{self.produto.nome} - {self.nome}"


class AssinaturaUsuario(models.Model):
    class Provider(models.TextChoices):
        INTERNAL = "INTERNAL", "Interno"
        MERCADO_PAGO = "MERCADO_PAGO", "Mercado Pago"

    class Status(models.TextChoices):
        PENDING = "PENDING", "Pendente"
        ACTIVE = "ACTIVE", "Ativa"
        PAST_DUE = "PAST_DUE", "Em atraso"
        CANCELED = "CANCELED", "Cancelada"
        EXPIRED = "EXPIRED", "Expirada"
        TRIALING = "TRIALING", "Em trial"

    class BillingInterval(models.TextChoices):
        MONTHLY = "MONTHLY", "Mensal"
        YEARLY = "YEARLY", "Anual"

    usuario = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="assinaturas",
    )
    produto = models.ForeignKey(
        ProdutoPlataforma,
        on_delete=models.CASCADE,
        related_name="assinaturas",
    )
    plano = models.ForeignKey(
        PlanoComercial,
        on_delete=models.PROTECT,
        related_name="assinaturas",
    )
    provider = models.CharField(max_length=20, choices=Provider.choices, default=Provider.INTERNAL)
    status = models.CharField(max_length=20, choices=Status.choices, default=Status.PENDING)
    billing_interval = models.CharField(
        max_length=20,
        choices=BillingInterval.choices,
        default=BillingInterval.MONTHLY,
    )
    auto_renew = models.BooleanField(default=True)
    preco_ciclo = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    moeda = models.CharField(max_length=12, blank=True, default="BRL")
    current_period_start = models.DateTimeField(null=True, blank=True)
    current_period_end = models.DateTimeField(null=True, blank=True)
    canceled_at = models.DateTimeField(null=True, blank=True)
    expires_at = models.DateTimeField(null=True, blank=True)
    provider_customer_id = models.CharField(max_length=120, blank=True, default="")
    provider_subscription_id = models.CharField(max_length=120, blank=True, default="")
    provider_plan_id = models.CharField(max_length=120, blank=True, default="")
    external_reference = models.CharField(max_length=120, blank=True, default="")
    checkout_url = models.TextField(blank=True, default="")
    observacao = models.TextField(blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-updated_at", "-created_at"]
        indexes = [
            models.Index(fields=["status", "updated_at"]),
            models.Index(fields=["provider", "provider_subscription_id"]),
        ]

    def __str__(self):
        return f"{self.usuario} - {self.plano.nome} ({self.status})"


class ConfiguracaoPagamento(models.Model):
    class Provider(models.TextChoices):
        MERCADO_PAGO = "MERCADO_PAGO", "Mercado Pago"

    trial_duration_days = models.PositiveIntegerField(default=30)
    trial_daily_io_import_limit = models.PositiveIntegerField(default=3)
    trial_daily_ip_import_limit = models.PositiveIntegerField(default=3)
    enabled = models.BooleanField(default=False)
    provider = models.CharField(max_length=20, choices=Provider.choices, default=Provider.MERCADO_PAGO)
    sandbox_mode = models.BooleanField(default=True)
    mercado_pago_public_key = models.CharField(max_length=255, blank=True, default="")
    mercado_pago_access_token = models.CharField(max_length=255, blank=True, default="")
    mercado_pago_webhook_secret = models.CharField(max_length=255, blank=True, default="")
    checkout_success_url = models.CharField(max_length=255, blank=True, default="")
    checkout_failure_url = models.CharField(max_length=255, blank=True, default="")
    checkout_pending_url = models.CharField(max_length=255, blank=True, default="")
    updated_at = models.DateTimeField(auto_now=True)
    updated_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="payment_configuration_updates",
    )

    class Meta:
        verbose_name = "Configuracao de pagamento"
        verbose_name_plural = "Configuracoes de pagamento"

    @classmethod
    def load(cls):
        config, _ = cls.objects.get_or_create(pk=1)
        return config

    @property
    def masked_access_token(self):
        value = (self.mercado_pago_access_token or "").strip()
        if len(value) <= 8:
            return "*" * len(value)
        return f"{value[:4]}...{value[-4:]}"

    @property
    def masked_public_key(self):
        value = (self.mercado_pago_public_key or "").strip()
        if len(value) <= 8:
            return "*" * len(value)
        return f"{value[:4]}...{value[-4:]}"

    def save(self, *args, **kwargs):
        self.pk = 1
        self.trial_duration_days = max(1, min(int(self.trial_duration_days or 30), 120))
        self.trial_daily_io_import_limit = max(1, min(int(self.trial_daily_io_import_limit or 3), 50))
        self.trial_daily_ip_import_limit = max(1, min(int(self.trial_daily_ip_import_limit or 3), 50))
        super().save(*args, **kwargs)

    def __str__(self):
        return "Configuracao de pagamento"


class EventoPagamentoWebhook(models.Model):
    provider = models.CharField(max_length=20, choices=ConfiguracaoPagamento.Provider.choices)
    external_id = models.CharField(max_length=120)
    event_type = models.CharField(max_length=120, blank=True, default="")
    raw_payload = models.JSONField(default=dict, blank=True)
    processed = models.BooleanField(default=False)
    processing_error = models.TextField(blank=True, default="")
    received_at = models.DateTimeField(auto_now_add=True)
    processed_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ["-received_at"]
        constraints = [
            models.UniqueConstraint(fields=["provider", "external_id"], name="unique_pagamento_webhook_event"),
        ]

    def __str__(self):
        return f"{self.provider} - {self.external_id}"


class ConsumoImportacaoDiaria(models.Model):
    class Modulo(models.TextChoices):
        IO = "IO", "IO"
        IP = "IP", "IP"

    usuario = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="consumos_importacao_diaria",
    )
    produto = models.ForeignKey(
        "ProdutoPlataforma",
        on_delete=models.CASCADE,
        related_name="consumos_importacao_diaria",
    )
    modulo = models.CharField(max_length=8, choices=Modulo.choices)
    referencia_data = models.DateField()
    importacoes_bem_sucedidas = models.PositiveIntegerField(default=0)
    criado_em = models.DateTimeField(auto_now_add=True)
    atualizado_em = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-referencia_data", "produto__nome", "modulo", "usuario__username"]
        constraints = [
            models.UniqueConstraint(
                fields=["usuario", "produto", "modulo", "referencia_data"],
                name="unique_consumo_importacao_diaria_usuario_produto_modulo_data",
            ),
        ]
        indexes = [
            models.Index(fields=["produto", "modulo", "referencia_data"]),
        ]

    def __str__(self):
        return f"{self.usuario} - {self.produto} - {self.modulo} ({self.referencia_data})"


class Caderno(models.Model):
    nome = models.CharField(max_length=80)
    criador = models.ForeignKey(
        "PerfilUsuario",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="cadernos_criados",
    )
    id_financeiro = models.ForeignKey(
        "FinanceiroID",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="cadernos",
    )
    ativo = models.BooleanField(default=True)
    criado_em = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return self.nome


class AppRotasMap(models.Model):
    class Tipo(models.TextChoices):
        ORIGEM = "ORIGEM", "ORIGEM"
        DESTINO = "DESTINO", "DESTINO"

    app = models.ForeignKey(App, on_delete=models.CASCADE, related_name="rotas_maps")
    tipo = models.CharField(max_length=20, choices=Tipo.choices)
    codigo = models.IntegerField()
    nome = models.CharField(max_length=120)
    ativo = models.BooleanField(default=True)
    criado_em = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["app_id", "tipo", "codigo"]
        constraints = [
            models.UniqueConstraint(fields=["app", "tipo", "codigo"], name="unique_app_rotas_map"),
        ]

    def __str__(self):
        return f"{self.app.slug} - {self.tipo} {self.codigo}: {self.nome}"


class AppRotaConfig(models.Model):
    app = models.ForeignKey(App, on_delete=models.CASCADE, related_name="rotas_configs")
    prefixo = models.CharField(max_length=80)
    nome_exibicao = models.CharField(max_length=120, blank=True, default="")
    ordem = models.IntegerField(default=0)
    ativo = models.BooleanField(default=True)
    criado_em = models.DateTimeField(auto_now_add=True)
    atualizado_em = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["app_id", "ordem", "prefixo"]
        constraints = [
            models.UniqueConstraint(fields=["app", "prefixo"], name="unique_app_rota_config"),
        ]

    def __str__(self):
        return f"{self.app.slug} - {self.prefixo}"


class AppMilhaoBlaMuralDia(models.Model):
    class Visibilidade(models.TextChoices):
        PUBLICA = "PUBLICA", "Publica"
        PRIVADA = "PRIVADA", "Privada"

    data_referencia = models.DateField(default=timezone.localdate, db_index=True)
    texto = models.TextField()
    visibilidade = models.CharField(
        max_length=12,
        choices=Visibilidade.choices,
        default=Visibilidade.PUBLICA,
    )
    autor = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="app_milhao_bla_mural_notas",
    )
    criado_em = models.DateTimeField(auto_now_add=True)
    atualizado_em = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-criado_em", "-id"]
        indexes = [
            models.Index(fields=["data_referencia", "criado_em"]),
            models.Index(fields=["autor", "data_referencia"]),
        ]

    def __str__(self):
        return f"{self.data_referencia.isoformat()} - {self.autor_id}"


class AppMilhaoBlaMuralDiaLeitura(models.Model):
    usuario = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="app_milhao_bla_mural_leituras",
    )
    data_referencia = models.DateField(db_index=True)
    visualizado_em = models.DateTimeField(default=timezone.now)

    class Meta:
        ordering = ["-visualizado_em", "-id"]
        constraints = [
            models.UniqueConstraint(
                fields=["usuario", "data_referencia"],
                name="unique_app_milhao_bla_mural_leitura_usuario_data",
            ),
        ]
        indexes = [
            models.Index(fields=["usuario", "data_referencia"]),
        ]

    def __str__(self):
        return f"{self.usuario_id} - {self.data_referencia.isoformat()}"


class CategoriaCompra(models.Model):
    nome = models.CharField(max_length=80, unique=True)

    def __str__(self):
        return self.nome


class TipoCompra(models.Model):
    nome = models.CharField(max_length=80, unique=True)

    def __str__(self):
        return self.nome


class CentroCusto(models.Model):
    nome = models.CharField(max_length=80, unique=True)

    def __str__(self):
        return self.nome


class StatusCompra(models.Model):
    nome = models.CharField(max_length=40, unique=True)
    ativo = models.BooleanField(default=True)

    def __str__(self):
        return self.nome


class Compra(models.Model):
    caderno = models.ForeignKey(Caderno, on_delete=models.CASCADE, related_name="compras", null=True, blank=True)
    nome = models.CharField(max_length=120, blank=True)
    descricao = models.TextField(blank=True)
    valor = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    data = models.DateField(null=True, blank=True)
    categoria = models.ForeignKey(CategoriaCompra, on_delete=models.PROTECT, null=True, blank=True)
    centro_custo = models.ForeignKey(CentroCusto, on_delete=models.PROTECT, null=True, blank=True)
    criado_em = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        base = self.nome or self.descricao or "Compra"
        return f"{base} - {self.valor}"


class CompraItem(models.Model):
    compra = models.ForeignKey(Compra, on_delete=models.CASCADE, related_name="itens")
    nome = models.CharField(max_length=120)
    valor = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    quantidade = models.PositiveIntegerField(default=1, validators=[MinValueValidator(1)])
    parcela = models.CharField(
        max_length=15,
        default="1/1",
        validators=[
            RegexValidator(
                regex=r"^\d{1,5}/\d{1,5}$|^1/-$",
                message="Parcela deve estar no formato 01/36 ou 1/-.",
            )
        ],
    )
    tipo = models.ForeignKey(TipoCompra, on_delete=models.PROTECT, null=True, blank=True)
    pago = models.BooleanField(default=False)

    class Meta:
        ordering = ["id"]

    def __str__(self):
        return self.nome


class Proposta(models.Model):
    cliente = models.ForeignKey("PerfilUsuario", on_delete=models.CASCADE, related_name="propostas")
    criada_por = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="propostas_criadas",
    )
    nome = models.CharField(max_length=120)
    codigo = models.CharField(max_length=40, blank=True)
    descricao = models.TextField()
    valor = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    prioridade = models.PositiveSmallIntegerField(
        default=50,
        validators=[MinValueValidator(1), MaxValueValidator(99)],
    )
    aprovada = models.BooleanField(null=True, blank=True)
    finalizada = models.BooleanField(default=False)
    finalizada_em = models.DateTimeField(null=True, blank=True)
    andamento = models.CharField(max_length=20, blank=True, default="")
    criado_em = models.DateTimeField(auto_now_add=True)
    decidido_em = models.DateTimeField(null=True, blank=True)
    aprovado_por = models.ForeignKey(
        User, on_delete=models.SET_NULL, null=True, blank=True, related_name="propostas_aprovadas"
    )
    observacao_cliente = models.TextField(blank=True)
    trabalho = models.ForeignKey(
        "RadarTrabalho",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="propostas_vinculadas",
    )

    def __str__(self):
        return f"{self.cliente.nome} - {self.valor} ({self.status})"

    def _prefixo_codigo(self):
        empresa = (self.cliente.empresa or self.cliente.nome or "").strip()
        sigla_empresa = (empresa[:3] if empresa else "XXX").upper()
        sigla_cidade = (self.cliente.sigla_cidade or "XXX").strip().upper()
        agora = timezone.localtime()
        mes = f"{agora.month:02d}"
        ano = f"{agora.year % 100:02d}"
        return f"Prop{sigla_empresa}{sigla_cidade}{mes}{ano}x"

    def _proximo_codigo(self):
        prefixo = self._prefixo_codigo()
        ultimo = (
            Proposta.objects.filter(codigo__startswith=prefixo)
            .order_by("-codigo")
            .values_list("codigo", flat=True)
            .first()
        )
        if ultimo:
            try:
                seq = int(ultimo.split("x")[-1])
            except (ValueError, IndexError):
                seq = 0
        else:
            seq = 0
        seq = min(seq + 1, 9999)
        return f"{prefixo}{seq:04d}"

    def save(self, *args, **kwargs):
        if not self.codigo:
            self.codigo = self._proximo_codigo()
        super().save(*args, **kwargs)

    @property
    def origem_trabalho(self):
        return self.trabalho

    @origem_trabalho.setter
    def origem_trabalho(self, value):
        self.trabalho = value


class PropostaAnexo(models.Model):
    class Tipo(models.TextChoices):
        NF = "NF", "NF"
        CONTRATO = "CONTRATO", "Contrato"
        PROPOSTA_FORMAL = "PROPOSTA_FORMAL", "Proposta formal"
        QUEBRA_CONTRATO = "QUEBRA_CONTRATO", "Quebra de contrato"
        PEDIDO_COMPRA = "PEDIDO_COMPRA", "Pedido de compra"
        ORDEM_SERVICO = "ORDEM_SERVICO", "Ordem de servico"
        OUTROS = "OUTROS", "Outros"

    proposta = models.ForeignKey(Proposta, on_delete=models.CASCADE, related_name="anexos")
    arquivo = models.FileField(upload_to="propostas/anexos/")
    tipo = models.CharField(max_length=30, choices=Tipo.choices, default=Tipo.OUTROS)
    criado_em = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-criado_em"]

    def __str__(self):
        return f"{self.proposta.codigo} - {self.get_tipo_display()}"


class TipoCanalIO(models.Model):
    nome = models.CharField(max_length=20, unique=True)
    ativo = models.BooleanField(default=True)

    class Meta:
        ordering = ["nome"]

    def __str__(self):
        return self.nome


class PlantaIO(models.Model):
    codigo = models.CharField(max_length=40, unique=True)

    class Meta:
        ordering = ["codigo"]

    def __str__(self):
        return self.codigo


class FinanceiroID(models.Model):
    codigo = models.CharField(max_length=40, unique=True)

    class Meta:
        ordering = ["codigo"]

    def __str__(self):
        return self.codigo


class InventarioID(models.Model):
    codigo = models.CharField(max_length=40, unique=True)

    class Meta:
        ordering = ["codigo"]

    def __str__(self):
        return self.codigo


class ListaIPID(models.Model):
    codigo = models.CharField(max_length=40, unique=True)

    class Meta:
        ordering = ["codigo"]

    def __str__(self):
        return self.codigo


class RadarID(models.Model):
    codigo = models.CharField(max_length=40, unique=True)

    class Meta:
        ordering = ["codigo"]

    def __str__(self):
        return self.codigo


class ListaIP(models.Model):
    cliente = models.ForeignKey("PerfilUsuario", on_delete=models.CASCADE, related_name="listas_ip_listas")
    id_listaip = models.ForeignKey(
        ListaIPID,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="listas",
    )
    nome = models.CharField(max_length=120)
    descricao = models.TextField(blank=True)
    faixa_inicio = models.GenericIPAddressField()
    faixa_fim = models.GenericIPAddressField()
    protocolo_padrao = models.CharField(max_length=30, blank=True)
    criado_em = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["nome"]

    def __str__(self):
        return self.nome


class ListaIPItem(models.Model):
    lista = models.ForeignKey(ListaIP, on_delete=models.CASCADE, related_name="ips")
    ip = models.GenericIPAddressField()
    nome_equipamento = models.CharField(max_length=120, blank=True)
    descricao = models.CharField(max_length=200, blank=True)
    mac = models.CharField(
        max_length=30,
        blank=True,
        validators=[
            RegexValidator(
                regex=r"^[0-9A-Fa-f]{2}([:-]?[0-9A-Fa-f]{2}){5}$",
                message="MAC deve estar no formato 00:11:22:33:44:55.",
            )
        ],
    )
    protocolo = models.CharField(max_length=30, blank=True)

    class Meta:
        ordering = ["ip"]
        constraints = [
            models.UniqueConstraint(fields=["lista", "ip"], name="unique_lista_ip"),
        ]

    def __str__(self):
        return f"{self.lista.nome} - {self.ip}"


class RadarContrato(models.Model):
    nome = models.CharField(max_length=120, unique=True)

    class Meta:
        ordering = ["nome"]

    def __str__(self):
        return self.nome


class RadarClassificacao(models.Model):
    nome = models.CharField(max_length=120, unique=True)

    class Meta:
        ordering = ["nome"]

    def __str__(self):
        return self.nome


class Radar(models.Model):
    cliente = models.ForeignKey("PerfilUsuario", on_delete=models.CASCADE, related_name="radares_cliente")
    id_radar = models.ForeignKey(
        RadarID,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="radares",
    )
    nome = models.CharField(max_length=120)
    descricao = models.TextField(blank=True)
    local = models.CharField(max_length=120, blank=True)
    criador = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="radares_criados",
    )
    criado_em = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["nome"]

    def __str__(self):
        return self.nome


class RadarTrabalho(models.Model):
    class Status(models.TextChoices):
        PENDENTE = "PENDENTE", "Pendente"
        EXECUTANDO = "EXECUTANDO", "Executando"
        FINALIZADA = "FINALIZADA", "Finalizada"

    radar = models.ForeignKey(Radar, on_delete=models.CASCADE, related_name="trabalhos")
    classificacao = models.ForeignKey(
        "RadarClassificacao",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="trabalhos",
    )
    contrato = models.ForeignKey(
        RadarContrato,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="trabalhos",
    )
    nome = models.CharField(max_length=120)
    descricao = models.TextField(blank=True)
    setor = models.CharField(max_length=120, blank=True)
    solicitante = models.CharField(max_length=120, blank=True)
    responsavel = models.CharField(max_length=120, blank=True)
    data_registro = models.DateField(default=timezone.localdate)
    horas_dia = models.DecimalField(max_digits=5, decimal_places=2, default=Decimal("8.00"))
    status = models.CharField(max_length=20, choices=Status.choices, default=Status.PENDENTE)
    ultimo_status_evento_em = models.DateTimeField(null=True, blank=True)
    criado_por = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="radar_trabalhos_criados",
    )
    criado_em = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-data_registro", "nome"]

    def __str__(self):
        return self.nome


class RadarColaborador(models.Model):
    perfil = models.ForeignKey(
        "PerfilUsuario",
        on_delete=models.CASCADE,
        related_name="radar_colaboradores",
    )
    nome = models.CharField(max_length=120)
    cargo = models.CharField(max_length=120, blank=True)
    atributos = models.JSONField(default=dict, blank=True)
    ativo = models.BooleanField(default=True)
    criado_em = models.DateTimeField(auto_now_add=True)
    atualizado_em = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["nome", "id"]
        constraints = [
            models.UniqueConstraint(
                fields=["perfil", "nome"],
                name="unique_radar_colaborador_por_perfil_nome",
            ),
        ]

    def __str__(self):
        return self.nome


class RadarTrabalhoColaborador(models.Model):
    trabalho = models.ForeignKey(RadarTrabalho, on_delete=models.CASCADE, related_name="colaboradores")
    colaborador = models.ForeignKey(
        RadarColaborador,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="trabalhos_vinculados",
    )
    nome = models.CharField(max_length=120)
    criado_em = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["nome", "id"]
        constraints = [
            models.UniqueConstraint(
                fields=["trabalho", "nome"],
                name="unique_radar_trabalho_colaborador",
            )
        ]

    def __str__(self):
        return self.nome


class RadarTrabalhoObservacao(models.Model):
    trabalho = models.ForeignKey(RadarTrabalho, on_delete=models.CASCADE, related_name="observacoes")
    texto = models.TextField()
    data_observacao = models.DateField(default=timezone.localdate)
    criado_em = models.DateTimeField(auto_now_add=True)
    atualizado_em = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-data_observacao", "-id"]

    def __str__(self):
        return f"{self.trabalho_id} - {self.data_observacao.isoformat()}"


class RadarAtividade(models.Model):
    class Status(models.TextChoices):
        PENDENTE = "PENDENTE", "Pendente"
        EXECUTANDO = "EXECUTANDO", "Executando"
        FINALIZADA = "FINALIZADA", "Finalizada"

    trabalho = models.ForeignKey(RadarTrabalho, on_delete=models.CASCADE, related_name="atividades")
    nome = models.CharField(max_length=120)
    descricao = models.TextField(blank=True)
    horas_trabalho = models.DecimalField(max_digits=8, decimal_places=2, null=True, blank=True)
    status = models.CharField(max_length=20, choices=Status.choices, default=Status.PENDENTE)
    inicio_execucao_em = models.DateTimeField(null=True, blank=True)
    finalizada_em = models.DateTimeField(null=True, blank=True)
    ordem = models.PositiveIntegerField(default=0, db_index=True)
    criado_em = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["ordem", "criado_em", "id"]

    def __str__(self):
        return self.nome

    def save(self, *args, **kwargs):
        is_new = self._state.adding
        if self.ordem <= 0 and self.trabalho_id:
            max_ordem = (
                RadarAtividade.objects.filter(trabalho_id=self.trabalho_id, status=self.status).aggregate(
                    max_ordem=Max("ordem")
                )["max_ordem"]
                or 0
            )
            self.ordem = max_ordem + 1
        super().save(*args, **kwargs)
        if is_new and self.trabalho_id:
            self.inherit_colaboradores_from_trabalho()

    def inherit_colaboradores_from_trabalho(self):
        if not self.trabalho_id:
            return 0
        existing_keys = set()
        for row in self.colaboradores.all():
            if row.colaborador_id:
                existing_keys.add(f"id:{row.colaborador_id}")
                continue
            nome_key = (row.nome or "").strip().casefold()
            if nome_key:
                existing_keys.add(f"nome:{nome_key}")

        to_create = []
        for row in self.trabalho.colaboradores.select_related("colaborador").all():
            nome = " ".join((row.nome or "").strip().split())[:120]
            if not nome:
                continue
            key = f"id:{row.colaborador_id}" if row.colaborador_id else f"nome:{nome.casefold()}"
            if key in existing_keys:
                continue
            existing_keys.add(key)
            to_create.append(
                RadarAtividadeColaborador(
                    atividade=self,
                    colaborador_id=row.colaborador_id,
                    nome=nome,
                )
            )
        if to_create:
            RadarAtividadeColaborador.objects.bulk_create(to_create, ignore_conflicts=True)
        return len(to_create)


class RadarAtividadeDiaExecucao(models.Model):
    atividade = models.ForeignKey(RadarAtividade, on_delete=models.CASCADE, related_name="dias_execucao")
    data_execucao = models.DateField()
    criado_em = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["data_execucao", "id"]
        constraints = [
            models.UniqueConstraint(
                fields=["atividade", "data_execucao"],
                name="unique_radar_atividade_dia_execucao",
            )
        ]

    def __str__(self):
        return f"{self.atividade_id} - {self.data_execucao.isoformat()}"


class RadarAtividadeColaborador(models.Model):
    atividade = models.ForeignKey(RadarAtividade, on_delete=models.CASCADE, related_name="colaboradores")
    colaborador = models.ForeignKey(
        RadarColaborador,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="atividades_vinculadas",
    )
    nome = models.CharField(max_length=120)
    criado_em = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["nome", "id"]
        constraints = [
            models.UniqueConstraint(
                fields=["atividade", "nome"],
                name="unique_radar_atividade_colaborador",
            )
        ]

    def __str__(self):
        return self.nome


class Inventario(models.Model):
    class TagsetPattern(models.TextChoices):
        TIPO_SEQ = "TIPO_SEQ", "Sequencial por tipo"
        SETORIZADO = "SETORIZADO", "Setorizado"
        INVENTARIO = "INVENTARIO", "Inventario + tipo"

    cliente = models.ForeignKey("PerfilUsuario", on_delete=models.CASCADE, related_name="inventarios_cliente")
    id_inventario = models.ForeignKey(
        InventarioID,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="inventarios",
    )
    tagset_pattern = models.CharField(
        max_length=20,
        choices=TagsetPattern.choices,
        default=TagsetPattern.TIPO_SEQ,
    )
    nome = models.CharField(max_length=120)
    descricao = models.TextField(blank=True)
    responsavel = models.CharField(max_length=120, blank=True)
    cidade = models.CharField(max_length=80, blank=True)
    estado = models.CharField(max_length=80, blank=True)
    pais = models.CharField(max_length=80, blank=True)
    criador = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="inventarios_criados",
    )
    criado_em = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["nome"]

    def __str__(self):
        return self.nome


class LocalRackIO(models.Model):
    cliente = models.ForeignKey(
        "PerfilUsuario",
        on_delete=models.CASCADE,
        related_name="locais_rack",
        null=True,
        blank=True,
    )
    nome = models.CharField(max_length=120)

    class Meta:
        ordering = ["nome"]
        constraints = [
            models.UniqueConstraint(fields=["cliente", "nome"], name="unique_localrackio_cliente_nome"),
        ]

    def __str__(self):
        return self.nome


class GrupoRackIO(models.Model):
    cliente = models.ForeignKey(
        "PerfilUsuario",
        on_delete=models.CASCADE,
        related_name="grupos_rack",
        null=True,
        blank=True,
    )
    nome = models.CharField(max_length=120)

    class Meta:
        ordering = ["nome"]
        constraints = [
            models.UniqueConstraint(fields=["cliente", "nome"], name="unique_gruporackio_cliente_nome"),
        ]

    def __str__(self):
        return self.nome


class TipoAtivo(models.Model):
    nome = models.CharField(max_length=80, unique=True)
    codigo = models.CharField(max_length=10, unique=True)
    ativo = models.BooleanField(default=True)
    criado_em = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["nome"]

    def __str__(self):
        return self.nome


class ModuloIO(models.Model):
    cliente = models.ForeignKey(
        "PerfilUsuario",
        on_delete=models.CASCADE,
        related_name="io_modulos",
        null=True,
        blank=True,
    )
    nome = models.CharField(max_length=120)
    modelo = models.CharField(max_length=80, blank=True)
    marca = models.CharField(max_length=80, blank=True)
    quantidade_canais = models.PositiveSmallIntegerField(
        validators=[MinValueValidator(1), MaxValueValidator(512)],
    )
    tipo_base = models.ForeignKey(TipoCanalIO, on_delete=models.PROTECT, related_name="modulos_base")
    is_default = models.BooleanField(default=False)
    criado_em = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["modelo", "id"]

    def __str__(self):
        return self.modelo or self.nome or f"Modulo {self.pk}"


class ModuloRackIO(models.Model):
    rack = models.ForeignKey("RackIO", on_delete=models.CASCADE, related_name="modulos")
    modulo_modelo = models.ForeignKey(ModuloIO, on_delete=models.PROTECT, related_name="instancias")
    nome = models.CharField(max_length=120, blank=True)
    criado_em = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["id"]

    def __str__(self):
        return self.modulo_modelo.modelo or self.modulo_modelo.nome or f"Modulo rack {self.pk}"


class CanalRackIO(models.Model):
    modulo = models.ForeignKey(ModuloRackIO, on_delete=models.CASCADE, related_name="canais")
    indice = models.PositiveSmallIntegerField()
    tag = models.CharField(max_length=120, blank=True)
    descricao = models.CharField(max_length=120, blank=True)
    tipo = models.ForeignKey(TipoCanalIO, on_delete=models.PROTECT, related_name="canais")
    comissionado = models.BooleanField(default=False)
    ativo = models.ForeignKey(
        "Ativo",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="canais_io",
    )
    ativo_item = models.ForeignKey(
        "AtivoItem",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="canais_io",
    )

    class Meta:
        ordering = ["indice"]
        constraints = [
            models.UniqueConstraint(fields=["modulo", "indice"], name="unique_modulo_rack_canal_indice"),
        ]

    def __str__(self):
        return f"{self.modulo} - {self.indice}"


class RackIO(models.Model):
    cliente = models.ForeignKey("PerfilUsuario", on_delete=models.CASCADE, related_name="io_racks")
    inventario = models.ForeignKey(
        "Inventario",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="racks_io",
    )
    local = models.ForeignKey(
        "LocalRackIO",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="racks",
    )
    grupo = models.ForeignKey(
        "GrupoRackIO",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="racks",
    )
    id_planta = models.ForeignKey(
        PlantaIO,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="racks",
    )
    nome = models.CharField(max_length=120)
    descricao = models.TextField(blank=True)
    slots_total = models.PositiveSmallIntegerField(
        validators=[MinValueValidator(1), MaxValueValidator(60)],
    )
    criado_em = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["nome"]

    def __str__(self):
        return self.nome


class Ativo(models.Model):
    inventario = models.ForeignKey(Inventario, on_delete=models.CASCADE, related_name="ativos")
    pai = models.ForeignKey(
        "self",
        on_delete=models.CASCADE,
        null=True,
        blank=True,
        related_name="subativos",
    )
    setor = models.CharField(max_length=120, blank=True)
    nome = models.CharField(max_length=120)
    tipo = models.ForeignKey(
        TipoAtivo,
        on_delete=models.PROTECT,
        null=True,
        blank=True,
        related_name="ativos",
    )
    identificacao = models.CharField(max_length=120, blank=True)
    tag_interna = models.CharField(max_length=120, blank=True)
    tag_set = models.CharField(max_length=120, blank=True)
    comissionado = models.BooleanField(default=False)
    comissionado_em = models.DateTimeField(null=True, blank=True)
    comissionado_por = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="ativos_comissionados",
    )
    em_manutencao = models.BooleanField(default=False)
    manutencao_em = models.DateTimeField(null=True, blank=True)
    manutencao_por = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="ativos_manutencao",
    )
    criado_em = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["nome"]

    def __str__(self):
        return self.nome


class AtivoItem(models.Model):
    ativo = models.ForeignKey(Ativo, on_delete=models.CASCADE, related_name="itens")
    nome = models.CharField(max_length=120)
    tipo = models.ForeignKey(
        TipoAtivo,
        on_delete=models.PROTECT,
        null=True,
        blank=True,
        related_name="itens",
    )
    identificacao = models.CharField(max_length=120, blank=True)
    tag_interna = models.CharField(max_length=120, blank=True)
    tag_set = models.CharField(max_length=120, blank=True)
    comissionado = models.BooleanField(default=False)
    comissionado_em = models.DateTimeField(null=True, blank=True)
    comissionado_por = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="ativo_itens_comissionados",
    )
    em_manutencao = models.BooleanField(default=False)
    manutencao_em = models.DateTimeField(null=True, blank=True)
    manutencao_por = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="ativo_itens_manutencao",
    )
    criado_em = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["nome"]

    def __str__(self):
        return self.nome




class RackSlotIO(models.Model):
    rack = models.ForeignKey(RackIO, on_delete=models.CASCADE, related_name="slots")
    posicao = models.PositiveSmallIntegerField()
    modulo = models.ForeignKey(
        ModuloRackIO,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="slots",
    )

    class Meta:
        ordering = ["posicao"]
        constraints = [
            models.UniqueConstraint(fields=["rack", "posicao"], name="unique_rack_slot_posicao"),
        ]

    def __str__(self):
        return f"{self.rack.nome} - S{self.posicao}"


class IngestRecord(models.Model):
    source_id = models.CharField(max_length=120, unique=True)
    client_id = models.CharField(max_length=120, blank=True)
    agent_id = models.CharField(max_length=120, blank=True)
    source = models.CharField(max_length=120, blank=True)
    payload = models.JSONField()
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True, null=True)

    class Meta:
        ordering = ["-created_at"]
        indexes = [
            models.Index(fields=["client_id", "agent_id", "source", "created_at"]),
        ]

    def __str__(self):
        return self.source_id


class IngestErrorLog(models.Model):
    source_id = models.CharField(max_length=120, blank=True)
    client_id = models.CharField(max_length=120, blank=True)
    agent_id = models.CharField(max_length=120, blank=True)
    source = models.CharField(max_length=120, blank=True)
    error = models.CharField(max_length=120)
    raw_payload = models.JSONField(null=True, blank=True)
    raw_body = models.TextField(blank=True)
    resolved = models.BooleanField(default=False)
    resolved_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-created_at"]
        indexes = [
            models.Index(fields=["source", "created_at"]),
            models.Index(fields=["source_id", "created_at"]),
        ]

    def __str__(self):
        return f"{self.error} - {self.source_id or self.source or 'ingest'}"


class IngestRule(models.Model):
    source = models.CharField(max_length=120, unique=True)
    required_fields = models.JSONField(default=list, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True, null=True)

    class Meta:
        ordering = ["source"]

    def __str__(self):
        return self.source


class AdminAccessLog(models.Model):
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="admin_access_logs",
    )
    module = models.CharField(max_length=120)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-created_at"]
        indexes = [
            models.Index(fields=["user", "created_at"]),
            models.Index(fields=["module", "created_at"]),
        ]

    def __str__(self):
        user_label = self.user.username if self.user else "anon"
        return f"{user_label} - {self.module}"


class SystemConfiguration(models.Model):
    DEFAULT_MAINTENANCE_MESSAGE = "O site esta em manutencao. Tente novamente em instantes."

    maintenance_mode_enabled = models.BooleanField(default=False)
    maintenance_message = models.TextField(blank=True, default=DEFAULT_MAINTENANCE_MESSAGE)
    updated_at = models.DateTimeField(auto_now=True)
    updated_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="system_configuration_updates",
    )

    class Meta:
        verbose_name = "Configuracao do sistema"
        verbose_name_plural = "Configuracoes do sistema"

    @classmethod
    def load(cls):
        config, _ = cls.objects.get_or_create(pk=1)
        return config

    def save(self, *args, **kwargs):
        self.pk = 1
        self.maintenance_message = (self.maintenance_message or "").strip() or self.DEFAULT_MAINTENANCE_MESSAGE
        super().save(*args, **kwargs)

    def __str__(self):
        return "Configuracoes do sistema"


class IOImportSettings(models.Model):
    class Provider(models.TextChoices):
        OPENAI = "OPENAI", "OpenAI"

    DEFAULT_API_BASE_URL = "https://api.openai.com/v1"
    DEFAULT_MODEL = "gpt-5-mini"

    enabled = models.BooleanField(default=False)
    provider = models.CharField(max_length=20, choices=Provider.choices, default=Provider.OPENAI)
    api_key = models.CharField(max_length=255, blank=True, default="")
    api_base_url = models.CharField(max_length=255, blank=True, default=DEFAULT_API_BASE_URL)
    model = models.CharField(max_length=120, blank=True, default=DEFAULT_MODEL)
    reasoning_effort = models.CharField(max_length=20, blank=True, default="medium")
    max_rows_for_ai = models.PositiveIntegerField(default=150)
    header_prompt = models.TextField(blank=True, default="")
    grouping_prompt = models.TextField(blank=True, default="")
    updated_at = models.DateTimeField(auto_now=True)
    updated_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="io_import_settings_updates",
    )

    class Meta:
        verbose_name = "Configuracao de importacao IO"
        verbose_name_plural = "Configuracoes de importacao IO"

    @classmethod
    def load(cls):
        config, _ = cls.objects.get_or_create(pk=1)
        return config

    @property
    def masked_api_key(self):
        value = (self.api_key or "").strip()
        if len(value) <= 8:
            return "*" * len(value)
        return f"{value[:4]}...{value[-4:]}"

    def save(self, *args, **kwargs):
        self.pk = 1
        self.api_base_url = (self.api_base_url or "").strip() or self.DEFAULT_API_BASE_URL
        self.model = (self.model or "").strip() or self.DEFAULT_MODEL
        self.reasoning_effort = (self.reasoning_effort or "").strip() or "medium"
        super().save(*args, **kwargs)

    def __str__(self):
        return "Configuracao de importacao IO"


class IOImportJob(models.Model):
    class Status(models.TextChoices):
        UPLOADED = "UPLOADED", "Upload recebido"
        REVIEW = "REVIEW", "Pronto para revisao"
        APPLIED = "APPLIED", "Aplicado"
        FAILED = "FAILED", "Falhou"

    class Mode(models.TextChoices):
        CREATE_RACK = "CREATE_RACK", "Criar rack novo"
        MERGE_RACK = "MERGE_RACK", "Preencher rack existente"

    class FileFormat(models.TextChoices):
        XLSX = "xlsx", "Excel"
        CSV = "csv", "CSV"
        TSV = "tsv", "TSV"
        UNKNOWN = "unknown", "Desconhecido"

    class AIStatus(models.TextChoices):
        SKIPPED = "SKIPPED", "Nao executado"
        SUCCESS = "SUCCESS", "Concluido"
        FAILED = "FAILED", "Falhou"

    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="io_import_jobs",
    )
    cliente = models.ForeignKey(
        "PerfilUsuario",
        on_delete=models.CASCADE,
        related_name="io_import_jobs",
        null=True,
        blank=True,
    )
    target_rack = models.ForeignKey(
        "RackIO",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="import_jobs_target",
    )
    applied_rack = models.ForeignKey(
        "RackIO",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="import_jobs_applied",
    )
    requested_local = models.ForeignKey(
        "LocalRackIO",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="io_import_jobs",
    )
    requested_grupo = models.ForeignKey(
        "GrupoRackIO",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="io_import_jobs",
    )
    requested_inventario = models.ForeignKey(
        "Inventario",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="io_import_jobs",
    )
    requested_rack_name = models.CharField(max_length=120, blank=True, default="")
    requested_planta_code = models.CharField(max_length=40, blank=True, default="")
    mode = models.CharField(max_length=20, choices=Mode.choices, default=Mode.CREATE_RACK)
    status = models.CharField(max_length=20, choices=Status.choices, default=Status.UPLOADED)
    file_format = models.CharField(max_length=20, choices=FileFormat.choices, default=FileFormat.UNKNOWN)
    source_file = models.FileField(upload_to="io/imports/")
    original_filename = models.CharField(max_length=255)
    file_sha256 = models.CharField(max_length=64, blank=True, default="")
    sheet_name = models.CharField(max_length=120, blank=True, default="")
    header_row_index = models.PositiveIntegerField(null=True, blank=True)
    rows_total = models.PositiveIntegerField(default=0)
    rows_parsed = models.PositiveIntegerField(default=0)
    ai_status = models.CharField(max_length=20, choices=AIStatus.choices, default=AIStatus.SKIPPED)
    ai_model = models.CharField(max_length=120, blank=True, default="")
    ai_error = models.TextField(blank=True, default="")
    column_map = models.JSONField(default=dict, blank=True)
    extracted_payload = models.JSONField(default=dict, blank=True)
    proposal_payload = models.JSONField(default=dict, blank=True)
    ai_payload = models.JSONField(default=dict, blank=True)
    progress_payload = models.JSONField(default=dict, blank=True)
    warnings = models.JSONField(default=list, blank=True)
    apply_log = models.JSONField(default=dict, blank=True)
    first_applied_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-created_at"]
        indexes = [
            models.Index(fields=["status", "created_at"]),
            models.Index(fields=["ai_status", "created_at"]),
            models.Index(fields=["created_by", "first_applied_at"]),
        ]

    def __str__(self):
        return f"{self.original_filename} - {self.get_status_display()}"


class IOImportAICache(models.Model):
    class Stage(models.TextChoices):
        WORKBOOK = "WORKBOOK", "Analise do workbook"
        SHEET = "SHEET", "Analise da guia"

    stage = models.CharField(max_length=20, choices=Stage.choices)
    fingerprint = models.CharField(max_length=64)
    file_sha256 = models.CharField(max_length=64, blank=True, default="")
    sheet_name = models.CharField(max_length=120, blank=True, default="")
    provider = models.CharField(max_length=20, blank=True, default="")
    model = models.CharField(max_length=120, blank=True, default="")
    settings_fingerprint = models.CharField(max_length=64, blank=True, default="")
    response_payload = models.JSONField(default=dict, blank=True)
    payload_meta = models.JSONField(default=dict, blank=True)
    hits = models.PositiveIntegerField(default=0)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    last_used_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-updated_at"]
        constraints = [
            models.UniqueConstraint(fields=["stage", "fingerprint"], name="unique_io_import_ai_cache_stage_fingerprint"),
        ]
        indexes = [
            models.Index(fields=["stage", "file_sha256", "updated_at"], name="core_ioimpo_stage_5a6484_idx"),
            models.Index(fields=["last_used_at"], name="core_ioimpo_last_us_8a5d4a_idx"),
        ]

    def __str__(self):
        label = self.sheet_name or self.file_sha256 or self.fingerprint[:12]
        return f"{self.stage} - {label}"


class IPImportSettings(models.Model):
    class Provider(models.TextChoices):
        OPENAI = "OPENAI", "OpenAI"

    DEFAULT_API_BASE_URL = "https://api.openai.com/v1"
    DEFAULT_MODEL = "gpt-5-mini"

    enabled = models.BooleanField(default=False)
    provider = models.CharField(max_length=20, choices=Provider.choices, default=Provider.OPENAI)
    api_key = models.CharField(max_length=255, blank=True, default="")
    api_base_url = models.CharField(max_length=255, blank=True, default=DEFAULT_API_BASE_URL)
    model = models.CharField(max_length=120, blank=True, default=DEFAULT_MODEL)
    reasoning_effort = models.CharField(max_length=20, blank=True, default="medium")
    max_rows_for_ai = models.PositiveIntegerField(default=180)
    header_prompt = models.TextField(blank=True, default="")
    grouping_prompt = models.TextField(blank=True, default="")
    updated_at = models.DateTimeField(auto_now=True)
    updated_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="ip_import_settings_updates",
    )

    class Meta:
        verbose_name = "Configuracao de importacao IP"
        verbose_name_plural = "Configuracoes de importacao IP"

    @classmethod
    def load(cls):
        config, _ = cls.objects.get_or_create(pk=1)
        return config

    @property
    def masked_api_key(self):
        value = (self.api_key or "").strip()
        if len(value) <= 8:
            return "*" * len(value)
        return f"{value[:4]}...{value[-4:]}"

    def save(self, *args, **kwargs):
        self.pk = 1
        self.api_base_url = (self.api_base_url or "").strip() or self.DEFAULT_API_BASE_URL
        self.model = (self.model or "").strip() or self.DEFAULT_MODEL
        self.reasoning_effort = (self.reasoning_effort or "").strip() or "medium"
        super().save(*args, **kwargs)

    def __str__(self):
        return "Configuracao de importacao IP"


class IPImportJob(models.Model):
    class Status(models.TextChoices):
        UPLOADED = "UPLOADED", "Upload recebido"
        REVIEW = "REVIEW", "Pronto para revisao"
        APPLIED = "APPLIED", "Aplicado"
        FAILED = "FAILED", "Falhou"

    class FileFormat(models.TextChoices):
        XLSX = "xlsx", "Excel"
        CSV = "csv", "CSV"
        TSV = "tsv", "TSV"
        UNKNOWN = "unknown", "Desconhecido"

    class AIStatus(models.TextChoices):
        SKIPPED = "SKIPPED", "Nao executado"
        SUCCESS = "SUCCESS", "Concluido"
        FAILED = "FAILED", "Falhou"

    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="ip_import_jobs",
    )
    cliente = models.ForeignKey(
        "PerfilUsuario",
        on_delete=models.CASCADE,
        related_name="ip_import_jobs",
        null=True,
        blank=True,
    )
    applied_lista = models.ForeignKey(
        "ListaIP",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="import_jobs_applied",
    )
    status = models.CharField(max_length=20, choices=Status.choices, default=Status.UPLOADED)
    file_format = models.CharField(max_length=20, choices=FileFormat.choices, default=FileFormat.UNKNOWN)
    source_file = models.FileField(upload_to="ip/imports/")
    original_filename = models.CharField(max_length=255)
    file_sha256 = models.CharField(max_length=64, blank=True, default="")
    sheet_name = models.CharField(max_length=120, blank=True, default="")
    header_row_index = models.PositiveIntegerField(null=True, blank=True)
    rows_total = models.PositiveIntegerField(default=0)
    rows_parsed = models.PositiveIntegerField(default=0)
    ai_status = models.CharField(max_length=20, choices=AIStatus.choices, default=AIStatus.SKIPPED)
    ai_model = models.CharField(max_length=120, blank=True, default="")
    ai_error = models.TextField(blank=True, default="")
    column_map = models.JSONField(default=dict, blank=True)
    extracted_payload = models.JSONField(default=dict, blank=True)
    proposal_payload = models.JSONField(default=dict, blank=True)
    ai_payload = models.JSONField(default=dict, blank=True)
    progress_payload = models.JSONField(default=dict, blank=True)
    warnings = models.JSONField(default=list, blank=True)
    apply_log = models.JSONField(default=dict, blank=True)
    first_applied_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-created_at"]
        indexes = [
            models.Index(fields=["status", "created_at"]),
            models.Index(fields=["ai_status", "created_at"]),
            models.Index(fields=["created_by", "first_applied_at"]),
        ]

    def __str__(self):
        return f"{self.original_filename} - {self.get_status_display()}"


class IPImportAICache(models.Model):
    class Stage(models.TextChoices):
        WORKBOOK = "WORKBOOK", "Analise do workbook"
        SHEET = "SHEET", "Analise da guia"

    stage = models.CharField(max_length=20, choices=Stage.choices)
    fingerprint = models.CharField(max_length=64)
    file_sha256 = models.CharField(max_length=64, blank=True, default="")
    sheet_name = models.CharField(max_length=120, blank=True, default="")
    provider = models.CharField(max_length=20, blank=True, default="")
    model = models.CharField(max_length=120, blank=True, default="")
    settings_fingerprint = models.CharField(max_length=64, blank=True, default="")
    response_payload = models.JSONField(default=dict, blank=True)
    payload_meta = models.JSONField(default=dict, blank=True)
    hits = models.PositiveIntegerField(default=0)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    last_used_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-updated_at"]
        constraints = [
            models.UniqueConstraint(fields=["stage", "fingerprint"], name="unique_ip_import_ai_cache_stage_fingerprint"),
        ]
        indexes = [
            models.Index(fields=["stage", "file_sha256", "updated_at"]),
            models.Index(fields=["last_used_at"]),
        ]

    def __str__(self):
        label = self.sheet_name or self.file_sha256 or self.fingerprint[:12]
        return f"{self.stage} - {label}"
