from django.db import models
from django.conf import settings
from django.contrib.auth.models import User
from django.core.validators import MaxValueValidator, MinValueValidator, RegexValidator
from django.utils import timezone

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
    origem_trabalho = models.ForeignKey(
        "RadarTrabalho",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="propostas_origem",
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
    status = models.CharField(max_length=20, choices=Status.choices, default=Status.PENDENTE)
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
    criado_em = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-criado_em"]

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
        ordering = ["nome"]

    def __str__(self):
        return self.nome


class ModuloRackIO(models.Model):
    rack = models.ForeignKey("RackIO", on_delete=models.CASCADE, related_name="modulos")
    modulo_modelo = models.ForeignKey(ModuloIO, on_delete=models.PROTECT, related_name="instancias")
    nome = models.CharField(max_length=120, blank=True)
    criado_em = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["id"]

    def __str__(self):
        return self.nome or self.modulo_modelo.nome


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
