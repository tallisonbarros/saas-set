from django.db import models
from django.contrib.auth.models import User
from django.core.validators import MaxValueValidator, MinValueValidator
from django.utils import timezone

class Cliente(models.Model):
    nome = models.CharField(max_length=120)
    email = models.EmailField()
    empresa = models.CharField(max_length=120, blank=True)
    sigla_cidade = models.CharField(max_length=3, blank=True)
    logo = models.ImageField(upload_to="clientes/logos/", blank=True, null=True)
    usuario = models.OneToOneField(User, on_delete=models.CASCADE)  # login do cliente
    ativo = models.BooleanField(default=True)
    tipos = models.ManyToManyField("TipoPerfil", blank=True, related_name="clientes")

    def __str__(self):
        return self.nome


class TipoPerfil(models.Model):
    nome = models.CharField(max_length=50, unique=True)

    def __str__(self):
        return self.nome


class Caderno(models.Model):
    nome = models.CharField(max_length=80)
    cliente = models.ForeignKey(Cliente, on_delete=models.CASCADE, related_name="cadernos")
    ativo = models.BooleanField(default=True)

    def __str__(self):
        return self.nome


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
    caderno = models.ForeignKey(Caderno, on_delete=models.CASCADE, related_name="compras")
    descricao = models.TextField()
    valor = models.DecimalField(max_digits=12, decimal_places=2)
    data = models.DateField()
    categoria = models.ForeignKey(CategoriaCompra, on_delete=models.PROTECT)
    tipo = models.ForeignKey(TipoCompra, on_delete=models.PROTECT)
    centro_custo = models.ForeignKey(CentroCusto, on_delete=models.PROTECT)
    status = models.ForeignKey(StatusCompra, on_delete=models.PROTECT)
    criado_em = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.descricao} - {self.valor}"


class Proposta(models.Model):
    class Status(models.TextChoices):
        PENDENTE = "PENDENTE", "Pendente"
        APROVADA = "APROVADA", "Aprovada"
        REPROVADA = "REPROVADA", "Reprovada"
        LEVANTAMENTO = "LEVANTAMENTO", "Levantamento"
        EXECUTANDO = "EXECUTANDO", "Executando"
        FINALIZADO = "FINALIZADO", "Finalizado"

    cliente = models.ForeignKey(Cliente, on_delete=models.CASCADE, related_name="propostas")
    nome = models.CharField(max_length=120)
    codigo = models.CharField(max_length=40, blank=True)
    descricao = models.TextField()
    valor = models.DecimalField(max_digits=12, decimal_places=2)
    prioridade = models.PositiveSmallIntegerField(
        default=50,
        validators=[MinValueValidator(1), MaxValueValidator(99)],
    )
    status = models.CharField(max_length=15, choices=Status.choices, default=Status.PENDENTE)
    criado_em = models.DateTimeField(auto_now_add=True)
    decidido_em = models.DateTimeField(null=True, blank=True)
    aprovado_por = models.ForeignKey(
        User, on_delete=models.SET_NULL, null=True, blank=True, related_name="propostas_aprovadas"
    )
    observacao_cliente = models.TextField(blank=True)

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
