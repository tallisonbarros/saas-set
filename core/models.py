from django.db import models
from django.contrib.auth.models import User

class Cliente(models.Model):
    nome = models.CharField(max_length=120)
    email = models.EmailField()
    logo = models.ImageField(upload_to="logos/", blank=True, null=True)
    usuario = models.OneToOneField(User, on_delete=models.CASCADE)  # login do cliente
    ativo = models.BooleanField(default=True)

    def __str__(self):
        return self.nome


class Proposta(models.Model):
    class Status(models.TextChoices):
        PENDENTE = "PENDENTE", "Pendente"
        APROVADA = "APROVADA", "Aprovada"
        REPROVADA = "REPROVADA", "Reprovada"

    cliente = models.ForeignKey(Cliente, on_delete=models.CASCADE, related_name="propostas")
    nome = models.CharField(max_length=120)
    codigo = models.CharField(max_length=40)
    descricao = models.TextField()
    valor = models.DecimalField(max_digits=12, decimal_places=2)
    status = models.CharField(max_length=10, choices=Status.choices, default=Status.PENDENTE)
    criado_em = models.DateTimeField(auto_now_add=True)
    decidido_em = models.DateTimeField(null=True, blank=True)
    aprovado_por = models.ForeignKey(
        User, on_delete=models.SET_NULL, null=True, blank=True, related_name="propostas_aprovadas"
    )
    observacao_cliente = models.TextField(blank=True)

    def __str__(self):
        return f"{self.cliente.nome} - {self.valor} ({self.status})"
