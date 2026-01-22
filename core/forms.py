from django import forms
from django.contrib.auth.models import User

from .models import PerfilUsuario, TipoPerfil


class PerfilUsuarioAdminForm(forms.ModelForm):
    senha_inicial = forms.CharField(
        label="Senha inicial",
        required=False,
        widget=forms.PasswordInput(render_value=True),
    )

    class Meta:
        model = PerfilUsuario
        fields = [
            "nome",
            "empresa",
            "sigla_cidade",
            "email",
            "logo",
            "ativo",
            "tipos",
            "plantas",
            "financeiros",
            "inventarios",
            "listas_ip",
        ]

    def clean_email(self):
        email = self.cleaned_data["email"].strip().lower()
        existing = User.objects.filter(username=email)
        if self.instance.pk:
            existing = existing.exclude(pk=self.instance.usuario_id)
        if existing.exists():
            raise forms.ValidationError("Email ja usado por outro usuario.")
        return email

    def save(self, commit=True):
        cliente = super().save(commit=False)
        senha_inicial = self.cleaned_data.get("senha_inicial")
        email = self.cleaned_data.get("email")

        if cliente.usuario_id:
            usuario = cliente.usuario
        else:
            usuario = User(username=email, email=email, is_active=True)
        usuario.username = email
        usuario.email = email
        if senha_inicial:
            usuario.set_password(senha_inicial)
        elif not usuario.pk:
            usuario.set_unusable_password()
        usuario.save()
        cliente.usuario = usuario
        if commit:
            cliente.save()
            self.save_m2m()
        return cliente


class UserCreateForm(forms.Form):
    username = forms.EmailField(label="Email")
    password = forms.CharField(label="Senha", widget=forms.PasswordInput)
    is_staff = forms.BooleanField(label="Administrador", required=False)

    def clean_username(self):
        username = self.cleaned_data["username"].strip().lower()
        if User.objects.filter(username=username).exists():
            raise forms.ValidationError("Email ja cadastrado.")
        return username

    def save(self):
        username = self.cleaned_data["username"]
        password = self.cleaned_data["password"]
        is_staff = self.cleaned_data["is_staff"]
        user = User.objects.create_user(
            username=username,
            email=username,
            password=password,
        )
        user.is_staff = is_staff
        user.save(update_fields=["is_staff"])
        return user


class TipoPerfilCreateForm(forms.Form):
    nome = forms.CharField(label="Nome do tipo", max_length=50)

    def clean_nome(self):
        nome = self.cleaned_data["nome"].strip()
        if TipoPerfil.objects.filter(nome__iexact=nome).exists():
            raise forms.ValidationError("Tipo ja existe.")
        return nome

    def save(self):
        nome = self.cleaned_data["nome"].strip()
        return TipoPerfil.objects.create(nome=nome)


class RegisterForm(forms.Form):
    nome = forms.CharField(label="Nome", max_length=120)
    email = forms.EmailField(label="Email")
    empresa = forms.CharField(label="Empresa", max_length=120, required=False)
    senha = forms.CharField(label="Senha", widget=forms.PasswordInput)
    senha_confirmacao = forms.CharField(label="Confirmar senha", widget=forms.PasswordInput)

    def clean_email(self):
        email = self.cleaned_data["email"].strip().lower()
        if User.objects.filter(username=email).exists():
            raise forms.ValidationError("Email ja cadastrado.")
        return email

    def clean(self):
        cleaned = super().clean()
        senha = cleaned.get("senha")
        senha_confirmacao = cleaned.get("senha_confirmacao")
        if senha and senha_confirmacao and senha != senha_confirmacao:
            self.add_error("senha_confirmacao", "As senhas nao conferem.")
        return cleaned

    def save(self):
        email = self.cleaned_data["email"].strip().lower()
        senha = self.cleaned_data["senha"]
        user = User.objects.create_user(
            username=email,
            email=email,
            password=senha,
        )
        return user
