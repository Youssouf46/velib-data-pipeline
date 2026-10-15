# Image de base : Linux léger avec Python 3.12
FROM python:3.12-slim

# Ne pas créer de fichiers .pyc et afficher les logs immédiatement
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Dossier de travail dans l'image
WORKDIR /app

# Sécurité : ne pas exécuter le programme en tant qu'administrateur (root)
RUN useradd --create-home appuser

# 1) D'abord les dépendances (changent rarement → mises en cache)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 2) Ensuite le code (change souvent)
COPY producer.py .

# Exécuter le programme avec l'utilisateur non-root
USER appuser

# Commande lancée au démarrage du conteneur
CMD ["python", "producer.py"]
