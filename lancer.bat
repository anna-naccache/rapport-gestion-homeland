@echo off
chcp 65001 > nul
title Rapport de Gestion - Homeland

echo.
echo ╔══════════════════════════════════════════╗
echo ║     RAPPORT DE GESTION - HOMELAND        ║
echo ╚══════════════════════════════════════════╝
echo.

:: Vérifie si Python est installé
python --version > nul 2>&1
IF ERRORLEVEL 1 (
    echo ❌ Python n'est pas installé.
    echo    Téléchargez-le sur https://www.python.org/downloads/
    pause
    exit /b 1
)

:: Va dans le dossier du script
cd /d "%~dp0"

:: Installe les dépendances si besoin
echo 📦 Vérification des dépendances...
python -m pip install requests flask flask-cors --quiet --disable-pip-version-check

echo.
echo 🚀 Démarrage du serveur de données...
echo    (Le tableau de bord s'ouvrira dans votre navigateur)
echo.

:: Lance le serveur Flask en arrière-plan et ouvre le rapport
start "" python server.py

:: Attendre que le serveur démarre
timeout /t 2 > nul

:: Ouvre le rapport dans le navigateur
start "" "%~dp0rapport_v3.html"

echo.
echo ✅ Serveur démarré et rapport ouvert !
echo.
echo    📊 Tableau de bord : rapport_v2.html
echo    🔌 Serveur API     : http://localhost:5055
echo.
echo    Pour arrêter le serveur, fermez la fenêtre "server.py"
echo    ou cherchez le processus Python dans le Gestionnaire des tâches.
echo.
pause
