#!/bin/bash

# Installer le cronjob
crontab /app/cronjob

# Démarrer cron
cron

# Garder le conteneur actif et voir les logs
touch /var/log/cron.log
tail -f /var/log/cron.log
