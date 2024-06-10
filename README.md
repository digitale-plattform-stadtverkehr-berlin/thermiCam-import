# Import Daten der Verkehrsdetektion Wärmebildkameras

Service um Daten der Wärmebildkameras im Berliner Straßennetz in einem [FROST-Server](https://fraunhoferiosb.github.io/FROST-Server/) zu speichern.

Die Struktur des FROST-Servers eignet sich zur Darstellung im [Masterportal](https://bitbucket.org/geowerkstatt-hamburg/masterportal/src/dev/) mit dem GFI-Theme [SensorChart](https://github.com/digitale-plattform-stadtverkehr-berlin/masterportal-addon-sensor-chart)

## Parameter

Einige Zugangsdaten müssen über Systemvariablen konfiguriert werden.
Wenn der Service als Docker-Container läuft können diese als Umgebungsvariablen in Docker gesetzt werden.

* **FROST_SERVER** - Basis Url für den Frost-Server.
* **FROST_USER/FROST_PASSWORD** - Zugangsdaten für den Frost-Server.

## Docker Image bauen und in GitHub Registry pushen

```bash
> docker build -t docker.pkg.github.com/digitale-plattform-stadtverkehr-berlin/thermicam-import/thermicam-import:<TAG> .
> docker push docker.pkg.github.com/digitale-plattform-stadtverkehr-berlin/thermicam-import/thermicam-import:<TAG>
```
