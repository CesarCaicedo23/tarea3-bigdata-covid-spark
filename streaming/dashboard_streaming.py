"""
Tarea 3 - Big Data (UNAD) | Dashboard web en tiempo real (solo librería estándar)

Sirve una página que se actualiza cada 2 segundos con el archivo JSON que escribe
spark_streaming_covid.py. Se abre desde el navegador del computador anfitrión:

    python3 dashboard_streaming.py --archivo salida_streaming/estado.json --puerto 8050
    -> http://<IP-de-la-VM>:8050      (ejemplo del anexo: http://192.168.1.7:8050)

Nota: la página carga Chart.js desde un CDN, por lo que el navegador necesita internet.
"""
import argparse
import http.server
import os

HTML = """<!doctype html>
<html lang="es"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>COVID-19 Colombia · Streaming en tiempo real</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<style>
 body{font-family:system-ui,Segoe UI,Arial,sans-serif;margin:0;background:#f4f6f8;color:#1c2733}
 header{background:#0b5394;color:#fff;padding:14px 24px;display:flex;justify-content:space-between;align-items:center}
 header h1{font-size:20px;margin:0} #estado{font-size:13px;opacity:.9}
 .kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:12px;padding:16px 24px 0}
 .kpi{background:#fff;border-radius:10px;padding:14px;box-shadow:0 1px 3px #0002}
 .kpi small{color:#5b6b7a;display:block} .kpi b{font-size:26px}
 .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(420px,1fr));gap:16px;padding:16px 24px}
 .card{background:#fff;border-radius:10px;padding:14px;box-shadow:0 1px 3px #0002}
 .card h2{font-size:15px;margin:0 0 8px} canvas{max-height:320px}
 #aviso{padding:30px;text-align:center;color:#5b6b7a}
</style></head><body>
<header><h1>Casos de COVID-19 en Colombia · Spark Streaming + Kafka</h1><div id="estado">Esperando datos…</div></header>
<div id="aviso">Aún no hay datos. Inicie el productor y el consumidor de Spark.</div>
<div id="contenido" style="display:none">
 <div class="kpis">
  <div class="kpi"><small>Casos recibidos</small><b id="k1">0</b></div>
  <div class="kpi"><small>Fallecidos</small><b id="k2">0</b></div>
  <div class="kpi"><small>Letalidad</small><b id="k3">0%</b></div>
  <div class="kpi"><small>Edad promedio</small><b id="k4">-</b></div>
  <div class="kpi"><small>Departamentos</small><b id="k5">0</b></div>
 </div>
 <div class="grid">
  <div class="card"><h2>Casos por departamento (top 15)</h2><canvas id="c1"></canvas></div>
  <div class="card"><h2>Casos por ventana de tiempo</h2><canvas id="c2"></canvas></div>
  <div class="card"><h2>Casos por grupo de edad</h2><canvas id="c3"></canvas></div>
  <div class="card"><h2>Casos por sexo</h2><canvas id="c4"></canvas></div>
 </div>
</div>
<script>
const ch = {};
function dibujar(id, tipo, datos, opc) {
  if (!window.Chart) return;
  if (ch[id]) { ch[id].data = datos; ch[id].update('none'); return; }
  ch[id] = new Chart(document.getElementById(id), {type: tipo, data: datos,
    options: Object.assign({animation: false, responsive: true}, opc || {})});
}
async function actualizar() {
  try {
    const r = await fetch('data.json?_=' + Date.now());
    if (!r.ok) throw new Error('sin datos');
    const d = await r.json();
    document.getElementById('aviso').style.display = 'none';
    document.getElementById('contenido').style.display = 'block';
    document.getElementById('estado').textContent = 'Lote ' + d.lote + ' · actualizado ' + d.actualizado;
    const k = d.kpis;
    k1.textContent = k.total_casos.toLocaleString('es-CO');
    k2.textContent = k.total_fallecidos.toLocaleString('es-CO');
    k3.textContent = k.letalidad_pct + '%';
    k4.textContent = k.edad_promedio ?? '-';
    k5.textContent = k.departamentos;
    const dep = d.por_departamento;
    dibujar('c1', 'bar', {labels: dep.map(x => x.departamento),
      datasets: [{label: 'Casos', data: dep.map(x => x.casos), backgroundColor: '#1f77b4'},
                 {label: 'Fallecidos', data: dep.map(x => x.fallecidos), backgroundColor: '#d62728'}]},
      {indexAxis: 'y'});
    const v = d.serie_ventanas;
    dibujar('c2', 'line', {labels: v.map(x => x.inicio),
      datasets: [{label: 'Casos', data: v.map(x => x.casos), borderColor: '#1f77b4', tension: .25},
                 {label: 'Fallecidos', data: v.map(x => x.fallecidos), borderColor: '#d62728', tension: .25}]});
    const g = d.por_grupo_edad;
    dibujar('c3', 'bar', {labels: g.map(x => x.grupo),
      datasets: [{label: 'Casos', data: g.map(x => x.casos), backgroundColor: '#2ca02c'}]});
    const s = Object.entries(d.por_sexo);
    dibujar('c4', 'doughnut', {labels: s.map(x => x[0]),
      datasets: [{data: s.map(x => x[1]), backgroundColor: ['#d62728', '#1f77b4', '#999999']}]});
  } catch (e) { /* aún no existe el archivo */ }
}
actualizar(); setInterval(actualizar, 2000);
</script></body></html>
"""


def crear_handler(ruta_json):
    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            ruta = self.path.split("?")[0]
            if ruta in ("/", "/index.html"):
                self._enviar(200, "text/html; charset=utf-8", HTML.encode("utf-8"))
            elif ruta == "/data.json":
                if os.path.exists(ruta_json):
                    with open(ruta_json, "rb") as f:
                        self._enviar(200, "application/json", f.read())
                else:
                    self._enviar(404, "text/plain", b"sin datos")
            else:
                self._enviar(404, "text/plain", b"no encontrado")

        def _enviar(self, codigo, tipo, cuerpo):
            self.send_response(codigo)
            self.send_header("Content-Type", tipo)
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(cuerpo)))
            self.end_headers()
            self.wfile.write(cuerpo)

        def log_message(self, *args):  # silencia el log por petición
            pass

    return Handler


def main():
    ap = argparse.ArgumentParser(description="Dashboard en tiempo real")
    ap.add_argument("--archivo", default="salida_streaming/estado.json")
    ap.add_argument("--puerto", type=int, default=8050)
    args = ap.parse_args()
    servidor = http.server.ThreadingHTTPServer(("0.0.0.0", args.puerto), crear_handler(args.archivo))
    print(f"Dashboard en http://<IP-de-la-VM>:{args.puerto}  (leyendo {args.archivo})  Ctrl+C para salir")
    try:
        servidor.serve_forever()
    except KeyboardInterrupt:
        print("\nDashboard detenido.")


if __name__ == "__main__":
    main()
