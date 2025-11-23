"""
Programador simple para el pipeline ETL usando la librería `schedule`.

Uso:
  - Ejecutar una vez inmediatamente:
      python run_etl_scheduler.py --once --nth 0

  - Programar ejecución nocturna a una hora específica (formato 24h):
      python run_etl_scheduler.py --time 02:00

  - O usar el Programador de Tareas de Windows (ejemplo):
      python C:/.../src/etl_a.py --nth 0

El script importará y llamará al ETL definido en `etl_pipeline.py`.
"""
import argparse
import time
import schedule
from datetime import datetime
from etl_pipeline import ETLConfig, ETL


def run_etl(nth, date_str, settings_path='settings.ini'):
    cfg = ETLConfig(settings_path=settings_path)
    etl = ETL(cfg)

    if date_str:
        target_date = datetime.fromisoformat(date_str).date()
    elif nth is not None:
        target_date = etl.get_nth_last_date_with_data(nth)
    else:
        target_date = None

    success = etl.run_etl(target_date)
    if success:
        print(f"[{datetime.now()}] ETL completed for {target_date}")
    else:
        print(f"[{datetime.now()}] ETL failed for {target_date}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Programador para el ETL de FleetLogix')
    parser.add_argument('--time', type=str, help='Hora HH:MM para ejecución diaria (24h). Ej: 02:00')
    parser.add_argument('--once', action='store_true', help='Ejecutar una vez inmediatamente y salir')
    parser.add_argument('--nth', type=int, default=0, help='n-ésimo último día con datos (0=último)')
    parser.add_argument('--date', type=str, help='Ejecutar para fecha específica YYYY-MM-DD')
    parser.add_argument('--settings', type=str, default='settings.ini', help='Ruta al archivo settings.ini (opcional)')
    args = parser.parse_args()

    if args.once:
        run_etl(args.nth, args.date, settings_path=args.settings)
        raise SystemExit(0)

    if not args.time:
        print('Por favor provee --time HH:MM para programación diaria o usa --once')
        raise SystemExit(2)

    def job():
        try:
            print(f"[{datetime.now()}] Iniciando ETL programado (nth={args.nth}, date={args.date})")
            run_etl(args.nth, args.date, settings_path=args.settings)
        except Exception as e:
            print(f"[{datetime.now()}] Error en ETL programado: {e}")

    schedule.every().day.at(args.time).do(job)
    print(f"Programador iniciado: el ETL se ejecutará todos los días a las {args.time}")

    while True:
        schedule.run_pending()
        time.sleep(30)
